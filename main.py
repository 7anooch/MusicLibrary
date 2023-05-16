import sys
from PyQt5.QtWidgets import QApplication, QWidget, QAbstractItemView, QListWidget, QSpacerItem, QAction, qApp, QSizePolicy
from PyQt5.QtWidgets import QVBoxLayout, QLabel, QLineEdit, QComboBox, QScrollArea, QGridLayout, QPushButton, QScrollArea, QMainWindow
from PyQt5.QtCore import QUrl, Qt, QByteArray, QBuffer, QSize, QIODevice, QTimer, QThreadPool, QRunnable, pyqtSignal, QObject
from PyQt5.QtGui import QPixmap, QImageReader, QIcon, QResizeEvent, QWheelEvent, QDesktopServices, QIntValidator
from PyQt5.QtNetwork import QNetworkAccessManager, QNetworkRequest
from db_handler import DatabaseHandler
import db_handler, sqlite3, os, requests, shutil, datetime, json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from PyQt5.QtWidgets import QInputDialog


with open('keys.json', 'r') as f:
    config = json.load(f)

SPOTIFY_CLIENT_SECRET = config['spotify']['client_secret']
SPOTIFY_CLIENT_ID = config['spotify']['client_id']
SPOTIFY_REDIRECT_URI = config['spotify']['redirect_uri']
SPOTIFY_SCOPE = config['spotify']['scope']
db_name = config['database']['name']

SPOTIFY_REDIRECT_URI = 'http://localhost:8080/callback'

class CoverLoader(QRunnable):
    def __init__(self, app, cover_url, album_name, artist_name, signal_handler):
        super().__init__()
        self.app = app
        self.cover_url = cover_url
        self.album_name = album_name
        self.artist_name = artist_name
        self.signal_handler = signal_handler

    def run(self):
        pixmap = self.app.load_cover(self.cover_url, self.album_name)
        self.signal_handler.cover_loaded.emit(pixmap, self.album_name, self.artist_name, self.cover_url)

class CoverSignalHandler(QObject):
    cover_loaded = pyqtSignal(QPixmap, str, str, str)

class MusicLibraryApp(QWidget):
    def connect_db(self, db_path):
        self.conn = sqlite3.connect(db_path)

    def __init__(self, db_handler=None):
        super().__init__()
        self.thread_pool = QThreadPool()
        if db_handler:
            self.db_handler = db_handler

        #self.clearing_filters = False
        self.init_ui()

    def are_filters_active(self):
        selected_genres = [self.genre_list.item(i).text() for i in range(self.genre_list.count()) if self.genre_list.item(i).isSelected()]
        lower_year_filter = self.lower_year_edit.text().strip()
        upper_year_filter = self.upper_year_edit.text().strip()
        scrobble_count_filter = self.scrobble_count_edit.text().strip()
        selected_filter = self.filter_combobox.currentText()

        # Return True if any of the filters are active
        return bool(
            selected_genres
            or lower_year_filter
            or upper_year_filter
            or scrobble_count_filter
            or selected_filter != "All albums"
        )

    def load_genres(self):
        cursor = self.db_handler.conn.cursor()
        cursor.execute("SELECT DISTINCT rym_genre FROM albums")
        album_genres = cursor.fetchall()

        genres = set()
        for al in album_genres:
            individual_genres = al[0].split(', ') if al[0] else []
            for genre in individual_genres:
                genres.add(genre)

        for genre in sorted(list(genres)):
            if genre:
                self.genre_list.addItem(genre)

    def clear_filters(self):
        # Clear genre list selection
        self.genre_list.clearSelection()

        # Reset filter combobox to default value
        index = self.filter_combobox.findText("All albums")
        if index >= 0:
            self.filter_combobox.setCurrentIndex(index)

        # Reset last_heard_combobox to default value
        index = self.last_heard_combobox.findText("Anytime")
        if index >= 0:
            self.last_heard_combobox.setCurrentIndex(index)

        # Clear year and scrobble count fields
        self.lower_year_edit.clear()
        self.upper_year_edit.clear()
        self.lower_scrob_edit.clear()
        self.upper_scrob_edit.clear()

        # Clear the existing album buttons in the scroll area
        for i in reversed(range(self.scroll_content_layout.count())):
            widget = self.scroll_content_layout.takeAt(i).widget()
            if widget:
                widget.deleteLater()

    def init_ui(self):
        self.setWindowTitle("Music Library")
        self.setGeometry(100, 100, 800, 600)

        grid_layout = QGridLayout()

        clear_button = QPushButton("Clear")
        clear_button.clicked.connect(self.clear_filters)

        self.execute_query_button = QPushButton("Run Query")
        grid_layout.addWidget(self.execute_query_button, 2, 4)
        self.execute_query_button.clicked.connect(self.update_album_list)

        # In your init_ui function, add the following lines to create the button
        self.save_to_spotify_button = QPushButton("Save to Spotify")
        self.save_to_spotify_button.clicked.connect(self.save_album_list_to_playlist)
        grid_layout.addWidget(self.save_to_spotify_button, 1, 4)  # Adjust the position as needed


        # Add this code in your init_ui method
        self.genre_operator_combobox = QComboBox()
        self.genre_operator_combobox.addItems(["OR", "AND"])
        grid_layout.addWidget(self.genre_operator_combobox, 0, 3)

        # Add filters
        genre_label = QLabel("Genre:")
        self.genre_list = QListWidget()
        self.genre_list.setMaximumHeight(150)
        self.genre_list.setSelectionMode(QAbstractItemView.MultiSelection)
        #self.genre_list.itemSelectionChanged.connect(self.update_album_list)

        # Create the filter combobox
        self.filter_combobox = QComboBox()
        self.filter_combobox.addItem("All albums")
        self.filter_combobox.addItem("Saved albums")
        self.filter_combobox.addItem("Not saved albums")

        # Connect the filter combobox signal
        #self.filter_combobox.currentTextChanged.connect(self.update_album_list)
        self.lower_year_edit = QLineEdit()
        self.lower_year_edit.setPlaceholderText("From Year")
        self.lower_year_edit.setValidator(QIntValidator(0, 9999))
        grid_layout.addWidget(self.lower_year_edit, 3, 2)

        # Create QLineEdit for upper year bound
        self.upper_year_edit = QLineEdit()
        self.upper_year_edit.setPlaceholderText("To Year")
        self.upper_year_edit.setValidator(QIntValidator(0, 9999))
        grid_layout.addWidget(self.upper_year_edit, 4, 2)

        scrobble_count_label = QLabel("Filter by scrobble count:")
        self.scrobble_count_edit = QLineEdit()

        grid_layout.addWidget(scrobble_count_label, 2, 3)
        self.lower_scrob_edit = QLineEdit()
        self.lower_scrob_edit.setPlaceholderText("Min album scrobbles")
        self.lower_scrob_edit.setValidator(QIntValidator(0, 9999))
        grid_layout.addWidget(self.lower_scrob_edit, 3, 3)

        # Create QLineEdit for upper year bound
        self.upper_scrob_edit = QLineEdit()
        self.upper_scrob_edit.setPlaceholderText("Max album scrobbles")
        self.upper_scrob_edit.setValidator(QIntValidator(0, 9999))
        grid_layout.addWidget(self.upper_scrob_edit, 4, 3)
        #self.lower_year_edit.returnPressed.connect(self.update_album_list)
        #self.upper_year_edit.returnPressed.connect(self.update_album_list)
        #self.lower_scrob_edit.returnPressed.connect(self.update_album_list)
        #self.upper_scrob_edit.returnPressed.connect(self.update_album_list)


        heard_label = QLabel("Last listened:")
        self.last_heard_combobox = QComboBox()
        self.last_heard_combobox.addItem("Anytime")
        self.last_heard_combobox.addItem("Past week")
        self.last_heard_combobox.addItem("Past month")
        self.last_heard_combobox.addItem("Past 3 months")
        self.last_heard_combobox.addItem("Past 6 months")
        self.last_heard_combobox.addItem("Past year")
        self.last_heard_combobox.addItem("Years")
        self.last_heard_combobox.addItem("Half a decade or more")
        #self.last_heard_combobox.currentTextChanged.connect(self.update_album_list)
        grid_layout.addWidget(self.last_heard_combobox, 1, 3)
        #self.last_heard_combobox.currentTextChanged.connect(self.update_album_list)
        grid_layout.addWidget(heard_label, 1, 2)
        clear_button.clicked.connect(self.clear_filters)

        year_label = QLabel("Filter by release year:")
        grid_layout.addWidget(year_label, 2, 2)
        grid_layout.addWidget(clear_button, 0, 4, 1, 1)
        grid_layout.addWidget(self.genre_list, 0, 0, 5, 2)
        grid_layout.addWidget(self.filter_combobox, 0, 2, 1, 1) # Add the filter combobox

        self.load_genres()

        # Add scroll area for albums
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)

        scroll_content = QWidget()
        self.scroll_content_layout = QGridLayout() # Make this an instance variable

        # Dummy album buttons
        for i in range(10):
            album_button = QPushButton(f"Album {i+1}")
            self.scroll_content_layout.addWidget(album_button)

        scroll_content.setLayout(self.scroll_content_layout)
        scroll_area.setWidget(scroll_content)

        layout = QVBoxLayout()
        layout.addLayout(grid_layout)
        layout.addWidget(scroll_area)

        self.setLayout(layout)

    def get_spotify_access_token(client_id, client_secret, redirect_uri, scope):
        code = get_spotify_authorization_code(client_id, redirect_uri, scope)
        url = 'https://accounts.spotify.com/api/token'
        auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

        headers = {
            'Authorization': f'Basic {auth_header}'
        }
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': redirect_uri
        }
        response = requests.post(url, headers=headers, data=data)
        token_data = response.json()
        access_token = token_data['access_token']
        return access_token


    def create_spotify_playlist(token, user_id, playlist_name):
        url = f"https://api.spotify.com/v1/users/{user_id}/playlists"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        data = {
            "name": playlist_name
        }

        response = requests.post(url, headers=headers, data=json.dumps(data))

        if response.status_code == 201:
            return response.json()["id"]
        else:
            return None

    def add_albums_to_spotify_playlist(token, playlist_id, album_uris):
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        data = {
            "uris": album_uris
        }

        response = requests.post(url, headers=headers, data=json.dumps(data))

        return response.status_code == 201

    def get_album_uris_from_current_album_list(self):
        album_uris = []

        for album in self.displayed_albums:
            if album[4] is not None:
                album_uris.append(album[4])

        return album_uris




    def load_cover(self, cover_url, album_name):
        if cover_url:
            try:
                response = requests.get(cover_url)
                if response.status_code == 200:
                    image_data = response.content

                    buffer = QBuffer()
                    buffer.setData(image_data)
                    buffer.open(QBuffer.ReadWrite)

                    image_reader = QImageReader()
                    image_reader.setDevice(buffer)

                    if image_reader.canRead():
                        pixmap = QPixmap.fromImageReader(image_reader)
                        buffer.close()
                        return pixmap.scaled(150, 150, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            except Exception as e:
                print(f"Error downloading cover for {album_name}: {e}")

        pixmap = QPixmap()
        pixmap.fill(Qt.transparent)
        return pixmap.scaled(150, 150, Qt.KeepAspectRatio, Qt.SmoothTransformation)

    def update_album_list(self, start=0): 
        self.current_album_ids = []
        self.displayed_albums = []
        selected_genres = [self.genre_list.item(i).text() for i in range(self.genre_list.count()) if self.genre_list.item(i).isSelected()]
        lower_year_filter = self.lower_year_edit.text().strip()
        upper_year_filter = self.upper_year_edit.text().strip()
        selected_filter = self.filter_combobox.currentText()
        last_heard_filter = self.last_heard_combobox.currentText()
        lower_scrob_filter = self.lower_scrob_edit.text().strip()
        upper_scrob_filter = self.upper_scrob_edit.text().strip()

        # Create a new cursor object
        cursor = self.db_handler.conn.cursor()

        # Prepare the SQL query based on the selected genre and filter
        query = "SELECT artist_name, album_name, cover_art_url, spotify_url, spotify_uri FROM albums"
        conditions = []
        params = []
    
        if selected_genres:
            genre_conditions = []
            for genre in selected_genres:
                genre_conditions.append("rym_genre LIKE ?")
                params.append(f"%{genre}%")
            selected_operator = self.genre_operator_combobox.currentText()
            conditions.append("(" + f" {selected_operator} ".join(genre_conditions) + ")")

        else:
            params = []

        if lower_year_filter and upper_year_filter:
            conditions.append("release_year BETWEEN ? AND ?")
            params.extend([lower_year_filter, upper_year_filter])
        elif lower_year_filter:
            conditions.append("release_year >= ?")
            params.append(lower_year_filter)
        elif upper_year_filter:
            conditions.append("release_year <= ?")
            params.append(upper_year_filter)

        if lower_scrob_filter and upper_scrob_filter:
            conditions.append("scrobble_count BETWEEN ? AND ?")
            params.extend([lower_scrob_filter, upper_scrob_filter])
        elif lower_scrob_filter:
            conditions.append("scrobble_count >= ?")
            params.append(lower_scrob_filter)
        elif upper_scrob_filter:
            conditions.append("scrobble_count <= ?")
            params.append(upper_scrob_filter)


        # Add condition for the saved/not saved albums
        if selected_filter == "Saved albums":
            conditions.append("saved='saved'")
        elif selected_filter == "Not saved albums":
            conditions.append("saved IS NULL")

        if last_heard_filter != "Anytime":
            last_heard_timestamp = None
            if last_heard_filter == "Past week":
                last_heard_date = datetime.datetime.now() - datetime.timedelta(days=7)
            elif last_heard_filter == "Past month":
                last_heard_date = datetime.datetime.now() - datetime.timedelta(days=30)
            elif last_heard_filter == "Past 3 months":
                last_heard_date = datetime.datetime.now() - datetime.timedelta(days=90)
            elif last_heard_filter == "Past 6 months":
                last_heard_date = datetime.datetime.now() - datetime.timedelta(days=180)
            elif last_heard_filter == "Past year":
                last_heard_date = datetime.datetime.now() - datetime.timedelta(days=365)
            elif last_heard_filter == "Years":
                last_heard_date = datetime.datetime.now() - datetime.timedelta(days=1825)
            elif last_heard_filter == "Half a decade or more":
                last_heard_date = datetime.datetime.now() - datetime.timedelta(days=1825)

            if last_heard_date:
                last_heard_timestamp = int(last_heard_date.timestamp())
                conditions.append("last_played >= ?")
                params.append(last_heard_timestamp)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Print the final query and parameters for debugging purposes
        print(f"Query: {query}")
        print(f"Params: {params}")

        # Execute the modified query with the conditions
        cursor.execute(query, tuple(params))
        albums = cursor.fetchall()
        self.current_album_ids = [album[0] for album in albums] 

        if len(albums) == 0:
            no_albums_label = QLabel("No albums found.")
            no_albums_label.setAlignment(Qt.AlignCenter)
            self.scroll_content_layout.addWidget(no_albums_label, 0, 0, 1, 4)
            return

        # Clear the existing album buttons in the scroll area
        for i in reversed(range(self.scroll_content_layout.count())):
            widget = self.scroll_content_layout.takeAt(i).widget()
            if widget:
                widget.deleteLater()

        def load_album_button(index, row, col):
            album = albums[index]
            album_widget = self.create_album_button(album[0], album[1], album[2], album[3])
            
            self.scroll_content_layout.addWidget(album_widget, row, col)
            self.scroll_content_layout.update()

        row = 0
        col = 0
        for index, album in enumerate(albums):
            self.displayed_albums.append(album)
            QTimer.singleShot(index * 100, lambda i=index, r=row, c=col: load_album_button(i, r, c))

            col += 1
            if col == 4:
                col = 0
                row += 1

        print(f"Filtered albums count: {len(albums)}")
        for album in albums:
            print(album)

    def create_album_button(self, artist_name, album_name, cover_url, spotify_url):
        cache_folder = "cache"
        cover_art_filename = f"{artist_name}_{album_name}.jpg".replace(" ", "_").replace("/", "_")
        cover_art_path = os.path.join(cache_folder, cover_art_filename)

        # Load the album cover from the cache folder
        pixmap = QPixmap(cover_art_path)

        layout = QVBoxLayout()
        button = QPushButton()

        # Download the album cover and save it to the cache folder if it doesn't exist
        if not os.path.exists(cover_art_path) and cover_url and cover_url != "None":
            if cover_url and cover_url != "None":
                try:
                    response = requests.get(cover_url, stream=True)
                except requests.exceptions.SSLError as e:
                    print(f"SSL Error while downloading album cover: {e}")
                    response = None
            else:
                response = None

            if response and response.status_code == 200:
                os.makedirs(cache_folder, exist_ok=True)
                with open(cover_art_path, "wb") as f:
                    response.raw.decode_content = True
                    shutil.copyfileobj(response.raw, f)
            else:
                response = None
        else:
            response = None

        if response is not None or not pixmap.isNull():
            icon = QIcon(pixmap.scaled(115, 115, Qt.KeepAspectRatio, Qt.SmoothTransformation))
            button.setIcon(icon)
            button.setIconSize(QSize(115, 115))
        else:
            button.setText(album_name)

        button.clicked.connect(lambda: self.open_spotify_url(spotify_url))

        layout.addWidget(button)

        artist_label = QLabel(artist_name)
        layout.addWidget(artist_label)

        container = QWidget()
        container.setLayout(layout)
        return container

    def open_spotify_url(self, url):
        QDesktopServices.openUrl(QUrl(url))

    def save_album_list_to_playlist(self):
        # Prompt the user to enter a playlist name
        playlist_name, ok = QInputDialog.getText(self, 'Playlist Name', 'Enter a name for your playlist:')
        if not ok or not playlist_name:
            return

       # Create a Spotify API client with the appropriate credentials and scope
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=SPOTIFY_CLIENT_ID,
                                                        client_secret=SPOTIFY_CLIENT_SECRET,
                                                        redirect_uri=SPOTIFY_REDIRECT_URI,
                                                        scope="playlist-modify-public"))

        # Get the user's ID
        user_id = sp.current_user()["id"]

        # Create a new playlist
        playlist = sp.user_playlist_create(user_id, playlist_name, public=True, collaborative=False, description="Playlist created by my Music Library App")

       # Get the album URIs from the current list
        album_uris = self.get_album_uris_from_current_album_list()

        # Add the album tracks to the playlist
        for album_uri in album_uris:
            tracks = sp.album_tracks(album_uri)
            track_uris = [track["uri"] for track in tracks["items"]]
            sp.playlist_add_items(playlist["id"], track_uris)

        print(f"Playlist created: {playlist['name']} (ID: {playlist['id']})")


def main():
    app = QApplication(sys.argv)

    # Create an instance of the DatabaseHandler class
    db_handler = DatabaseHandler(db_name)

    music_library_app = MusicLibraryApp(db_handler)
    music_library_app.show()

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
