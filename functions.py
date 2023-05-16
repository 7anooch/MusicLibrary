import requests, spotipy, base64, webbrowser, json, sqlite3, schedule, time, pylast, re, random
import musicbrainzngs, discogs_client, requests.exceptions, urllib.parse, unicodedata, datetime, os
import pandas as pd
import matplotlib.pyplot as plt
from spotipy.oauth2 import SpotifyClientCredentials
from urllib.parse import urlencode, urlparse, parse_qs
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
from requests.exceptions import ConnectTimeout
from pylast import PyLastError
import rymscraper
from rymscraper import RymUrl
from rapidfuzz import fuzz, process 
import selenium.common.exceptions

with open('keys.json', 'r') as f:
    config = json.load(f)

SPOTIFY_CLIENT_SECRET = config['spotify']['client_secret']
SPOTIFY_CLIENT_ID = config['spotify']['client_id']
LASTFM_API_KEY = config['lastfm']['api_key']
LASTFM_SECRET = config['lastfm']['secret']
LASTFM_USER = config['lastfm']['user']
SPOTIFY_REDIRECT_URI = config['spotify']['redirect_uri']
SPOTIFY_SCOPE = config['spotify']['scope']
db_name = config['database']['name']
d = discogs_client.Client("MusicLibrary/0.1", user_token=config['discogs']['token'])  # Replace with your API key
SCRAPERAPI_KEY = config['scraperapi']['key']
MUSICBRAINZ_API_URL = "https://musicbrainz.org/ws/2/"

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:86.0) Gecko/20100101 Firefox/86.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:86.0) Gecko/20100101 Firefox/86.0',
    'Mozilla/5.0 (Linux; Android 10; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Mobile Safari/537.36',
    'Mozilla/5.0 (iPad; CPU OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:86.0) Gecko/20100101 Firefox/86.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:86.0) Gecko/20100101 Firefox/86.0',
    'Mozilla/5.0 (Linux; Android 10; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.181 Mobile Safari/537.36',
    'Mozilla/5.0 (iPad; CPU OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1'
]

referers = [
    'https://www.google.com/',
    'https://www.bing.com/',
    'https://www.yahoo.com/',
    'https://duckduckgo.com/',
    'https://www.reddit.com/',
    'https://www.facebook.com/',
    'https://www.twitter.com/',
    'https://www.linkedin.com/',
    'https://www.pinterest.com/',
    'https://www.instagram.com/'
]

# ----- Lastfm functions ----- 
# Fetches recent tracks for a Last.fm user, with retry logic and a delay between attempts.
def fetch_lastfm_data(api_key, user, limit=1000, page=1, retries=3, delay=5):
    url = 'http://ws.audioscrobbler.com/2.0/'
    params = {
        'method': 'user.getrecenttracks',
        'user': user,
        'api_key': api_key,
        'format': 'json',
        'limit': limit,
        'page': page
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            if 'recenttracks' not in data:
                print("Error fetching Last.fm data:", data)
                return None

            return data
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                print(f"Request timed out. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Request timed out. Giving up.")
                return None

# Fetches track info from Last.fm using the artist and track names.
def fetch_track_info(api_key, artist, track):
    url = 'http://ws.audioscrobbler.com/2.0/'
    params = {
        'method': 'track.getInfo',
        'artist': artist,
        'track': track,
        'api_key': api_key,
        'format': 'json',
    }
    response = requests.get(url, params=params)
    data = response.json()

    if 'track' not in data:
        print("Error fetching track info:", data)
        return None

    return data['track']

# Saves Last.fm data to the SQLite3 database, updating track information
def save_lastfm_data_to_db(conn):
    cursor = conn.cursor()
    lastfm_data = fetch_lastfm_data(LASTFM_API_KEY, LASTFM_USER)

    if lastfm_data is None:
        print("Failed to update Last.fm data")
        return

    total_pages = lastfm_data['recenttracks']['@attr']['totalPages']

    for page in range(1, int(total_pages) + 1):
        lastfm_data = fetch_lastfm_data(LASTFM_API_KEY, LASTFM_USER, page=page)
        tracks = lastfm_data['recenttracks']['track']

        for track in tracks:
            if 'date' in track:
                track_id = track['mbid'] if 'mbid' in track and track['mbid'] else track['url']
                track_name = track['name']
                artist_name = track['artist']['#text']
                album_name = track['album']['#text'] if 'album' in track else None
                timestamp = int(track['date']['uts'])
                track_info = fetch_track_info(LASTFM_API_KEY, artist_name, track_name)
                if track_info and 'playcount' in track_info:
                    scrobble_count = int(track_info['playcount'])
                else:
                    scrobble_count = 0

#                cursor.execute('''INSERT OR REPLACE INTO tracks (track_id, track_name, artist_name, album_name, timestamp, scrobble_count)
#                                 VALUES (?, ?, ?, ?, ?, ?)''', (track_id, track_name, artist_name, album_name, timestamp, scrobble_count))
                cursor.execute('''INSERT OR IGNORE INTO tracks (track_id, track_name, artist_name, album_name, timestamp)
                                  VALUES (?, ?, ?, ?, ?)''', (track_id, track_name, artist_name, album_name, timestamp))
        conn.commit()
        print(f"Saved Last.fm data for page {page}")

# Updates the Last.fm data in the database
def update_lastfm_data():
    conn = create_database()
    save_lastfm_data_to_db(conn)

# Update album cover art using the Last.fm API
def update_albums_with_cover_arts(conn, api_key):
    cursor = conn.cursor()
    cursor.execute("SELECT album_id, artist_name, album_name FROM albums WHERE cover_art_url IS NULL")
    albums = cursor.fetchall()

    missing_cover_arts = []

    for album_id, artist_name, album_name in albums:
        cover_art_url = get_lastfm_cover_art_url(api_key, artist_name, album_name)
        if cover_art_url:
            cursor.execute("UPDATE albums SET cover_art_url=? WHERE album_id=?", (cover_art_url, album_id))
            print(f"Updated cover art for {album_name}: {cover_art_url}")
        else:
            print(f"No cover art found for {album_name}")
            missing_cover_arts.append(f"{artist_name} - {album_name}")

    with open("missing_cover_arts.txt", "w") as file:
        file.write("\n".join(missing_cover_arts))

    conn.commit()

# Fetches the album cover art URL from the Last.fm API for a given artist and album.
def get_lastfm_cover_art_url(api_key, artist_name, album_name):
    network = pylast.LastFMNetwork(api_key=api_key)
    album = network.get_album(artist_name, album_name)
    try:
        cover_art_url = album.get_cover_image()
        if cover_art_url:
            return cover_art_url
    except Exception as e:
        print(f"Error fetching cover art for {album_name}: {e}")

    return None

# Updates the artist_image column for all artists in the database.
def update_artists_with_images(conn):
    cursor = conn.cursor()

    # Check if the file exists. If it does, read it. Otherwise, create an empty list.
    if os.path.exists('unfound_artist_images.txt'):
        with open('unfound_artist_images.txt', 'r') as f:
            existing_artists = f.read().splitlines()
    else:
        existing_artists = []

    cursor.execute("SELECT artist_id, artist_name FROM artists WHERE artist_image IS NULL")
    artists = cursor.fetchall()

    not_found_artists = []  # list to store artists for which images are not found

    for artist_id, artist_name in artists:
        if artist_name:  # Check if the artist_name is not None
            if artist_name in existing_artists:
                continue

            artist_image_url = get_lastfm_artist_image(artist_name)

            if artist_image_url:
                cursor.execute("UPDATE artists SET artist_image=? WHERE artist_id=?", (artist_image_url, artist_id))
                print(f"Updated artist image for {artist_name}: {artist_image_url}")
            else:
                print(f"No artist image found for {artist_name}")
                not_found_artists.append(artist_name)
        else:
            print("Artist name is None")

    # Write to the file in append mode once after all artists have been processed
    with open('unfound_artist_images.txt', 'a') as f:
        for artist in not_found_artists:
            f.write(f"{artist}\n")

    conn.commit()


    # Write the list of not found artists to a text file
    with open("not_found_artists.txt", "w") as f:
        for artist in not_found_artists:
            f.write(artist + "\n")

# Fetches the artist image URL from the Last.fm API for a given artist.
def get_lastfm_artist_image(artist_name):
    url = f'https://ws.audioscrobbler.com/2.0/'
    params = {
        'method': 'artist.getinfo',
        'artist': artist_name,
        'api_key': LASTFM_API_KEY,
        'format': 'json'
    }

    max_retries = 3
    for _ in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'artist' in data and 'image' in data['artist']:
                    for image in data['artist']['image']:
                        if image['size'] == 'extralarge' and '#text' in image:
                            return image['#text']
            break
        except requests.exceptions.Timeout:
            print(f"Timeout error occurred for artist: {artist_name}")
        except requests.exceptions.SSLError:
            print(f"SSL error occurred for artist: {artist_name}")
            time.sleep(1)  # Wait for 1 second before retrying the request
    return None

# Fetches the genre tags for a given artist and album from the Last.fm API.
def get_lastfm_genres(artist_name, album_name):
    api_key = LASTFM_API_KEY
    url = f'http://ws.audioscrobbler.com/2.0/?method=album.getinfo&artist={artist_name}&album={album_name}&api_key={api_key}&format=json'
    
    data = None
    
    try:
        response = requests.get(url)
        data = response.json()

        if 'album' in data and 'tags' in data['album'] and 'tag' in data['album']['tags']:
            genres = [tag['name'] for tag in data['album']['tags']['tag']]
            return genres
        else:
            print(f"No tags found for {artist_name} - {album_name}")
            return []

    except Exception as e:
        print(f"Error in get_lastfm_genres for {artist_name} - {album_name}: {e}")
        if data:
            print(f"JSON data: {data}")
        return []

# Updates the genres column for all albums in the database with genres from the Last.fm API.
def update_albums_with_lastfm_genres(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT album_id, artist_name, album_name, genres FROM albums WHERE genres IS NULL OR genres = ""')
    albums = cursor.fetchall()

    print("Updating genres for albums...")
    for album_id, artist_name, album_name, existing_genres in albums:
        if not existing_genres:
            existing_genres_list = []
        else:
            existing_genres_list = [genre.strip() for genre in existing_genres.split(',')]

        try:
            lastfm_genres = get_lastfm_genres(artist_name, album_name)
            
            if lastfm_genres:
                print(f"Last.fm genres found for {artist_name} - {album_name}: {lastfm_genres}")
                updated_genres_list = list(set(existing_genres_list + lastfm_genres))
                updated_genres = ', '.join(updated_genres_list)

                if updated_genres != existing_genres:
                    cursor.execute('UPDATE albums SET genres = ? WHERE album_id = ?', (updated_genres, album_id))
                    conn.commit()
                    print(f"Updated genres for {artist_name} - {album_name}: {updated_genres}")
            else:
                print(f"No Last.fm genres found for {artist_name} - {album_name}")
        except requests.exceptions.ConnectTimeout:
            print(f"Connection timeout for {artist_name} - {album_name}. Skipping this album.")

    print("Genres updated.")

# Fetches recently played tracks from Last.fm
def get_recent_lastfm_tracks(lastfm_username, lastfm_api_key, from_timestamp):
    params = {
        'method': 'user.getRecentTracks',
        'user': lastfm_username,
        'api_key': lastfm_api_key,
        'format': 'json',
        'from': from_timestamp
    }
    response = requests.get('http://ws.audioscrobbler.com/2.0/', params=params)
    return response.json()

# Updates the scrobbles table with recent tracks from Last.fm
def update_scrobbles(conn, recent_tracks):
    cursor = conn.cursor()
    for track in recent_tracks['recenttracks']['track']:
        if 'date' not in track:
            # Ignore tracks that are currently playing
            continue

        artist_name = track['artist']['#text']
        album_name = track['album']['#text']
        track_name = track['name']
        scrobble_timestamp = int(track['date']['uts'])

        # Insert/update artist and album information in the albums table
        # (Replace this with the appropriate function you have for inserting/updating the albums table)
        insert_or_update_album(conn, artist_name, album_name)

        # Insert/update scrobble information in the tracks table
        cursor.execute('''INSERT OR IGNORE INTO tracks (artist_name, album_name, track_name, scrobble_timestamp)
                          VALUES (?, ?, ?, ?)''',
                       (artist_name, album_name, track_name, scrobble_timestamp))
        conn.commit()

# Fetches scrobbles from Last.fm and returns them as a list of dictionaries
def fetch_lastfm_scrobbles(conn, api_key, api_secret, username, from_timestamp=None, limit=None):
    print("fetch_lastfm_scrobbles called")
    network = pylast.LastFMNetwork(api_key=api_key, api_secret=api_secret)
    user = network.get_user(username)
    print(f"User: {username}")

    try:
        if from_timestamp is None:
            print("Fetching recent tracks without timestamp")
            recent_tracks = user.get_recent_tracks(limit=limit)
        else:
            print(f"Fetching recent tracks from timestamp: {from_timestamp}")
            recent_tracks = user.get_recent_tracks(time_from=from_timestamp, limit=limit)

        scrobbles = []
        scrobble_count = 0

        for track in recent_tracks:
            scrobble = {
                'date': track.timestamp,
                'artist_name': track.track.artist.name,
                'album_name': track.album,
                'track_name': track.track.title
            }
            scrobbles.append(scrobble)
            scrobble_count += 1
            print(f"Scrobble {scrobble_count}: {scrobble}")

        print(f"Fetched {len(scrobbles)} scrobbles")
        # store_intermediate_result(conn, "recent_scrobbles", scrobbles)
        print("Scrobbles fetched and stored successfully")
        return scrobbles

    except PyLastError as e:
        print(f"An error occurred while fetching Last.fm scrobbles: {e}")

# Retrieves release year for an album from Last.fm
def get_lastfm_release_year(artist_name, album_name, api_key):
    url = f'http://ws.audioscrobbler.com/2.0/?method=album.getinfo&artist={artist_name}&album={album_name}&api_key={api_key}&format=json'
    try:
        response = requests.get(url)
        data = response.json()
        if 'album' in data and 'releasedate' in data['album']:
            release_year = data['album']['releasedate'].strip()[:4]
            if release_year.isdigit():
                return int(release_year)
    except Exception as e:
        print(f"Error fetching release year for {artist_name} - {album_name}: {e}")

    return None

# Updates albums with release years fetched from Last.fm
def update_albums_with_lastfm_release_years(conn, lastfm_api_key):
    cursor = conn.cursor()
    cursor.execute("SELECT album_id, artist_name, album_name FROM albums WHERE release_year IS NULL")
    albums = cursor.fetchall()

    missing_release_years = []

    for album_id, artist_name, album_name in albums:
        release_year = get_lastfm_release_year(artist_name, album_name, lastfm_api_key)
        if release_year:
            cursor.execute("UPDATE albums SET release_year=? WHERE album_id=?", (release_year, album_id))
            print(f"Updated release year for {album_name}: {release_year}")
        else:
            print(f"No release year found for {album_name}")
            missing_release_years.append(f"{artist_name} - {album_name}")

    with open("missing_release_years.txt", "w") as file:
        file.write("\n".join(missing_release_years))

    conn.commit()

# Fetches scrobbles from Last.fm API and saves them to the database
def fetch_scrobbles_and_save_to_db(conn):
    cursor = conn.cursor()
    # Create the playlists table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS playlist (
        id INTEGER PRIMARY KEY,
        track_name TEXT NOT NULL,
        artist_name TEXT NOT NULL,
        album_name TEXT,
        timestamp INTEGER NOT NULL
    )
    """
    conn.execute(create_table_query)
    conn.commit()

    max_retries = 5
    retry_delay = 10  # Delay in seconds between retries

    last_successful_page = get_last_successful_page(conn)

    # Fetch scrobbles from the Last.fm API
    scrobbles = []
    page = last_successful_page
    while True:
        retry_count = 0
        success = False
        while retry_count < max_retries:
            try:
                response = requests.get("http://ws.audioscrobbler.com/2.0/", params={
                    "method": "user.getRecentTracks",
                    "user": LASTFM_USER,
                    "api_key": LASTFM_API_KEY,
                    "format": "json",
                    "limit": 200,
                    "page": page
                })

                data = response.json()
                success = True
                break
            except requests.exceptions.RequestException as e:
                print(f"Request failed with error: {e}")
                print(f"Retrying... (attempt {retry_count + 1})")
                retry_count += 1
                time.sleep(retry_delay)

        if not success:
            print("Reached maximum retries. Aborting...")
            return

        tracks = data["recenttracks"]["track"]

        if not tracks:
            break

        for track in tracks:
            if 'date' not in track:
                continue
            scrobbles.append({
                "track_name": track["name"],
                "artist_name": track["artist"]["#text"],
                "album_name": track["album"]["#text"],
                "timestamp": int(track["date"]["uts"])
            })

        print(f"Successfully saved {len(scrobbles)} scrobbles from page {page} to the database.")

        if len(tracks) < 200:
            break
        update_last_successful_page(conn, page)
        page += 1

    # Save scrobbles to the database
    insert_query = """
    INSERT INTO playlist (track_name, artist_name, album_name, timestamp)
    VALUES (?, ?, ?, ?)
    """
    for scrobble in scrobbles:
        conn.execute(insert_query, (scrobble["track_name"], scrobble["artist_name"], scrobble["album_name"], scrobble["timestamp"]))
    conn.commit()

# Retrieves the last successful page fetched from the Last.fm API
def get_last_successful_page(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT last_successful_page FROM lastfm_state WHERE id = 1")
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        return 1

# Updates the last successful page fetched from the Last.fm API
def update_last_successful_page(conn, page):
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO lastfm_state (id, last_successful_page) VALUES (1, ?)", (page,))
    conn.commit()

# Fetches missing scrobbles from the Last.fm API and adds them to the database
def fetch_missing_scrobbles(api_key, username, conn):
    lastfm_api_url = f"http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user={username}&api_key={api_key}&format=json&limit=200"
    current_page = 1
    missing_scrobbles_count = 0

    while True:
        try:
            response = requests.get(f"{lastfm_api_url}&page={current_page}", timeout=10)
        except requests.exceptions.ConnectTimeout:
            print("Connection to Last.fm API timed out, skipping this page.")
            current_page += 1
            continue
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while fetching data from Last.fm API: {e}")
            break

        data = response.json()

        if "recenttracks" not in data:
            break

        tracks = data["recenttracks"]["track"]

        if not tracks:
            break

        for track in tracks:
            if "date" not in track:  # Currently playing track
                continue

            artist_name = track["artist"]["#text"]
            album_name = track["album"]["#text"]
            track_name = track["name"]
            timestamp = int(track["date"]["uts"])

            # Check if the scrobble exists in the database
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM playlist WHERE track_name = ? AND artist_name = ? AND timestamp = ?",
                           (track_name, artist_name, timestamp))
            scrobble_exists = cursor.fetchone()[0]

            if not scrobble_exists:
                # Insert the scrobble into the playlists table
                cursor.execute("INSERT INTO playlist (track_name, artist_name, album_name, timestamp) VALUES (?, ?, ?, ?)",
                               (track_name, artist_name, remove_special_editions(album_name), timestamp))
                conn.commit()
                missing_scrobbles_count += 1

        current_page += 1

    print(f"Fetched {missing_scrobbles_count} missing scrobbles")


# ---------- NEW -------------

def fetch_lastfm_data(api_key, user, limit=200, page=1, retries=3, delay=5):
    url = 'http://ws.audioscrobbler.com/2.0/'
    params = {
        'method': 'user.getrecenttracks',
        'user': user,
        'api_key': api_key,
        'format': 'json',
        'limit': limit,
        'page': page
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            if 'recenttracks' not in data:
                print("Error fetching Last.fm data:", data)
                return None

            return data['recenttracks']['track']
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Reached maximum retries. Giving up.")
                return None

def save_scrobbles_to_db(conn, scrobbles):
    cursor = conn.cursor()

    # Query to insert data into the 'tracks' table
    insert_query = """
    INSERT INTO playlist (track_name, artist_name, album_name, timestamp, mbid)
    VALUES (?, ?, ?, ?, ?)
    """

    for track in scrobbles:
        if 'date' not in track:
            continue
        mbid = track['mbid'] if 'mbid' in track and track['mbid'] else None
        track_name = track['name']
        artist_name = track['artist']['#text']
        album_name = track['album']['#text'] if 'album' in track else None
        timestamp = int(track['date']['uts'])
        conn.execute(insert_query, (track_name, artist_name, album_name, timestamp, mbid))

    conn.commit()
    print(f"Successfully saved {len(scrobbles)} scrobbles to the database.")

def fetch_and_save_scrobbles(conn, api_key, user):
    PAGE = 1
    MAX_RETRIES = 4  # Maximum number of retries per page
    total_scrobbles = 0  # Total number of scrobbles written

    while True:
        retry_count = 0

        while retry_count < MAX_RETRIES:
            scrobbles = fetch_lastfm_data(api_key, user, page=PAGE)
            
            # If we fetched some data, break the retry loop
            if scrobbles is not None and len(scrobbles) > 0:
                break

            # If the fetch failed, print an error and retry
            print(f"Failed to fetch Last.fm data for page {PAGE}. Retrying... ({retry_count + 1}/{MAX_RETRIES})")
            retry_count += 1

        # If we've exhausted our retries and still haven't fetched any data, we stop the main loop
        if retry_count == MAX_RETRIES:
            print("No more scrobbles to fetch.")
            break

        # Save fetched data to the database
        save_scrobbles_to_db(conn, scrobbles)
        total_scrobbles += len(scrobbles)
        print(f"Page {PAGE} completed. Total scrobbles saved so far: {total_scrobbles}")
        
        PAGE += 1



# ----- Musicbrainz functions ----- 
# Fetches artist data from MusicBrainz based on artist name
def fetch_musicbrainz_artist_data(artist_name):
    url = 'https://musicbrainz.org/ws/2/artist'
    headers = {
        'User-Agent': 'YourApp/1.0 (7anooch@gmail.com)'
    }

    params = {
        'query': f'artist:{artist_name}',
        'fmt': 'json'
    }

    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    return data

# Fetches genres for the specified artist and album from MusicBrainz
def get_musicbrainz_genres(artist_name, album_name):
    try:
        result = musicbrainzngs.search_release_groups(artist=artist_name, release=album_name, limit=1)
        genres = None
        release_group = None
        if 'release-group-list' in result and result['release-group-list']:
            release_group = result['release-group-list'][0]
            if 'tag-list' in release_group:
                genres = [tag['name'] for tag in release_group['tag-list']]
                genres = ', '.join(genres)
                
        if release_group:
            # Fetch the release ID as well
            release_id = release_group['id']
            return (genres, release_id)
        else:
            return (None, None)
    except musicbrainzngs.MusicBrainzError as e:
        print(f"Error fetching genres for {album_name}: {e}")
    return (None, None)

# Update album genres and cover art using MusicBrainz data
def update_albums_with_genres(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT album_id, artist_name, album_name FROM albums WHERE genres IS NULL OR genres = ''")
    albums = cursor.fetchall()

    not_found_albums = []

    for album_id, artist_name, album_name in albums:
        genres, release_id = get_musicbrainz_genres(artist_name, album_name)
        if genres:
            cursor.execute("UPDATE albums SET genres=? WHERE album_id=?", (genres, album_id))
            print(f"Updated genres for {artist_name} - {album_name}: {genres}")
        else:
            not_found_albums.append(f"{artist_name} - {album_name}")
            print(f"No genres found for {artist_name} - {album_name}")

    if release_id:
            cover_art_url = fetch_cover_art_url(release_id)
            if cover_art_url:
                cursor.execute("UPDATE albums SET cover_art_url=? WHERE album_id=?", (cover_art_url, album_id))
                print(f"Updated cover art for {artist_name} - {album_name}: {cover_art_url}")

    conn.commit()

    # Save the list of albums with no genres found to a text file
    with open('albums_no_genres_found.txt', 'w') as file:
        for album in not_found_albums:
            file.write(album + '\n') #old version?

# Get cover art URL using MusicBrainz release ID
def fetch_cover_art_url(release_id):
    cover_art_url = f'https://coverartarchive.org/release/{release_id}/front'
    response = requests.head(cover_art_url)
    
    if response.status_code == 200:
        return cover_art_url
    else:
        print(f"No cover art found for release ID: {release_id}")
        return None

# Get MusicBrainz cover art URL using artist and album names
def get_musicbrainz_cover_art_url(artist, album):
    artist = artist.lower()
    album = album.lower()

    query = f'artist:{artist} release:{album}'
    search_results = musicbrainzngs.search_release_groups(query, limit=1)

    if search_results.get('release-group-list'):
        release_group = search_results['release-group-list'][0]
        if release_group:
            cover_art_url = f'https://coverartarchive.org/release-group/{release_group["id"]}'
            return cover_art_url

    return None

# Get cover art URL using MusicBrainz data and artist and album names
def get_cover_art_url(artist_name, album_name):
    try:
        release_group_result = musicbrainzngs.search_release_groups(artist=artist_name, release=album_name, limit=1)

        if 'release-group-list' in release_group_result and release_group_result['release-group-list']:
            release_group = release_group_result['release-group-list'][0]
            release_group_id = release_group['id']

            releases_result = musicbrainzngs.browse_releases(release_group=release_group_id, limit=1)
            
            if 'release-list' in releases_result and releases_result['release-list']:
                release = releases_result['release-list'][0]
                release_id = release['id']
                cover_art_url = f"https://coverartarchive.org/release/{release_id}/front-500"
                
                # Check if the cover art exists
                response = requests.head(cover_art_url)
                if response.status_code == 200:
                    return cover_art_url
                else:
                    print(f"Cover art not found for {artist_name} - {album_name}")
            else:
                print(f"Release not found for {artist_name} - {album_name}")
        else:
            print(f"Release group not found for {artist_name} - {album_name}")

    except musicbrainzngs.MusicBrainzError as e:
        print(f"Error fetching cover art for {artist_name} - {album_name}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error checking cover art existence for {artist_name} - {album_name}: {e}")

    return None

# Fetches the release year for a given artist and album using the MusicBrainz API.
def get_musicbrainz_release_year(artist_name, album_name):
    try:
        result = musicbrainzngs.search_releases(artist=artist_name, release=album_name, limit=1)
        if 'release-list' in result and result['release-list']:
            release = result['release-list'][0]
            if release:
                if 'date' in release:
                    release_year = release['date'][:4]
                    return release_year
    except musicbrainzngs.MusicBrainzError as e:
        print(f"Error fetching release year for {album_name}: {e}")

    return None

# Updates the release_year column for all albums in the database.
def update_albums_with_release_years(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT album_id, artist_name, album_name FROM albums WHERE release_year IS NULL")
    albums = cursor.fetchall()

    missing_release_years = []

    for album_id, artist_name, album_name in albums:
        release_year = get_musicbrainz_release_year(artist_name, album_name)
        if release_year:
            cursor.execute("UPDATE albums SET release_year=? WHERE album_id=?", (release_year, album_id))
            print(f"Updated release year for {album_name}: {release_year}")
        else:
            print(f"No release year found for {album_name}")
            missing_release_years.append(f"{artist_name} - {album_name}")

    with open("missing_release_years.txt", "w") as file:
        file.write("\n".join(missing_release_years))

    conn.commit()

# Searches for MusicBrainz release MBID using artist and album names
def search_release_mbid(artist_name, album_name, max_retries=5):
    with open("missing_mbid.txt", "a+") as missing_file:
        missing_file.seek(0)
        missing_entries = missing_file.readlines()
        entry = f"{artist_name} - {album_name}\n"
        if entry in missing_entries:
            print(f"Skipping {artist_name} - {album_name} (already in missing_mbid.txt)")
            return None

    query = f'artist:"{artist_name}" release:"{album_name}"'
    url = "https://musicbrainz.org/ws/2/release"
    params = {
        "fmt": "json",
        "query": query
    }
    headers = {
        "User-Agent": "musicparty/1.0.0 (hindalnoori@gmail.com)"
    }

    retries = 0
    found_mbid = None
    while retries <= max_retries:
        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if data["count"] > 0:
                    found_mbid = data["releases"][0]["id"]
                    break
                else:
                    print(f"Couldn't find MBID for {artist_name} - {album_name}, added to missing_mbid.txt")
                    with open("missing_mbid.txt", "a") as missing_file:
                        missing_file.write(entry)
                    return None  # add this line to exit the function if no releases were found
        except (requests.exceptions.ConnectTimeout, requests.exceptions.RequestException):
            print(f"Connection timeout for {artist_name} - {album_name}. Retry {retries}/{max_retries}")
            retries += 1
            time.sleep(2)  # Increasing sleep duration to 2 seconds

        time.sleep(1)  # Adding a delay of 1 second between requests to respect rate limit

    return found_mbid

# Updates the MusicBrainz release MBID for albums in the database
def update_album_mbid(conn):
    # Fetch all albums from the database
    cursor = conn.cursor()
    cursor.execute("SELECT album_id, artist_name, album_name FROM albums WHERE mbid IS NULL")
    albums = cursor.fetchall()
    x = 1
    for album in albums:
        # Fetch MBID using search_release_mbid function
        mbid = search_release_mbid(album[1], album[2])

        if mbid:
            # Update the mbid column in the albums table
            cursor.execute(
                "UPDATE albums SET mbid = ? WHERE album_id = ?",
                (mbid, album[0])
            )
            conn.commit()

            print(f"Updated MBID for {album[2]} by {album[1]}: {mbid}")
        elif mbid is None:
            continue
        else:
            print(f"Could not fetch MBID for {album[2]} by {album[1]}.")

        x += 1
        if x % 4 == 0:
            # Sleep for 1 second to avoid rate limit
            time.sleep(1)

# Updates release types of albums in the database using MusicBrainz API
def update_release_types(conn):
    # Configure the MusicBrainz client with your app information
    musicbrainzngs.set_useragent("MusicLibrary", "0.1", "YourAppURL")

    # Get all albums from the database
    cursor = conn.cursor()
    cursor.execute("SELECT album_id, mbid FROM albums")
    albums = cursor.fetchall()

    for album in albums:
        album_id, mbid = album

        try:
            # Fetch the release from MusicBrainz API using the MBID
            result = musicbrainzngs.get_release_by_id(mbid, includes=["release-groups"])
            release_group = result["release"]["release-group"]

            # Get the primary type of the release group
            release_type = release_group.get("primary-type", None)

            if release_type:
                # Update the 'release_type' column in the 'albums' table
                cursor.execute(
                    "UPDATE albums SET release_type = ? WHERE album_id = ?",
                    (release_type, album_id)
                )
                conn.commit()
                print(f"Updated release type for album {album_id}: {release_type}")
            else:
                print(f"Release type not found for album {album_id}")

        except musicbrainzngs.MusicBrainzError as e:
            print(f"Error fetching release group type for album {album_id}: {e}")

# Updates missing release information of albums in the database using MusicBrainz API
def update_release_info(conn):
    # Configure the MusicBrainz client with your app information
    musicbrainzngs.set_useragent("MusicLibrary", "0.1", "YourAppURL")

    # Get all albums from the database that are missing release_type, country, release_length, or tracks_mb
    cursor = conn.cursor()
    cursor.execute("SELECT album_id, mbid FROM albums WHERE release_type IS NULL OR country IS NULL OR release_length IS NULL OR tracks_mb IS NULL")
    albums = cursor.fetchall()

    for album in albums:
        album_id, mbid = album

        if mbid is None:
            continue

        try:
            # Fetch the release from MusicBrainz API using the MBID
            result = musicbrainzngs.get_release_by_id(mbid, includes=["release-groups", "recordings"])
            release = result["release"]
            release_group = release["release-group"]

            # Get the primary type of the release group
            release_type = release_group.get("primary-type", "N/A")

            # Get the country of release
            country = release.get("country", "N/A")

            # Calculate the release length (sum of track durations) in minutes and the total number of tracks
            release_length = 0
            total_tracks = 0
            for medium in release["medium-list"]:
                for track in medium["track-list"]:
                    if "length" in track:
                        release_length += int(track["length"])
                    total_tracks += 1

            release_length /= 60000  # Convert milliseconds to minutes

            # Update the 'release_type', 'country', 'release_length', and 'tracks_mb' columns in the 'albums' table
            cursor.execute(
                "UPDATE albums SET release_type = ?, country = ?, release_length = ?, tracks_mb = ? WHERE album_id = ?",
                (release_type, country, release_length, total_tracks, album_id)
            )
            conn.commit()
            print(f"Updated release info for album {album_id}: type={release_type}, country={country}, length={release_length} minutes, tracks={total_tracks}")

        except musicbrainzngs.MusicBrainzError as e:
            print(f"Error fetching release info for album {album_id}: {e}")


# ----- Spotify functions ----- 
# Gets Spotify authorization code by opening a web browser for user authentication
def get_spotify_authorization_code(client_id, redirect_uri, scope):
    authorize_url = 'https://accounts.spotify.com/authorize'
    params = {
        'client_id': client_id,
        'response_type': 'code',
        'redirect_uri': redirect_uri,
        'scope': scope
    }
    url = f"{authorize_url}?{urlencode(params)}"
    webbrowser.open(url)
    response_url = input("Please paste the redirected URL here: ")
    query = urlparse(response_url).query
    response_params = parse_qs(query)
    code = response_params['code'][0]
    return code

# Gets Spotify access token using the authorization code
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

# Fetches saved albums from the user's Spotify account with retry logic and a delay between attempts
def fetch_spotify_saved_albums(access_token, limit=50, offset=0, retries=3, delay=5):
    url = 'https://api.spotify.com/v1/me/albums'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    params = {
        'limit': limit,
        'offset': offset
    }

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            data = response.json()
            return data
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                print(f"Request timed out. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise

# Saves the user's saved albums from Spotify to the database
def save_spotify_saved_albums_to_db(conn, access_token):
    cursor = conn.cursor()
    limit = 50
    offset = 0

    while True:
        albums_data = fetch_spotify_saved_albums(access_token, limit=limit, offset=offset)
        albums = albums_data['items']

        if not albums:
            break

        for album_item in albums:
            album = album_item['album']
            album_id = album['id']
            album_name = album['name']
            artist_name = album['artists'][0]['name']

            cursor.execute('''INSERT OR IGNORE INTO saved_albums (album_id, album_name, artist_name)
                              VALUES (?, ?, ?)''', (album_id, album_name, artist_name))

        offset += limit

    conn.commit()


# Fetches recently saved albums from Spotify
def get_recent_saved_albums(spotify_token, after_timestamp):
    headers = {
        'Authorization': f'Bearer {spotify_token}'
    }
    params = {
        'limit': 50,
        'after': after_timestamp
    }
    response = requests.get('https://api.spotify.com/v1/me/albums', headers=headers, params=params)
    return response.json()

# Updates the saved_albums table with recent albums from Spotify
def update_saved_albums(conn, recent_albums):
    cursor = conn.cursor()
    for item in recent_albums['items']:
        album = item['album']
        artist_name = album['artists'][0]['name']
        album_name = album['name']
        saved_timestamp = int(item['added_at'])

        # Insert/update artist and album information in the albums table
        # (Replace this with the appropriate function you have for inserting/updating the albums table)
        insert_or_update_album(conn, artist_name, album_name)

        # Insert/update saved album information in the saved_albums table
        cursor.execute('''INSERT OR IGNORE INTO saved_albums (artist_name, album_name, saved_timestamp)
                          VALUES (?, ?, ?)''',
                       (artist_name, album_name, saved_timestamp))
        conn.commit()

# Extract artist and album names from Spotify response JSON
def parse_saved_albums(response_json):
    albums = []
    for item in response_json['items']:
        album = item['album']
        artist_name = album['artists'][0]['name']
        album_name = album['name']
        albums.append((artist_name, album_name))
    return albums

# Search Spotify with query and query_type using the given access token
def search_spotify(query, query_type, spotify_token, max_retries=3, delay=2):
    headers = {
        'Authorization': f'Bearer {spotify_token}'
    }
    params = {
        'q': query,
        'type': query_type,
        'limit': 1
    }
    
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get('https://api.spotify.com/v1/search', headers=headers, params=params, timeout=10)
            return response.json()
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            print(f"Error: {e}. Retrying in {delay} seconds...")
            retries += 1
            time.sleep(delay)
    
    print(f"Failed to fetch data from Spotify for query: {query}. Max retries exceeded.")
    return None

# Updates artist and album Spotify URLs in the database
def update_artist_and_album_urls(conn, spotify_token):
    cursor = conn.cursor()
    cursor.execute("SELECT artist_id, artist_name, spotify_url FROM artists WHERE spotify_url IS NULL")
    artists = cursor.fetchall()

    headers = {
        'Authorization': f'Bearer {spotify_token}'
    }

    for artist_id, artist_name, artist_spotify_url in artists:
        artist_search_url = f'https://api.spotify.com/v1/search?q={urllib.parse.quote(artist_name)}&type=artist&limit=1'
        try:
            artist_response = requests.get(artist_search_url, headers=headers)
            artist_data = artist_response.json()

            if 'artists' in artist_data and artist_data['artists']['items']:
                artist_spotify_url = artist_data['artists']['items'][0]['external_urls']['spotify']
                cursor.execute("UPDATE artists SET spotify_url=? WHERE artist_id=?", (artist_spotify_url, artist_id))
                print(f"Updated Spotify URL for artist {artist_name}: {artist_spotify_url}")

        except Exception as e:
            print(f"Error fetching Spotify URL for artist {artist_name}: {e}")

    cursor.execute("SELECT album_id, artist_name, album_name, spotify_url FROM albums WHERE spotify_url IS NULL")
    albums = cursor.fetchall()

    for album_id, artist_name, album_name, album_spotify_url in albums:
        album_search_url = f'https://api.spotify.com/v1/search?q=album:{urllib.parse.quote(str(album_name))}%20artist:{urllib.parse.quote(str(artist_name))}&type=album&limit=1'

#        album_search_url = f'https://api.spotify.com/v1/search?q=album:{urllib.parse.quote(album_name)}%20artist:{urllib.parse.quote(artist_name)}&type=album&limit=1'
        try:
            album_response = requests.get(album_search_url, headers=headers)
            album_data = album_response.json()

            if 'albums' in album_data and album_data['albums']['items']:
                album_spotify_url = album_data['albums']['items'][0]['external_urls']['spotify']
                cursor.execute("UPDATE albums SET spotify_url=? WHERE album_id=?", (album_spotify_url, album_id))
                print(f"Updated Spotify URL for album {artist_name} - {album_name}: {album_spotify_url}")

        except Exception as e:
            print(f"Error fetching Spotify URL for album {artist_name} - {album_name}: {e}")

    conn.commit()
    print("Updated artist and album URLs.")


# Updates missing album data (release year, cover art) from Spotify API
def update_missing_album_data(conn, spotify_access_token):
    # Get albums with missing release year or cover art
    cursor = conn.cursor()
    cursor.execute("SELECT album_id, release_year, cover_art_url, spotify_url FROM albums WHERE release_year IS NULL OR cover_art_url IS NULL")
    albums_with_missing_data = cursor.fetchall()

    for album_id, release_year, cover_art_url, spotify_url in albums_with_missing_data:
        if not spotify_url:  # Skip if spotify_url is None
            continue

        # Extract Spotify album ID from the spotify_url
        spotify_album_id = spotify_url.split('/')[-1]

        # Fetch album info from Spotify API
        new_release_year, new_cover_art_url = get_album_info(spotify_access_token, spotify_album_id)

        # If either release year or cover art was missing and we found the new value, update the row
        if (release_year is None and new_release_year) or (cover_art_url is None and new_cover_art_url):
            cursor.execute(
                "UPDATE albums SET release_year = ?, cover_art_url = ? WHERE album_id = ?",
                (new_release_year or release_year, new_cover_art_url or cover_art_url, album_id)
            )

    conn.commit()

# Fetches album info (release year, cover art URL) from Spotify API
def get_album_info(spotify_access_token, spotify_album_id):
    url = f"https://api.spotify.com/v1/albums/{spotify_album_id}"
    headers = {"Authorization": f"Bearer {spotify_access_token}"}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        release_year = data.get("release_date", "").split("-")[0] or None
        cover_art_url = data["images"][0]["url"] if data["images"] else None
        return release_year, cover_art_url
    else:
        print(f"Error fetching album info from Spotify API. Status code: {response.status_code}")
        return None, None

# Converts Spotify URL to Spotify URI
def url_to_uri(spotify_url):
    if spotify_url is None:
        return None
    return spotify_url.replace("https://open.spotify.com/album/", "spotify:album:")

# Saves recently saved albums from the Spotify API to the intermediate_results table.
def save_recent_saved_albums(conn, token, last_update):
    response_json = get_recent_saved_albums(token, last_update)
    cursor = conn.cursor()
    cursor.execute("""
    INSERT OR REPLACE INTO intermediate_results (key, value)
    VALUES (?, ?)
    """, ("recent_saved_albums", json.dumps(response_json)))
    conn.commit()

# Parses and inserts saved albums from Spotify API response
def parse_and_insert_saved_albums(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM intermediate_results WHERE key = ?", ("recent_saved_albums",))
    response_json = json.loads(cursor.fetchone()[0])
    new_saved_albums = parse_saved_albums(response_json)
    insert_new_saved_albums(conn, new_saved_albums)

#  ------ NEW ---------
def update_spotify_data(conn, spotify_token):
    cursor = conn.cursor()
    headers = {
        'Authorization': f'Bearer {spotify_token}'
    }

    # Update artist and album URLs
    cursor.execute("SELECT artist_id, artist_name, spotify_url FROM artists WHERE spotify_url IS NULL")
    artists = cursor.fetchall()

    x = 0
    for artist_id, artist_name, artist_spotify_url in artists:
        artist_search_url = f'https://api.spotify.com/v1/search?q={urllib.parse.quote(artist_name)}&type=artist&limit=1'
        if x % 15:
            time.sleep(2)
        try:
            artist_response = requests.get(artist_search_url, headers=headers)
            artist_data = artist_response.json()

            if 'artists' in artist_data and artist_data['artists']['items']:
                artist_spotify_url = artist_data['artists']['items'][0]['external_urls']['spotify']
                cursor.execute("UPDATE artists SET spotify_url=? WHERE artist_id=?", (artist_spotify_url, artist_id))
                print(f"Updated Spotify URL for artist {artist_name}: {artist_spotify_url}")
        except Exception as e:
            print(f"Error fetching Spotify URL for artist {artist_name}: {e}")
        x += 1    
    cursor.execute("SELECT album_id, artist_name, album_name, spotify_url FROM albums WHERE spotify_url IS NULL OR release_year IS NULL OR cover_art_url IS NULL")
    albums = cursor.fetchall()

    for album_id, artist_name, album_name, album_spotify_url in albums:
        album_search_url = f'https://api.spotify.com/v1/search?q=album:{urllib.parse.quote(str(album_name))}%20artist:{urllib.parse.quote(str(artist_name))}&type=album&limit=1'
        if x % 15:
            time.sleep(2)
        try:
            album_response = requests.get(album_search_url, headers=headers)
            album_data = album_response.json()

            if 'albums' in album_data and album_data['albums']['items']:
                album_spotify_url = album_data['albums']['items'][0]['external_urls']['spotify']
                release_year = album_data['albums']['items'][0]['release_date'][:4]  # extracting the year
                cover_art_url = album_data['albums']['items'][0]['images'][0]['url'] if album_data['albums']['items'][0]['images'] else None
                release_type = album_data['albums']['items'][0]['album_type']
                cursor.execute("UPDATE albums SET spotify_url = ?, release_year = ?, cover_art_url = ?, release_type = ? WHERE album_id = ?", 
                               (album_spotify_url, release_year, cover_art_url, release_type, album_id))
                print(f"Updated Spotify URL, release year, release type, and cover art URL for album {artist_name} - {album_name}: {album_spotify_url}, {release_year}, {cover_art_url}")

        except Exception as e:
            print(f"Error fetching Spotify URL for album {artist_name} - {album_name}: {e}")
        x += 1 
    conn.commit()
    print("Updated artist and album URLs.")



# ----- Discogs functions -----
# Fetches the album cover art URL from the Discogs API for a given artist and album.
def get_discogs_cover_art_url(artist_name, album_name):
    try:
        discogs_search = d.search(artist=artist_name, release_title=album_name, type="release")
        results = discogs_search.page(1)

        if results:
            best_match = results[0]
            cover_art_url = best_match.thumb
            if cover_art_url:
                return cover_art_url
            else:
                print(f"Cover art not found for {artist_name} - {album_name}")
        else:
            print(f"Release not found for {artist_name} - {album_name}")

    except discogs_client.exceptions.HTTPError as e:
        print(f"Error fetching cover art from Discogs for {artist_name} - {album_name}: {e}")

    return None

# Fetches the artist image URL from the Discogs API for a given artist.
def get_discogs_artist_image_url(artist_name):
    results = d.search(artist_name, type='artist')

    if results and results.pages > 0:
        best_match = results[0]

        if 'thumb' in best_match.data:
            return best_match.data['thumb']
    return None



# ----- RateYourMusic functions ----
# Fetches genres from RateYourMusic using given RYM URL
def get_genres_from_rym(rym_url, use_scraperapi=True):
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": random.choice(referers)
    }

    if use_scraperapi:
        # Build the ScraperAPI URL
        url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={rym_url}"
    else:
        url = rym_url

    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching RYM page: {rym_url}")
            return None
    except ConnectionError as e:
        print(f"Connection error occurred: {e}")

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract primary and secondary genres
    primary_genres = [elem.get_text(strip=True) for elem in soup.select(".release_pri_genres a.genre")]
    secondary_genres = [elem.get_text(strip=True) for elem in soup.select(".release_sec_genres a.genre")]

    genres = primary_genres + secondary_genres
    return ", ".join(genres) if genres else False

# Gets genres for an album from RateYourMusic
def get_rym_genres(artist_name, album_name, network, retry_attempts=3, retry_delay=5):
    search_query = f"{artist_name} - {album_name}"

    for attempt in range(retry_attempts):
        try:
            album_infos = network.get_album_infos(name=search_query)
            genres = album_infos["Genres"]
            genres = genres.replace('\n', ', ')  # Replace line breaks with a comma and space
            return genres
        except (AttributeError, IndexError, TypeError):
            print(f"Couldn't fetch RYM genres for {artist_name} - {album_name}")
            return None
        except selenium.common.exceptions.WebDriverException:
            if attempt < retry_attempts - 1:
                print(f"Attempt {attempt+1} of {retry_attempts} failed. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"All attempts failed for {artist_name} - {album_name}. Please check your network connection and ensure rateyourmusic.com is online.")
                return None

# Searches for an album on RateYourMusic and returns its URL
def search_album_on_rym(artist, album_title):
    search_term = quote_plus(f"{artist} {album_title}")
    search_url = f"https://rateyourmusic.com/search?searchtype=l&searchterm={search_term}"
    response = requests.get(search_url)

    if response.status_code != 200:
        print(f"Error fetching search URL: {search_url}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    search_result = soup.find("div", {"class": "disco_release"})
    if not search_result:
        print(f"Album not found: {artist} - {album_title}")
        return None

    album_page_url = search_result.find("a")["href"]
    return "https://rateyourmusic.com" + album_page_url

# Updates albums with RYM genres if not already set
def update_rym_genres(conn, use_scraperapi=False):
    if use_scraperapi == True:
        print("using scraperAPI")
    cursor = conn.cursor()

    skipped_albums_file = "skipped_albums.txt"
    skipped_albums = load_skipped_albums_list(skipped_albums_file)
    cursor.execute("SELECT album_name, artist_name, release_type, album_id, num_tracks FROM albums WHERE genre IS NULL")
    albums = cursor.fetchall()
    network = rymscraper.RymNetwork(headless=True)

    x = 1
    for album in albums:
        album_name, artist_name, release_type, album_id, num_tracks = album

        if (artist_name, album_name) in skipped_albums:
            continue

        if x % 3 == 0 and use_scraperapi==False:
            time.sleep(30)
        x += 1

        rym_url_dashes, rym_url_underscores = generate_rym_url(conn, artist_name, album_name, release_type, album_id, num_tracks)
        
        if use_scraperapi:
            try:
                genres = get_genres_from_rym(rym_url_dashes, use_scraperapi=True)
                if not genres:
                    genres = get_genres_from_rym(rym_url_underscores, use_scraperapi=True)
            except requests.exceptions.ConnectTimeout:
                print(f"Timeout when trying to get genres for {artist_name} - {album_name}")
                continue
        else:
            genres = get_rym_genres(artist_name, album_name, network)

        if genres:
            cursor.execute("UPDATE albums SET genre=? WHERE album_name=? AND artist_name=?", (genres, album_name, artist_name))
            conn.commit()
            print(f"Updated RYM genres for {artist_name} - {album_name}: {genres}")
        else:
            print(f"Couldn't fetch RYM genres for {artist_name} - {album_name}")
            skipped_albums.add((artist_name, album_name))
            save_skipped_albums_list(skipped_albums_file, skipped_albums)

    network.browser.close()
    network.browser.quit()



# Generates two RYM URLs for a given album (with dashes and underscores)
def generate_rym_url(conn, artist, album_title, release_type, album_id, num_tracks):
    def format_name(name, replace_char="-"):
        return name.strip().replace(" ", replace_char).lower()

    cursor = conn.cursor()
    formatted_artist = clean_for_rym_url(format_name(artist))

    release_type_mapping = {"album": "album", "ep": "ep", "single": "single"}

    if release_type is None or release_type == 'Single' or release_type == 'N/A':
        if num_tracks is None:
            new_release_type = "Album"
        elif num_tracks > 3:
            new_release_type = "Album"
        else:
            new_release_type = "Single"

        if new_release_type != release_type:
            cursor.execute("UPDATE albums SET release_type = ? WHERE album_id = ?", (new_release_type, album_id))
            conn.commit()
            print(f"Corrected release type for album {album_id}: {new_release_type}")

        release_type = new_release_type

    formatted_release_type = release_type_mapping.get(release_type.lower() if release_type else None, "album")

    # First attempt with dashes
    formatted_album_dashes = clean_for_rym_url(format_name(remove_special_editions(album_title), replace_char="-"))
    rym_url_dashes = f"https://rateyourmusic.com/release/{formatted_release_type}/{formatted_artist}/{formatted_album_dashes}/"

    # Second attempt with underscores
    formatted_album_underscores = clean_for_rym_url(format_name(remove_special_editions(album_title), replace_char="_"))
    rym_url_underscores = f"https://rateyourmusic.com/release/{formatted_release_type}/{formatted_artist}/{formatted_album_underscores}/"

    return rym_url_dashes, rym_url_underscores

# Normalizes and cleans up text for use in RYM URL generation
def clean_for_rym_url(text):
    replacements = [
        ("!", ""),          # Remove exclamation marks
        ("&", "and"),       # Replace '&' with 'and'
        (",", ""),          # Remove commas
        (":", ""),          
        ("'", ""),          
        ("...", "-"),         
        (".", ""),          
        (" - ", "-"), 
        ("/ ", ""),           
        ("/", "-"),          
        ("?", ""),          
        ("(", ""),          
        (")", ""),
        ("#", "-")         
    ]
    normalized_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    for old, new in replacements:
        normalized_text = normalized_text.replace(old, new)
    return normalized_text


# ----- Database functions ----- 
# Counts the number of entries in the specified SQLite3 table
def count_entries_in_table(table_name):
    conn = sqlite3.connect('MusicLibrary.db')
    cursor = conn.cursor()

    cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
    count = cursor.fetchone()[0]

    conn.close()

    return count

# Establishes a connection to the SQLite3 database with the specified name
def connect_to_database(db_name):
    conn = sqlite3.connect(db_name)
    return conn

# Prints saved album data in a user-friendly format
def print_saved_albums(albums_data):
    for item in albums_data['items']:
        album = item['album']
        print(f"Album: {album['name']}, Artist: {album['artists'][0]['name']}")

def check_if_populated(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    return count > 0

def check_if_table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    return cursor.fetchone() is not None

# Main function that connects to the database, updates data, and closes the connection
def main():
    db_name = "MusicLibrary.db"
    conn = sqlite3.connect(db_name)

    if not check_if_table_exists(conn, "albums"):
        print("First time running in this directory. Setting up the database...")
        first_time_functions(conn)
        print("Setup complete. Run again to perform updates.")
    else:
        conn = sqlite3.connect(db_name)
        print("Database exists. Checking if it's populated...")
        if (check_if_table_exists(conn, "albums") and 
            check_if_table_exists(conn, "artists") and 
            check_if_table_exists(conn, "saved_albums")):
            if (check_if_populated(conn, "albums") and 
                check_if_populated(conn, "artists") and 
                check_if_populated(conn, "saved_albums")):
                print("Database is populated. Running other functions...")
                if should_execute_function(conn):
                     update_databases(conn, LASTFM_USER, LASTFM_API_KEY)
                     set_last_executed_date(conn, datetime.datetime.now())
            else:
                print("Database is not fully populated. Setting up the database...")
                first_time_functions(conn)
                print("Setup complete. Run again to perform updates.")
        else:
            print("First time running in this directory. Setting up the database...")
            first_time_functions(conn)
            print("Setup complete. Run again to perform updates.")
    conn.close()

# Creates a table for saved albums if it doesn't exist
def create_saved_albums_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS saved_albums (
                        album_id TEXT PRIMARY KEY,
                        album_name TEXT,
                        artist_name TEXT,
                        added_at TIMESTAMP
                     )''')
    conn.commit()

# Shows the schema for the specified table in the SQLite3 database
def show_table_schema(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name});")
    schema = cursor.fetchall()
    return schema

    print(f"Schema for table '{table}':")
    schema = show_table_schema(cursor, table)
    for column in schema:
        print(f"  {column[1]} ({column[2]})")
    print()

# Selects all rows from the specified table in the SQLite3 database.
def select_all_from_table(cursor, table_name):
    cursor.execute(f"SELECT * FROM {table_name};")
    rows = cursor.fetchall()
    return rows

# Creates and populates the albums table in the SQLite3 database
def create_and_populate_albums_table(conn):
    cursor = conn.cursor()

    # Create the albums table
    # cursor.execute('''CREATE TABLE IF NOT EXISTS albums (
    #                     album_id TEXT PRIMARY KEY,
    #                     album_name TEXT,
    #                     artist_id INTEGER,
    #                     artist_name TEXT,
    #                     scrobble_count INTEGER,
    #                     added_at TIMESTAMP,
    #                     genres TEXT,
    #                     FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    #                   )''')

    # Populate the albums table with unique albums from the tracks table
    cursor.execute('''INSERT OR IGNORE INTO albums (album_id, album_name, artist_name, scrobble_count, added_at, genres)
                      SELECT MIN(track_id), album_name, artist_name, MIN(scrobble_count), MIN(timestamp), ''
                      FROM (
                            SELECT track_id, album_name, artist_name, COUNT(*) as scrobble_count, MIN(timestamp) as timestamp
                            FROM tracks
                            WHERE album_name != ''
                            GROUP BY track_id
                           )
                      GROUP BY album_name, artist_name
                   ''')

    conn.commit()

# Create a new albums table with the correct schema and copy data from the old table
def modify_albums_table(conn):
    cursor = conn.cursor()

    # Create a new table with the correct schema
    cursor.execute('''CREATE TABLE IF NOT EXISTS albums_new (
                        album_id TEXT PRIMARY KEY,
                        album_name TEXT,
                        artist_id TEXT,
                        added_at TIMESTAMP,
                        scrobble_count INTEGER,
                        cover_art_url TEXT,
                        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
                     )''')

    # Copy data from the old table to the new table
    cursor.execute('''INSERT INTO albums_new (album_id, album_name, artist_id, added_at, scrobble_count)
                      SELECT album_id, album_name, artist_id, added_at, scrobble_count FROM albums
                   ''')

    # Drop the old table
    cursor.execute("DROP TABLE albums")

    # Rename the new table to the original table name
    cursor.execute("ALTER TABLE albums_new RENAME TO albums")

    conn.commit()

# Insert unique artist_ids from the albums table into the artists table
def populate_artists_table(conn):
    cursor = conn.cursor()
    cursor.execute('''INSERT OR IGNORE INTO artists (artist_id)
                      SELECT DISTINCT artist_id
                      FROM albums
                   ''')
    conn.commit()

# Update the scrobble count for each album in the new_albums table
def update_album_scrobble_counts(conn):
    cursor = conn.cursor()

    # Retrieve albums
    cursor.execute("SELECT album_id, artist_name, album_name, num_tracks FROM new_albums")
    albums = cursor.fetchall()

    for album_id, artist_name, album_name, num_tracks in albums:
        # Find the sum of scrobbles for each album, considering similar album names
        cursor.execute('''SELECT SUM(scrobble_count)
                          FROM new_tracks
                          WHERE artist_name = ? AND
                                (album_name = ? OR
                                 album_name LIKE ? || ' (%)' OR
                                 album_name LIKE ? || ' [%]')
                       ''', (artist_name, album_name, album_name, album_name))

        sum_scrobbles = cursor.fetchone()[0]

        if num_tracks:
            avg_scrobbles = int(sum_scrobbles / num_tracks)
            cursor.execute("UPDATE new_albums SET scrobble_count = ? WHERE album_id = ?", (avg_scrobbles, album_id))

    conn.commit()

# Update the artist_id in the albums table based on the artist_name
def update_album_artist_ids(conn):
    cursor = conn.cursor()

    # Update the artist_id in the albums table based on the artist_name
    cursor.execute('''UPDATE albums SET artist_id = (
                        SELECT artist_id FROM artists WHERE artists.artist_name = albums.artist_name)
                   ''')

    conn.commit()

# Marks albums in the database as saved if they exist in the saved_albums table.
def update_saved_spotify_albums(conn):
    cursor = conn.cursor()
    cursor.execute("""UPDATE albums
                      SET saved = 'saved'
                      WHERE EXISTS (SELECT 1 FROM saved_albums
                                    WHERE IGNORE_PARENTHESIS_AND_BRACKETS(albums.album_name, saved_albums.album_name)
                                    AND albums.artist_name = saved_albums.artist_name)
                   """)
    conn.commit()

# Creates a table for storing update timestamps
def create_updates_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS updates (
                        source TEXT PRIMARY KEY,
                        last_update INTEGER
                      )''')
    conn.commit()

# Retrieves the last update timestamp for a specific source
def get_last_update_timestamp(conn, source):
    cursor = conn.cursor()
    cursor.execute("SELECT last_update FROM updates WHERE source = ?", (source,))
    result = cursor.fetchone()
    timestamp = result[0] if result else None

    # Store the timestamp in the intermediate_results table
    store_intermediate_result(conn, f"{source}_last_update", timestamp)

    return timestamp

# Sets the last update timestamp for a specific source
def set_last_update_timestamp(conn, source, timestamp):
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO updates (source, last_update) VALUES (?, ?)", (source, timestamp))
    conn.commit()

# Creates a new table for storing track information
def create_new_tracks_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS new_tracks (
                        track_id INTEGER PRIMARY KEY,
                        artist_name TEXT,
                        album_name TEXT,
                        track_name TEXT,
                        scrobble_count INTEGER
                      )''')
    conn.commit()

# Populates the new_tracks table with data from the new_playlist table
def populate_new_tracks_table(conn):
    cursor = conn.cursor()
    cursor.execute('''INSERT OR IGNORE INTO new_tracks (artist_name, album_name, track_name, scrobble_count)
                      SELECT artist_name, album_name, track_name, COUNT(*) as scrobble_count
                      FROM new_playlist
                      GROUP BY artist_name, album_name, track_name
                   ''')
    conn.commit()

# Inserts scrobbles into the new_playlist table
def insert_scrobbles_into_new_playlist(conn, scrobbles):
    cursor = conn.cursor()
    
    if scrobbles is None:
        print("No scrobbles to insert.")
        return
    

    # Clear the new_playlist table
    cursor.execute("DELETE FROM new_playlist")
    conn.commit()

    # Insert scrobbles into the new_playlist table
    for scrobble in scrobbles:
        cursor.execute('''INSERT INTO new_playlist (date, artist_name, album_name, track_name)
                          VALUES (?, ?, ?, ?)''', (scrobble['date'], scrobble['artist_name'], scrobble['album_name'], scrobble['track_name']))
    conn.commit()

# Creates a new table for storing playlist data
def create_new_playlist_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS new_playlist (
                        id INTEGER PRIMARY KEY,
                        date INTEGER,
                        artist_name TEXT,
                        album_name TEXT,
                        track_name TEXT
                      )''')
    conn.commit()

# Create new_albums table with album metadata columns
def create_new_albums_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS new_albums (
                        album_id INTEGER PRIMARY KEY,
                        album_name TEXT,
                        artist_id INTEGER,
                        artist_name TEXT,
                        scrobble_count INTEGER,
                        release_year INTEGER,
                        saved TEXT,
                        genres TEXT,
                        num_tracks INTEGER,
                        cover_art_url TEXT,
                        added_at TIMESTAMP,
                        spotify_url TEXT,
                        genre TEXT,
                        last_played INTEGER
                      )''')
    conn.commit()

# Insert unique artist and album pairs from new_tracks into new_albums
def populate_new_albums_table(conn):
    cursor = conn.cursor()
    cursor.execute('''INSERT OR IGNORE INTO new_albums (artist_name, album_name)
                      SELECT DISTINCT artist_name, album_name
                      FROM new_tracks
                   ''')
    conn.commit()

# Insert new saved albums into saved_albums table
def insert_new_saved_albums(conn, new_saved_albums):
    print(new_saved_albums[:5])
    cursor = conn.cursor()

    for album in new_saved_albums:
        artist_name, album_name = album
        
        # Check if the album already exists in the saved_albums table
        cursor.execute('''SELECT COUNT(*) FROM saved_albums
                          WHERE artist_name = ? AND album_name = ?''', (artist_name, album_name))
        album_exists = cursor.fetchone()[0]

        # If the album doesn't exist, insert it into the saved_albums table
        if not album_exists:
            cursor.execute('''INSERT INTO saved_albums (artist_name, album_name)
                              VALUES (?, ?)''', (artist_name, album_name))
            print(f"Inserted new saved album: {artist_name} - {album_name}")

    conn.commit()

# Update num_tracks for each album in new_albums table
def update_album_track_counts(conn):
    cursor = conn.cursor()

    # Retrieve albums
    cursor.execute("SELECT album_id, artist_name, album_name FROM new_albums")
    albums = cursor.fetchall()

    for album_id, artist_name, album_name in albums:
        # Count the number of tracks for each album, considering similar album names
        cursor.execute('''SELECT COUNT(*)
                          FROM new_tracks
                          WHERE artist_name = ? AND
                                (album_name = ? OR
                                 album_name LIKE ? || ' (%)' OR
                                 album_name LIKE ? || ' [%]')
                       ''', (artist_name, album_name, album_name, album_name))

        num_tracks = cursor.fetchone()[0]

        if num_tracks:
            cursor.execute("UPDATE new_albums SET num_tracks = ? WHERE album_id = ?", (num_tracks, album_id))
            print(f"Updated track count for {artist_name} - {album_name}: {num_tracks}")

    conn.commit()

# Append new albums to the albums table or update their scrobble counts
def append_and_update_albums(conn):
    cursor = conn.cursor()

    # Fetch albums from the new_albums table
    cursor.execute("SELECT artist_name, album_name, scrobble_count, num_tracks, release_year, cover_art_url FROM new_albums")
    new_albums = cursor.fetchall()

    for artist_name, album_name, scrobble_count, num_tracks, release_year, cover_art_url in new_albums:
        # Check if the album exists in the albums table
        cursor.execute("SELECT album_id, scrobble_count FROM albums WHERE artist_name=? AND album_name=?", (artist_name, album_name))
        existing_album = cursor.fetchone()

        if existing_album:
            # If the album exists, add the scrobble_count value from the new_albums table
            album_id, existing_scrobble_count = existing_album
            updated_scrobble_count = existing_scrobble_count + scrobble_count
            cursor.execute("UPDATE albums SET scrobble_count=? WHERE album_id=?", (updated_scrobble_count, album_id))
            print(f"Updated scrobble_count for {artist_name} - {album_name}: {updated_scrobble_count}")
        else:
            # If the album doesn't exist, insert it into the albums table
            cursor.execute('''INSERT INTO albums (artist_name, album_name, scrobble_count, num_tracks, release_year, cover_art_url)
                              VALUES (?, ?, ?, ?, ?, ?)''',
                           (artist_name, album_name, scrobble_count, num_tracks, release_year, cover_art_url))
            print(f"Inserted {artist_name} - {album_name} into albums table")

    conn.commit()

# Update albums with missing artist_id or album_id
def update_albums_with_missing_ids(conn):
    cursor = conn.cursor()

    # Retrieve albums with missing artist_id or album_id
    cursor.execute("SELECT album_id, artist_name, album_name FROM albums WHERE artist_id IS NULL OR album_id IS NULL")
    albums = cursor.fetchall()

    for album_id, artist_name, album_name in albums:
        # Check if the artist exists in the artists table
        cursor.execute("SELECT artist_id FROM artists WHERE artist_name=?", (artist_name,))
        artist = cursor.fetchone()

        if not artist:
            # If the artist doesn't exist, insert the artist into the artists table and get the artist_id
            cursor.execute("INSERT INTO artists (artist_name) VALUES (?)", (artist_name,))
            artist_id = cursor.lastrowid
            print(f"Inserted {artist_name} into artists table with artist_id {artist_id}")
        else:
            artist_id = artist[0]

        # Update the albums table with the correct artist_id and album_id
        cursor.execute("UPDATE albums SET artist_id = ? WHERE album_id = ?", (artist_id, album_id))
        print(f"Updated {artist_name} - {album_name} with artist_id {artist_id}")

    conn.commit()

# Delete unwanted albums and orphaned artists
def delete_unwanted_albums_and_artists(conn):
    cursor = conn.cursor()

    # Delete albums with num_tracks = 1 and scrobble_count = 1, unless saved is not NULL
    cursor.execute("DELETE FROM albums WHERE num_tracks = 1 AND scrobble_count = 1 AND saved IS NULL")
    
    deleted_album_rows = cursor.rowcount
    print(f"Deleted {deleted_album_rows} unwanted albums")

    # Delete artists not found in the albums table
    cursor.execute('''DELETE FROM artists WHERE NOT EXISTS 
                      (SELECT 1 FROM albums WHERE albums.artist_id = artists.artist_id)''')
    
    deleted_artist_rows = cursor.rowcount
    print(f"Deleted {deleted_artist_rows} orphaned artists")

    conn.commit()    

# Drops specified tables from the database
def drop_tables(conn, table_names):
    cursor = conn.cursor()

    for table_name in table_names:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Dropped table: {table_name}")
        except Exception as e:
            print(f"Error dropping table {table_name}: {e}")

    conn.commit()

# Updates the last_played timestamp for each album in the albums table
def update_last_played_for_albums(conn):
    # Create a new cursor object
    cursor = conn.cursor()

    # Get all albums from the albums table
    cursor.execute("SELECT artist_name, album_name FROM albums")
    albums = cursor.fetchall()

    for artist_name, album_name in albums:
        modified_album_name = remove_special_editions(album_name)
        # Find the latest timestamp for the album in the playlists table
        cursor.execute("""
            SELECT MAX(timestamp) FROM playlist
            WHERE artist_name = ? AND album_name = ?
        """, (artist_name, modified_album_name))
        last_played = cursor.fetchone()[0]

        # Update the last_played column for the album in the albums table
        cursor.execute("""
            UPDATE albums
            SET last_played = ?
            WHERE artist_name = ? AND album_name = ?
        """, (last_played, artist_name, album_name))

    # Commit the changes and close the cursor
    conn.commit()
    cursor.close()

# Cleans special edition tags from album names in the albums table
def clean_album_names(conn):
    # Create a new cursor object
    cursor = conn.cursor()

    # Fetch all the album names from the albums table
    cursor.execute("SELECT id, album_name FROM albums")
    albums = cursor.fetchall()

    # Iterate through the albums and clean their names using remove_special_editions
    for album in albums:
        album_id = album[0]
        original_album_name = album[1]
        cleaned_album_name = remove_special_editions(original_album_name)

        # Update the album name in the albums table
        cursor.execute("UPDATE albums SET album_name=? WHERE id=?", (cleaned_album_name, album_id))

    # Commit the changes to the database
    conn.commit()

    print("Album names cleaned.")    

# Deletes duplicate scrobbles from the playlist table based on timestamp
def delete_duplicate_scrobbles(conn):
    cursor = conn.cursor()
    
    # Delete duplicate rows based on the same timestamp
    query = """
    DELETE FROM playlist
    WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM playlist
        GROUP BY timestamp
    )
    """
    
    cursor.execute(query)
    conn.commit()
    
    # Report the number of deleted rows
    deleted_rows_count = cursor.rowcount
    print(f"Deleted {deleted_rows_count} duplicate scrobbles")

# Returns the last executed date of the script from the database
def get_last_executed_date(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT date FROM last_updated")
    result = cursor.fetchone()
    if result is not None and result[0] is not None:
        date_str = result[0]
        return datetime.datetime.strptime(date_str, '%Y-%m-%d')
    else:
        return None

# Sets the last executed date of the script in the database
def set_last_executed_date(conn, date):
    cursor = conn.cursor()
    cursor.execute("UPDATE last_updated SET date = ?", (date.strftime('%Y-%m-%d'),))
    conn.commit()

# Determines if the function should be executed based on the last executed date
def should_execute_function(conn):
    last_executed_date = get_last_executed_date(conn)
    current_date = datetime.datetime.now().date()
    
    # Check if last_executed_date is not None
    if last_executed_date:
        return current_date != last_executed_date.date()
    else:
        # Return True or whatever is appropriate when last_executed_date is None
        return True


# Inserts an initial last executed date into the database
def insert_initial_last_executed_date(conn):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO last_updated (date) VALUES (?)", ('1970-01-01',))
    conn.commit()

# Updates the last_played timestamp for new and existing albums in the database.
def update_last_played(conn):
    cursor = conn.cursor()

    # Update 'last_played' in new_albums
    cursor.execute("""
        UPDATE new_albums
        SET last_played = (
            SELECT MAX(date)
            FROM new_playlist
            WHERE new_playlist.artist_name = new_albums.artist_name AND new_playlist.album_name = new_albums.album_name
        )
    """)

    # Update 'last_played' for existing albums
    cursor.execute("""
        UPDATE albums
        SET last_played = (
            SELECT last_played
            FROM new_albums
            WHERE new_albums.artist_name = albums.artist_name AND new_albums.album_name = albums.album_name
        )
        WHERE EXISTS (
            SELECT 1
            FROM new_albums
            WHERE new_albums.artist_name = albums.artist_name AND new_albums.album_name = albums.album_name
        )
    """)

    # Insert 'last_played' for new albums
    cursor.execute("""
        INSERT INTO albums (artist_name, album_name, last_played)
        SELECT artist_name, album_name, last_played
        FROM new_albums
        WHERE NOT EXISTS (
            SELECT 1
            FROM albums
            WHERE new_albums.artist_name = albums.artist_name AND new_albums.album_name = albums.album_name
        )
    """)

    conn.commit()

# Creates an intermediate_results table in the database
def create_intermediate_results_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS intermediate_results (
        key TEXT PRIMARY KEY,
        value BLOB
    )
    """)
    conn.commit()

# Stores an intermediate result in the intermediate_results table
def store_intermediate_result(conn, key, value):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO intermediate_results (key, value)
        VALUES (?, ?)
    """, (key, value))
    conn.commit()

# Retrieves an intermediate result from the intermediate_results table
def get_intermediate_result(conn, key):
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM intermediate_results WHERE key = ?", (key,))
    result = cursor.fetchone()
    
    if result:
        print(f"Key '{key}' found in intermediate_results table with value: {result[0]}")
        return result[0]
    else:
        print(f"Key '{key}' not found in intermediate_results table")
        return None

# Creates a table to store executed functions status
def create_executed_functions_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS executed_functions (
        function_name TEXT PRIMARY KEY,
        executed INTEGER
    );
    """)
    conn.commit()

# Saves the Spotify access token to the database
def save_spotify_access_token(conn, client_id, client_secret, redirect_uri, scope):
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
    store_intermediate_result(conn, "spotify_access_token", access_token)
    return access_token

# Updates various tables in the database with new data from Last.fm and Spotify
def update_databases(conn, lastfm_username, lastfm_api_key):
    create_executed_functions_table(conn)

    def function_executed(conn, function_name):
        cursor = conn.cursor()
        cursor.execute("SELECT executed FROM executed_functions WHERE function_name=?", (function_name,))
        result = cursor.fetchone()
        return result and result[0]

    def set_function_executed(conn, function_name):
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO executed_functions (function_name, executed) VALUES (?, 1)", (function_name,))
        conn.commit()

    def execute_if_not_done(func_name, func, conn, *args, **kwargs):
        if not function_executed(conn, func_name):
            func(conn, *args, **kwargs)
            set_function_executed(conn, func_name)
            print(f"----- {func_name} completed")

    print('----- starting update')

    execute_if_not_done('fetch_timestamp_lastfm', get_last_update_timestamp, conn, "lastfm")
    execute_if_not_done('fetch_timestamp_spotify', get_last_update_timestamp, conn, "spotify")
    lastfm_last_update = get_intermediate_result(conn, "lastfm_last_update")
    spotify_last_update = get_intermediate_result(conn, "spotify_last_update")

    execute_if_not_done('spotify_access_token', save_spotify_access_token, conn, SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI, SPOTIFY_SCOPE)
    stoken = get_intermediate_result(conn, "spotify_access_token")
    execute_if_not_done("save_recent_saved_albums", save_recent_saved_albums, conn, stoken, spotify_last_update)
    execute_if_not_done("parse_and_insert_saved_albums", parse_and_insert_saved_albums, conn)
    execute_if_not_done("set_last_update_timestamp_spotify", set_last_update_timestamp, conn, "spotify", int(time.time()))
    execute_if_not_done("create_new_tracks_table",create_new_tracks_table, conn)
    execute_if_not_done("create_new_playlist_table",create_new_playlist_table, conn)
    execute_if_not_done("create_new_albums_table",create_new_albums_table, conn)
    print(lastfm_last_update)
    #execute_if_not_done('get_new_scrobbles', fetch_lastfm_scrobbles, conn, api_key=LASTFM_API_KEY, api_secret=LASTFM_SECRET, username=LASTFM_USER, from_timestamp=lastfm_last_update)
    new_scrob = fetch_lastfm_scrobbles(conn, LASTFM_API_KEY, LASTFM_SECRET, LASTFM_USER, from_timestamp=lastfm_last_update)
    insert_scrobbles_into_new_playlist(conn, new_scrob)

    execute_if_not_done("insert_scrobbles_into_new_playlist",insert_scrobbles_into_new_playlist, conn, new_scrob)
    execute_if_not_done( "populate_new_tracks_table" , populate_new_tracks_table, conn)
    execute_if_not_done( "populate_new_albums_table" , populate_new_albums_table, conn) 
    execute_if_not_done( "update_album_track_counts" , update_album_track_counts, conn)
    execute_if_not_done( "update_album_scrobble_counts", update_album_scrobble_counts, conn)
    
    execute_if_not_done( "set_last_update_timestamp_lastfm" ,set_last_update_timestamp, conn, "lastfm", int(time.time()))
    execute_if_not_done( "append_and_update_albums" , append_and_update_albums, conn)
    execute_if_not_done( "update_albums_with_missing_ids" ,update_albums_with_missing_ids, conn)
    execute_if_not_done( "update_last_played",update_last_played, conn)
    execute_if_not_done( "update_artist_and_album_urls" ,update_artist_and_album_urls, conn, stoken)

    execute_if_not_done( "delete_unwanted_albums_and_artists", delete_unwanted_albums_and_artists, conn)
    execute_if_not_done( "update data from spotify", update_spotify_data, conn, stoken)
    execute_if_not_done( "update_albums_with_lastfm_release_years" ,update_albums_with_lastfm_release_years, conn, LASTFM_API_KEY)
    execute_if_not_done( "update_album_mbid" , update_album_mbid, conn)
    execute_if_not_done( "update_release_info" , update_release_info, conn)
    execute_if_not_done( "update_rym_genres" , update_rym_genres, conn, use_scraperapi=USE_SCRAPER)
    execute_if_not_done( "update_albums_with_cover_arts" ,update_albums_with_cover_arts, conn, LASTFM_API_KEY)
    execute_if_not_done( "update_artists_with_images",update_artists_with_images, conn)

    print('----- cleaning up:')
    table_names_to_drop = ['new_playlist', 'new_tracks', 'new_albums']
    execute_if_not_done("drop_tables", drop_tables, conn, table_names_to_drop)

    reset_executed_functions(conn)
    print('----- update complete')

# Resets the executed status of all functions in executed_functions table
def reset_executed_functions(conn):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE executed_functions
        SET executed = 0
    """)
    conn.commit()

# Drops the 'genres' column from the 'albums' table
def drop_genres_column(conn):
    # Connect to the SQLite database
    cursor = conn.cursor()

    # Check if the table has the genres column
    cursor.execute("PRAGMA table_info(albums)")
    columns = cursor.fetchall()
    has_genres = any(col[1] == 'genres' for col in columns)

    if not has_genres:
        print("The 'genres' column does not exist in the 'albums' table.")
        return

    # Create a new table without the genres column
    cursor.execute("""
    CREATE TABLE albums_new (
        album_id INTEGER PRIMARY KEY,
        album_name TEXT ,
        artist_id INTEGER,
        artist_name TEXT ,
        scrobble_count INTEGER,
        release_year INTEGER ,
        saved TEXT ,
        num_tracks INTEGER ,
        cover_art_url TEXT,
        added_at TIMESTAMP,
        spotify_url TEXT ,
        rym_genre TEXT ,
        last_played INTEGER ,
        spotify_uri TEXT ,
        mbid TEXT ,
        release_type TEXT ,
        country TEXT,
        release_length INTEGER ,
        tracks_mb INTEGER
    )
    """)

    # Copy the data from the old table to the new table
    cursor.execute("""
    INSERT INTO albums_new (album_id, album_name, artist_id, artist_name, scrobble_count, release_year, saved, num_tracks, cover_art_url, added_at, spotify_url, rym_genre, last_played, spotify_uri, mbid, release_type, country, release_length, tracks_mb)
    SELECT album_id, album_name, artist_id, artist_name, scrobble_count, release_year, saved, num_tracks, cover_art_url, added_at, spotify_url, rym_genre, last_played, spotify_uri, mbid, release_type, country, release_length, tracks_mb FROM albums
    """)

    # Drop the old table
    cursor.execute("DROP TABLE albums")

    # Rename the new table to the original name
    cursor.execute("ALTER TABLE albums_new RENAME TO albums")

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

# Updates album_id values in the 'albums' table to have sequential numbering
def update_album_ids(conn):
    cursor = conn.cursor()
    
    # Fetch all records from the albums table
    cursor.execute("SELECT * FROM albums")
    albums = cursor.fetchall()
    
    # Create a temporary table with the new album_id values
    cursor.execute("""
    CREATE TEMPORARY TABLE temp_albums (
        album_id INTEGER PRIMARY KEY ,
        album_name TEXT ,
        artist_id INTEGER,
        artist_name TEXT ,
        scrobble_count INTEGER,
        release_year INTEGER ,
        saved TEXT ,
        num_tracks INTEGER ,
        cover_art_url TEXT,
        added_at TIMESTAMP,
        spotify_url TEXT ,
        rym_genre TEXT ,
        last_played INTEGER ,
        spotify_uri TEXT ,
        mbid TEXT ,
        release_type TEXT ,
        country TEXT,
        release_length INTEGER ,
        tracks_mb INTEGER
    )
    """)
    
    # Insert records into the temporary table with updated album_id values
    for index, album in enumerate(albums, start=1):
        new_album_id = index
        old_album_id = album[0]
        
        cursor.execute("""
        INSERT INTO temp_albums (album_id, album_name, artist_id, artist_name, scrobble_count, release_year, saved, num_tracks, cover_art_url, added_at, spotify_url, rym_genre, last_played, spotify_uri, mbid, release_type, country, release_length, tracks_mb)
        SELECT ?, album_name, artist_id, artist_name, scrobble_count, release_year, saved, num_tracks, cover_art_url, added_at, spotify_url, rym_genre, last_played, spotify_uri, mbid, release_type, country, release_length, tracks_mb
        FROM albums WHERE album_id = ?
        """, (new_album_id, old_album_id))

    # Drop the original albums table
    cursor.execute("DROP TABLE albums")
    
    # Rename the temporary table to albums
    cursor.execute("ALTER TABLE temp_albums RENAME TO albums")
    
    conn.commit()

# ----------- NEW ------------
def add_latest_timestamp_to_updates(conn):
    cursor = conn.cursor()
    
    # Fetch the maximum timestamp from the 'playlist' table
    cursor.execute("SELECT MAX(timestamp) FROM playlist")
    max_timestamp = cursor.fetchone()[0]
    
    if max_timestamp is not None:
        # If a maximum timestamp was found, insert it into the 'updates' table
        insert_query = """
        INSERT INTO updates (source, last_update)
        VALUES (?, ?)
        """
        cursor.execute(insert_query, ('lastfm', max_timestamp))
        conn.commit()
        print(f"Successfully saved the latest timestamp ({max_timestamp}) to the updates table.")
    else:
        print("No data found in the playlist table.")

# Builds database with all the required tables
def setup_database(conn, db_name):
 #   conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS artists (
        artist_id INTEGER PRIMARY KEY,
        artist_name TEXT,
        artist_image TEXT,
        spotify_url TEXT,
        genre TEXT,
        last_played INTEGER
        -- FOREIGN KEY(artist_id) REFERENCES albums(artist_id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS albums (
        album_id INTEGER PRIMARY KEY AUTOINCREMENT,
        album_name TEXT,
        artist_id INTEGER,
        artist_name TEXT,
        scrobble_count INTEGER,
        release_year INTEGER,
        saved TEXT,
        num_tracks INTEGER,
        genre TEXT,
        release_type TEXT,
        cover_art_url TEXT,
        spotify_url TEXT,
        last_played INTEGER,
        spotify_uri TEXT,
        mbid TEXT,
        country TEXT,
        release_length INTEGER,
        tracks_mb INTEGER,
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    )
    ''')



    cursor.execute('''
    CREATE TABLE IF NOT EXISTS executed_functions (
        function_name TEXT PRIMARY KEY,
        executed INTEGER
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS intermediate_results (
        key TEXT PRIMARY KEY,
        value BLOB
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS last_updated (
        date TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS lastfm_state (
        id INTEGER PRIMARY KEY,
        last_successful_page INTEGER
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS playlist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        track_name TEXT,
        artist_name TEXT,
        album_name TEXT,
        timestamp INTEGER,
        mbid TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS saved_albums (
        album_id TEXT PRIMARY KEY,
        album_name TEXT,
        artist_name TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tracks (
        track_id INTEGER PRIMARY KEY,
        track_name TEXT,
        artist_name TEXT,
        album_name TEXT,
        timestamp INTEGER,
        scrobble_count INTEGER,
        mbid TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS updates (
        source TEXT PRIMARY KEY,
        last_update INTEGER
    )
    ''')

    conn.commit()

    print("Database setup complete.")

    return conn

def populate_tracks_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO tracks (track_name, artist_name, album_name, scrobble_count, timestamp, mbid)
        SELECT track_name, artist_name, album_name, COUNT(*) as scrobble_count, MAX(timestamp) as timestamp, mbid
        FROM playlist
        GROUP BY track_name, artist_name, album_name
    ''')
    conn.commit()

def clean_album_names(conn):
    cursor = conn.cursor()

    # Select all rows from the playlist
    cursor.execute("SELECT id, album_name FROM playlist")
    rows = cursor.fetchall()

    # Define pattern to remove from album names
    patterns = ['\(.*deluxe.*\)', '\(.*edition.*\)', '\(.*version.*\)', '\(.*remaster.*\)', '\(.*issue.*\)']

    # For each row, clean the album_name field and update the database
    for row in rows:
        new_album_name = row[1]
        for pattern in patterns:
            new_album_name = re.sub(pattern, '', new_album_name, flags=re.IGNORECASE)

        # Update the row in the database with the cleaned album_name
        cursor.execute("UPDATE playlist SET album_name = ? WHERE id = ?", (new_album_name.strip(), row[0]))

    # Commit changes to the database
    conn.commit()

# Insert unique artist and album pairs from tracks into albums
def populate_albums_table(conn):
    cursor = conn.cursor()
    
    # Insert artists into the artists table, ignoring duplicates
    cursor.execute('''
        INSERT OR IGNORE INTO artists (artist_name)
        SELECT DISTINCT artist_name
        FROM tracks
    ''')

    # Insert albums into the albums table, joining on the artists table to get the artist_id
    cursor.execute('''
        INSERT OR IGNORE INTO albums (artist_id, artist_name, album_name, mbid, last_played)
        SELECT artists.artist_id, tracks.artist_name, tracks.album_name, tracks.mbid, MAX(tracks.timestamp)
        FROM tracks
        INNER JOIN artists ON tracks.artist_name = artists.artist_name
        GROUP BY tracks.artist_name, tracks.album_name
    ''')

    conn.commit()

def update_albums_table_scrobbles(conn):
    cursor = conn.cursor()

    # Update num_tracks and scrobble_count in albums table
    cursor.execute('''
        UPDATE albums
        SET 
            num_tracks = (
                SELECT COUNT(*)
                FROM tracks
                WHERE albums.artist_name = tracks.artist_name AND albums.album_name = tracks.album_name
            ),
            scrobble_count = (
                SELECT CAST(SUM(scrobble_count) AS INTEGER) / COUNT(*)
                FROM tracks
                WHERE albums.artist_name = tracks.artist_name AND albums.album_name = tracks.album_name
            )
    ''')

    conn.commit()

def clean_saved_album_names(conn):
    # Create a new cursor object
    cursor = conn.cursor()

    # Fetch all the album names from the albums table
    cursor.execute("SELECT album_id, album_name FROM saved_albums")
    albums = cursor.fetchall()

    # Iterate through the albums and clean their names using remove_special_editions
    for album in albums:
        album_id = album[0]
        original_album_name = album[1]
        cleaned_album_name = remove_special_editions(original_album_name)

        # Update the album name in the albums table
        cursor.execute("UPDATE saved_albums SET album_name=? WHERE album_id=?", (cleaned_album_name, album_id))

    # Commit the changes to the database
    conn.commit()

    print("Album names cleaned.")

def update_last_played_in_artists(conn):
    cursor = conn.cursor()

    # Update query
    update_query = """
    UPDATE artists
    SET last_played = (
        SELECT MAX(last_played)
        FROM albums
        WHERE artists.artist_name = albums.artist_name
    )
    """

    cursor.execute(update_query)
    conn.commit()

def delete_incomplete_albums(conn):
    cursor = conn.cursor()

    # Delete query
    delete_query = """
    DELETE FROM albums
    WHERE album_name IS NULL OR album_name = '' OR artist_name IS NULL OR artist_name = ''
    """

    cursor.execute(delete_query)
    conn.commit()

    print(f"{cursor.rowcount} records deleted from the albums table.")

def remove_duplicates_albums(conn):
    cursor = conn.cursor()

    # Fetch all albums
    cursor.execute("SELECT album_id, artist_name, album_name, last_played, num_tracks, scrobble_count, saved FROM albums")
    albums = cursor.fetchall()

    # Get a list of unique album names (after cleaning)
    unique_albums = get_unique_albums(albums)

    # For each unique album, find all fuzzy matches from the original albums
    for unique_album in unique_albums:
        matched_albums = find_fuzzy_matches(unique_album, albums)

        # Determine which album to keep and update
        album_to_keep = determine_album_to_keep(unique_album, matched_albums)

        if album_to_keep is not None:
            # Update the album_to_keep in the database
            update_album_in_database(conn, album_to_keep)

            # Delete the other duplicate albums from the database
            delete_duplicate_albums(conn, matched_albums, album_to_keep)

    conn.commit()

def get_unique_albums(albums):
    unique_albums = []
    seen_albums = set()

    for album in albums:
        album_info = (album[1], album[2])  # assuming that artist_name is at index 1 and album_name at index 2

        if album_info not in seen_albums:
            seen_albums.add(album_info)
            unique_albums.append(album)

    return unique_albums

def find_fuzzy_matches(target_album, albums):
    matched_albums = []
    target_artist, target_album_name = target_album[1].lower(), target_album[2].lower()  # assuming that artist_name is at index 1 and album_name at index 2

    for album in albums:
        if album[1].lower() == target_artist:  # assuming that artist_name is at index 1
            ratio = fuzz.ratio(target_album_name, album[2].lower())  # assuming that album_name is at index 2
            if ratio > 95:  # or some other threshold
                matched_albums.append(album)

    return matched_albums

def determine_album_to_keep(unique_album, matched_albums):
    # Extract album names and whether they're saved
    album_names = [(album[1], album[6] == 'true' if album[6] is not None else False, album[0]) for album in matched_albums]
    
    # Sort albums so that saved albums are first and albums with smaller IDs are first within those groups
    album_names.sort(key=lambda x: (-x[1], x[2]))
    
    # Use fuzzy matching to find the best match to unique_album among the sorted list of album names
    best_match = process.extractOne(unique_album[2].lower(), [album[0].lower() for album in album_names])
    
    # Return the dictionary for the best match
    for album in matched_albums:
        if album[1].lower() == best_match[0]:
            return album

def delete_duplicate_albums(conn, matched_albums, album_to_keep):
    cursor = conn.cursor()
    album_ids_to_delete = [album[0] for album in matched_albums if album[0] != album_to_keep[0]]  # Assuming album_id is at index 0
    for album_id in album_ids_to_delete:
        cursor.execute("DELETE FROM albums WHERE album_id = ?", (album_id,))

def update_album_in_database(conn, album):
    cursor = conn.cursor()
    cursor.execute('''UPDATE albums
                      SET last_played = ?, num_tracks = ?, scrobble_count = ?
                      WHERE album_id = ?''', (album[3], album[4], album[5], album[0]))  # Assuming last_played is at index 3, num_tracks at index 4, scrobble_count at index 5 and album_id at index 0

def create_setup_functions_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS setup_functions (
        function_name TEXT PRIMARY KEY,
        executed INTEGER
    );
    """)
    conn.commit()

def first_time_functions(conn):

    create_setup_functions_table(conn)

    def function_executed(conn, function_name):
        cursor = conn.cursor()
        cursor.execute("SELECT executed FROM setup_functions WHERE function_name=?", (function_name,))
        result = cursor.fetchone()
        return result and result[0]

    def set_function_executed(conn, function_name):
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO setup_functions (function_name, executed) VALUES (?, 1)", (function_name,))
        conn.commit()

    def execute_if_not_done(func_name, func, conn, *args, **kwargs):
        if not function_executed(conn, func_name):
            func(conn, *args, **kwargs)
            set_function_executed(conn, func_name)
            print(f"----- {func_name} completed")

    execute_if_not_done( "setup database", setup_database, conn, db_name)
    execute_if_not_done( "fetch scrobbles", fetch_and_save_scrobbles, conn, LASTFM_API_KEY, LASTFM_USER)
    execute_if_not_done( "add latest scrobble timestamp", add_latest_timestamp_to_updates, conn)
    execute_if_not_done( "clean album names in playlist" , clean_album_names, conn)
    execute_if_not_done( "populate tracks table" , populate_tracks_table, conn)
    execute_if_not_done( "populate albums table", populate_albums_table, conn)
    execute_if_not_done( "update scrobble count", update_albums_table_scrobbles, conn)
    token = get_spotify_access_token(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI, SPOTIFY_SCOPE)
    execute_if_not_done( "save saved albums", save_spotify_saved_albums_to_db, conn, token)
    execute_if_not_done( "clean saved albums", clean_saved_album_names, conn)
    execute_if_not_done( "updated saved spotify albums", update_saved_spotify_albums, conn)
    execute_if_not_done( "delete incomplete albums", delete_incomplete_albums, conn)
    execute_if_not_done( "delete unwanted albums", delete_unwanted_albums_and_artists, conn)
    execute_if_not_done("update_last_played", update_last_played_in_artists, conn)
    execute_if_not_done( "remove duplicate albums", remove_duplicates_albums, conn)

# ----- Misc functions ------
# Compares two strings after removing any text within parentheses or brackets.
def ignore_parentheses_and_brackets(string1, string2):
    pattern = r' ?[\(\[].*?[\)\]]'
    cleaned_string1 = re.sub(pattern, '', string1)
    cleaned_string2 = re.sub(pattern, '', string2)
    return cleaned_string1 == cleaned_string2

# Saves unique genres to a file
def save_genres_to_file(conn, filename="genres.txt"):
    cursor = conn.cursor()

    cursor.execute("SELECT genres FROM albums")
    genres_data = cursor.fetchall()

    unique_genres = set()

    for genres in genres_data:
        if genres[0]:
            genre_list = [genre.strip() for genre in genres[0].split(',')]
            unique_genres.update(genre_list)

    with open(filename, "w") as file:
        file.write("\n".join(sorted(unique_genres)))

    print(f"Saved genres to {filename}")

# Removes specified genres from the albums in the database
def remove_genres_from_albums(conn, filename="genres.txt"):
    cursor = conn.cursor()

    # Read genres to remove from the file
    with open(filename, "r") as file:
        genres_to_remove = {line.strip() for line in file.readlines()}

    # Get albums with their genres
    cursor.execute("SELECT album_id, genres FROM albums")
    albums = cursor.fetchall()

    for album_id, genres in albums:
        if not genres:
            continue

        # Split genres into a list, remove the unwanted genres and join them back
        genre_list = [genre.strip() for genre in genres.split(',')]
        updated_genre_list = [genre for genre in genre_list if genre not in genres_to_remove]
        updated_genres = ', '.join(updated_genre_list)

        # Update the album's genres
        if updated_genres != genres:
            cursor.execute("UPDATE albums SET genres = ? WHERE album_id = ?", (updated_genres, album_id))

    conn.commit()
    print("Removed specified genres from albums.")

# Updates an existing genre with a new genre in the albums table
def update_genre(conn, current_genre, new_genre):
    # Create a new cursor object
    cursor = conn.cursor()

    # Update genre in the albums table
    query = "UPDATE albums SET genres = REPLACE(genres, ?, ?) WHERE genres LIKE ?"
    cursor.execute(query, (current_genre, new_genre, f"%{current_genre}%"))

    # Commit the changes and close the cursor
    conn.commit()
    cursor.close()

# Loads a list of skipped albums from a file
def load_skipped_albums_list(file_name):
    if not os.path.isfile(file_name):
        return set()

    skipped_albums = set()
    try:
        with open(file_name, "r") as f:
            for line in f:
                parts = line.strip().split(" - ")
                if len(parts) == 2:
                    skipped_albums.add(tuple(parts))
                else:
                    print(f"Invalid entry in skipped_albums file: {line.strip()}")
    except FileNotFoundError:
        pass

    return skipped_albums

# Saves a list of skipped albums to a file
def save_skipped_albums_list(skipped_albums_file, skipped_albums):
    with open(skipped_albums_file, "w") as f:
        for artist_name, album_name in skipped_albums:
            f.write(f"{artist_name} - {album_name}\n")

# Removes special edition markers from an album name
def remove_special_editions(album_name):
    if album_name is None:
        return ""

    pattern = r'(\[.*\]|\(.*\)|\{.*\})'
    modified_album_name = re.sub(pattern, '', album_name, flags=re.IGNORECASE)

    return modified_album_name.strip()









