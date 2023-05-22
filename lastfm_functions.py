import requests, spotipy, base64, webbrowser, json, sqlite3, schedule, time, pylast, re, random
import musicbrainzngs, discogs_client, requests.exceptions, urllib.parse, unicodedata, datetime, os
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.parse import quote_plus
from requests.exceptions import ConnectTimeout
from pylast import PyLastError
from rapidfuzz import fuzz, process
from functions import *

with open('keys.json', 'r') as f:
    config = json.load(f)

LASTFM_API_KEY = config['lastfm']['api_key']
LASTFM_SECRET = config['lastfm']['secret']
LASTFM_USER = config['lastfm']['user']
db_name = config['database']['name']

def update_albums_with_cover_arts(conn, api_key):
    """ Update album cover art using the Last.fm API """
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

def get_lastfm_cover_art_url(api_key, artist_name, album_name):
    """ Fetches the album cover art URL from the Last.fm API for a given artist and album."""
    network = pylast.LastFMNetwork(api_key=api_key)
    album = network.get_album(artist_name, album_name)
    try:
        cover_art_url = album.get_cover_image()
        if cover_art_url:
            return cover_art_url
    except Exception as e:
        print(f"Error fetching cover art for {album_name}: {e}")

    return None

def update_artists_with_images(conn):
    """ Updates the artist_image column for all artists in the database. """
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

def get_lastfm_artist_image(artist_name):
    """ Fetches the artist image URL from the Last.fm API for a given artist. """
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

# check?
def get_recent_lastfm_tracks(lastfm_username, lastfm_api_key, from_timestamp):
    """ Fetches recently played tracks from Last.fm"""
    params = {
        'method': 'user.getRecentTracks',
        'user': lastfm_username,
        'api_key': lastfm_api_key,
        'format': 'json',
        'from': from_timestamp
    }
    response = requests.get('http://ws.audioscrobbler.com/2.0/', params=params)
    return response.json()

# check?
def update_scrobbles(conn, recent_tracks):
    """ Updates the scrobbles table with recent tracks from Last.fm """
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

def fetch_lastfm_scrobbles(conn, api_key, api_secret, username, from_timestamp=None, limit=None):
    """ Fetches scrobbles from Last.fm and returns them as a list of dictionaries"""
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

def get_lastfm_release_year(artist_name, album_name, api_key):
    """ Retrieves release year for an album from Last.fm"""
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

def update_albums_with_lastfm_release_years(conn, lastfm_api_key):
    """ Updates albums with release years fetched from Last.fm"""
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

# check
def fetch_scrobbles_and_save_to_db(conn):
    """ Fetches scrobbles from Last.fm API and saves them to the database """
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

# check
def get_last_successful_page(conn):
    """ Retrieves the last successful page fetched from the Last.fm API"""
    cursor = conn.cursor()
    cursor.execute("SELECT last_successful_page FROM lastfm_state WHERE id = 1")
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        return 1

# check
def update_last_successful_page(conn, page):
    """ Updates the last successful page fetched from the Last.fm API"""
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO lastfm_state (id, last_successful_page) VALUES (1, ?)", (page,))
    conn.commit()

# check
def fetch_missing_scrobbles(api_key, username, conn):
    """ Fetches missing scrobbles from the Last.fm API and adds them to the database """
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

def fetch_lastfm_data(api_key, user, limit=200, page=1, retries=3, delay=5):
    """ Fetches recent tracks for a Last.fm user, with retry logic and a delay between attempts. """
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



