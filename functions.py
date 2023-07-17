import requests, spotipy, base64, webbrowser, json, sqlite3, schedule, time, pylast, re, random, argparse
import musicbrainzngs, discogs_client, requests.exceptions, urllib.parse, unicodedata, datetime, os, csv
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
from urllib.parse import urlencode, urlparse, parse_qs
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
from requests.exceptions import ConnectTimeout
from pylast import PyLastError
from rymscraper import rymscraper, RymUrl
from rapidfuzz import fuzz, process 
import selenium.common.exceptions
from lastfm_functions import *
from musicbrainz_functions import *
from spotify_functions import *
from rym_functions import *
from db_functions import *

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

if config['spotify']['use'] == 'yes':
    use_spotify = True
elif config['spotify']['use'] == 'no':
    use_spotify = False

# if SPOTIFY_CLIENT_ID == "[your spotify client ID]" and SPOTIFY_CLIENT_SECRET == "[your spotify client secret]":
#     use_spotify = False

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


# Main function that connects to the database, updates data, and closes the connection
def main(update, import_csv, reset, db_name=db_name):
    conn = sqlite3.connect(db_name)
    conn.create_function("IGNORE_PARENTHESIS_AND_BRACKETS", 2, ignore_parentheses_and_brackets)

    if reset:
        reset_executed_functions(conn)

    if update:
        print('Updating...')
        if not check_if_table_exists(conn, "albums"):
            if SPOTIFY_CLIENT_ID == "[your spotify client ID]" and LASTFM_API_KEY == "[your last.fm API key]":
                print("Please set your Spotify and Last.fm API keys first. You can choose to only set a Last.fm API key if you wish.")
                return
            else:
                print("First time running in this directory. Setting up the database...")
                first_time_functions(conn)
            if import_csv:
                update_database_from_csv(conn)
            print("Setup complete. Updating now.")
        else:
            conn = sqlite3.connect(db_name)
            print("Database exists. Checking if it's populated...")
            if (check_if_table_exists(conn, "albums") and 
                check_if_table_exists(conn, "artists")):
                if (check_if_populated(conn, "albums") and 
                    check_if_populated(conn, "artists")):
                    print("Database is populated. Running other functions...")
                    if import_csv:
                        update_database_from_csv(conn)
                    update_databases(conn, LASTFM_USER, LASTFM_API_KEY)
                    set_last_executed_date(conn, datetime.datetime.now())
                else:
                    print("Database is not fully populated. Setting up the database...")
                    first_time_functions(conn)
                    print("Setup complete. Updating now.")
                    if import_csv:
                        update_database_from_csv(conn)
                    update_databases(conn, LASTFM_USER, LASTFM_API_KEY)
            else:
                print("First time running in this directory. Setting up the database...")
                first_time_functions(conn)
                print("Setup complete. Updating now.")
                if import_csv:
                    update_database_from_csv(conn)
                update_databases(conn, LASTFM_USER, LASTFM_API_KEY)
    else:
        if not check_if_table_exists(conn, "albums"):
            if SPOTIFY_CLIENT_ID == "[your spotify client ID]" and LASTFM_API_KEY == "[your last.fm API key]":
                print("Please set your Spotify and Last.fm API keys first. You can choose to only set a Last.fm API key if you wish.")
                return
            else:
                print("First time running in this directory. Setting up the database...")
                first_time_functions(conn)
            if import_csv:
                update_database_from_csv(conn)
            print("Setup complete. Run again to perform updates.")
        else:
            conn = sqlite3.connect(db_name)
            print("Database exists. Checking if it's populated...")
            if (check_if_table_exists(conn, "albums") and 
                check_if_table_exists(conn, "artists")):
                if (check_if_populated(conn, "albums") and 
                    check_if_populated(conn, "artists")):
                    print("Database is populated. Running other functions...")
                    if should_execute_function(conn):
                        if import_csv:
                            update_database_from_csv(conn)
                        update_databases(conn, LASTFM_USER, LASTFM_API_KEY)
                        set_last_executed_date(conn, datetime.datetime.now())
                else:
                    print("Database is not fully populated. Setting up the database...")
                    first_time_functions(conn)
                    if import_csv:
                        update_database_from_csv(conn)
                    print("Setup complete. Run again to perform updates.")
            else:
                print("First time running in this directory. Setting up the database...")
                first_time_functions(conn)
                if import_csv:
                    update_database_from_csv(conn)
                print("Setup complete. Run again to perform updates.")
    conn.close()

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
        # Return True and set a last_executed_date when last_executed_date is None
        set_last_executed_date(conn, datetime.datetime.now())
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
    cursor = conn.cursor()
    if use_spotify:
        function_names = ["fetch_timestamp_lastfm", "fetch_timestamp_spotify", "spotify_access_token", "save_recent_saved_albums", "parse_and_insert_saved_albums",
                      "set_last_update_timestamp_spotify", "create_new_tracks_table", "create_new_playlist_table", "create_new_albums_table",
                      "insert_scrobbles_into_new_playlist", "populate_new_tracks_table", "populate_new_albums_table", "clean_new_album_names",
                      "update_album_track_counts", "update_album_scrobble_counts", "set_last_update_timestamp_lastfm", "append_and_update_albums",
                      "update saved spotify albums", "update_albums_with_missing_ids", "update_last_played", "update new artist scrobbles",
                      "delete incomplete albums", "remove duplicate albums", "retrieve saved album and artist info", "update_artist_and_album_urls", 
                      "delete_unwanted_albums_and_artists", "update data from spotify", "update_albums_with_lastfm_release_years", "update_album_mbid",
                      "update_release_info", "update spotify album length", "delete unwanted albums again", "update_rym_genres", 
                      "update artist genres","update_albums_with_cover_arts", "update_artists_with_images", "drop_tables"]
    elif not use_spotify:
        function_names = ["fetch_timestamp_lastfm", "create_new_tracks_table", "create_new_playlist_table", "create_new_albums_table",
                      "insert_scrobbles_into_new_playlist", "populate_new_tracks_table", "populate_new_albums_table", "clean_new_album_names",
                      "update_album_track_counts", "update_album_scrobble_counts", "set_last_update_timestamp_lastfm", "append_and_update_albums",
                      "update saved spotify albums", "update_albums_with_missing_ids", "update_last_played", "update new artist scrobbles",
                      "delete incomplete albums", "remove duplicate albums", "retrieve saved album and artist info", "update_artist_and_album_urls", 
                      "delete_unwanted_albums_and_artists", "update data from spotify", "update_albums_with_lastfm_release_years", "update_album_mbid",
                      "update_release_info", "update spotify album length", "delete unwanted albums again", "update_rym_genres", 
                      "update artist genres","update_albums_with_cover_arts", "update_artists_with_images", "drop_tables"]

    for func_name in function_names:
        cursor.execute("INSERT OR IGNORE INTO executed_functions (function_name, executed) VALUES (?, 0)", (func_name,))
    conn.commit()

    lastfm_retries = 3

    def is_spotify_token_valid(access_token):
        headers = {
        'Authorization': f'Bearer {access_token}'
        }
        response = requests.get('https://api.spotify.com/v1/me', headers=headers)

        if response.status_code == 200:
            return True
        else:
            return False

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
        if func_name == 'spotify_access_token':
            token = get_intermediate_result(conn, "spotify_access_token")
            if is_spotify_token_valid(token) == False:
                func(conn, *args, **kwargs)
                set_function_executed(conn, func_name)
                print(f"----- {func_name} completed")
        else:
            if not function_executed(conn, func_name):
                func(conn, *args, **kwargs)
                set_function_executed(conn, func_name)
                print(f"----- {func_name} completed")

    reset_executed_functions_if_all_done(conn)
    print('----- starting update')
    execute_if_not_done('fetch_timestamp_lastfm', get_last_update_timestamp, conn, "lastfm")
    lastfm_last_update = get_intermediate_result(conn, "lastfm_last_update")

    if use_spotify:
        execute_if_not_done('fetch_timestamp_spotify', get_last_update_timestamp, conn, "spotify")
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
    if not function_executed(conn, "insert_scrobbles_into_new_playlist"):
        for i in range(lastfm_retries):
            result = fetch_lastfm_scrobbles(conn, LASTFM_API_KEY, LASTFM_SECRET, LASTFM_USER, from_timestamp=lastfm_last_update)
        
            if result is not None:
                new_scrob, lastfm_ts = result
                execute_if_not_done("insert_scrobbles_into_new_playlist",insert_scrobbles_into_new_playlist, conn, new_scrob)
                break
            else:
                print(f"Attempt {i+1} at fetching last.fm scrobbles failed. Retrying in a couple of seconds...")
                time.sleep(2)
        else:
            raise Exception("Maximum retry attempts exceeded. The last.fm API may be having issues, please try again later.")
    else:
        lastfm_ts = lastfm_last_update

    execute_if_not_done( "populate_new_tracks_table" , populate_new_tracks_table, conn)
    execute_if_not_done( "populate_new_albums_table" , populate_new_albums_table, conn)
    execute_if_not_done( "clean_new_album_names", clean_new_album_names, conn)
    execute_if_not_done( "update_album_track_counts" , update_album_track_counts, conn)
    execute_if_not_done( "update_album_scrobble_counts", update_album_scrobble_counts, conn)
    
    execute_if_not_done( "set_last_update_timestamp_lastfm" ,set_last_update_timestamp, conn, "lastfm", lastfm_ts)
    execute_if_not_done( "append_and_update_albums" , append_and_update_albums, conn)
    if use_spotify:
        execute_if_not_done( "update saved spotify albums", update_saved_spotify_albums, conn)
    execute_if_not_done( "update_albums_with_missing_ids" ,update_albums_with_missing_ids, conn)
    execute_if_not_done( "update_last_played",update_last_played, conn)
    execute_if_not_done( "update new artist scrobbles", update_artist_new_scrobbles, conn)
    execute_if_not_done( "delete incomplete albums", delete_incomplete_albums, conn)
    execute_if_not_done( "remove duplicate albums", remove_duplicates_albums, conn)
    execute_if_not_done( "retrieve saved album and artist info", update_database_from_github_csv, conn)
    if use_spotify:
        execute_if_not_done( "update_artist_and_album_urls" ,update_artist_and_album_urls, conn, stoken)

    execute_if_not_done( "delete_unwanted_albums_and_artists", delete_unwanted_albums_and_artists, conn)
    if use_spotify:
        execute_if_not_done( "update data from spotify", update_spotify_data, conn, stoken)
    execute_if_not_done( "update_albums_with_lastfm_release_years" ,update_albums_with_lastfm_release_years, conn, LASTFM_API_KEY)
    execute_if_not_done( "update_album_mbid" , update_album_mbid, conn)
    execute_if_not_done( "update_release_info" , update_release_info, conn)
    if use_spotify:
        execute_if_not_done( "update spotify album length", update_album_durations, conn)
    #execute_if_not_done( "delete unwanted albums again", delete_unwanted_albums_again, conn)
    execute_if_not_done( "update_rym_genres" , update_rym_genres, conn, use_scraperapi=USE_SCRAPER)
    execute_if_not_done( "update artist genres" , update_artist_genres, conn)
    execute_if_not_done( "update_albums_with_cover_arts" ,update_albums_with_cover_arts, conn, LASTFM_API_KEY)
    execute_if_not_done( "update_artists_with_images",update_artists_with_images, conn)

    print('----- cleaning up:')
    table_names_to_drop = ['new_playlist', 'new_tracks', 'new_albums']
    execute_if_not_done("drop_tables", drop_tables, conn, table_names_to_drop)

    reset_executed_functions(conn)
    print('----- update complete')

def reset_executed_functions_if_all_done(conn):
    cursor = conn.cursor()
    
    # Check if all executed tags are set to 1
    cursor.execute("""
        SELECT COUNT(*)
        FROM executed_functions
        WHERE executed = 0
    """)
    not_executed_count = cursor.fetchone()[0]
    
    # If there are no rows where executed is 0 (i.e., all are 1)
    if not_executed_count == 0:
        cursor.execute("""
            UPDATE executed_functions
            SET executed = 0
        """)
        conn.commit()
        print("Successfully reset all executed tags.")
    else:
        print("Not all functions have been executed yet.")

# Resets the executed status of all functions in executed_functions table
def reset_executed_functions(conn):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE executed_functions
        SET executed = 0
    """)
    conn.commit()

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
        last_played INTEGER,
        scrobble_count INTEGER
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
        spotify_id TEXT,
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS intermediate_results (
        key TEXT PRIMARY KEY,
        value BLOB
    )
    """)

    conn.commit()
    print("Database setup complete.")
    return conn

def first_time_functions(conn):
    create_setup_functions_table(conn)
    cursor = conn.cursor()

    if not use_spotify:
        function_names = ["setup database", "fetch scrobbles", "add latest scrobble timestamp", "clean album names in playlist",
                      "populate tracks table", "populate albums table", "update scrobble count", "update artist scrobs", 
                      "delete incomplete albums", "delete unwanted albums", "update_last_played", "remove duplicate albums", "insert executed date"]
    else:
        function_names = ["setup database", "fetch scrobbles", "add latest scrobble timestamp", "clean album names in playlist",
                      "populate tracks table", "populate albums table", "update scrobble count", "update artist scrobs", "save saved albums",
                      "add spotify update timestamp", "clean saved albums", "updated saved spotify albums", 
                      "delete incomplete albums", "delete unwanted albums", "update_last_played", "remove duplicate albums", "insert executed date"]
    for func_name in function_names:
        cursor.execute("INSERT OR IGNORE INTO setup_functions (function_name, executed) VALUES (?, 0)", (func_name,))
    conn.commit()

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
    execute_if_not_done( "update artist scrobs", update_artist_scrobbles, conn)
    if use_spotify:
        token = get_spotify_access_token(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI, SPOTIFY_SCOPE)
        execute_if_not_done( "save saved albums", save_spotify_saved_albums_to_db, conn, token)
        execute_if_not_done( "add spotify update timestamp", add_current_timestamp_to_updates, conn)
        execute_if_not_done( "clean saved albums", clean_saved_album_names, conn)
        execute_if_not_done( "updated saved spotify albums", update_saved_spotify_albums, conn)
    execute_if_not_done( "delete incomplete albums", delete_incomplete_albums, conn)
    execute_if_not_done( "delete unwanted albums", delete_unwanted_albums_and_artists, conn)
    execute_if_not_done("update_last_played", update_last_played_in_artists, conn)
    execute_if_not_done( "remove duplicate albums", remove_duplicates_albums, conn)
    execute_if_not_done( "insert executed date", insert_initial_last_executed_date, conn)





