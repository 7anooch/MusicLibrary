import requests, spotipy, base64, webbrowser, json, sqlite3, schedule, time, pylast, re, random, argparse
import musicbrainzngs, discogs_client, requests.exceptions, urllib.parse, unicodedata, datetime, os
import pandas as pd
import matplotlib.pyplot as plt
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
def main(update, db_name=db_name):
    conn = sqlite3.connect(db_name)
    conn.create_function("IGNORE_PARENTHESIS_AND_BRACKETS", 2, ignore_parentheses_and_brackets)

    if update:
        print('Updating...')
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
                    update_databases(conn, LASTFM_USER, LASTFM_API_KEY)
                    set_last_executed_date(conn, datetime.datetime.now())
                else:
                    print("Database is not fully populated. Setting up the database...")
                    first_time_functions(conn)
                    print("Setup complete. Updating now.")
                    update_databases(conn, LASTFM_USER, LASTFM_API_KEY)
            else:
                print("First time running in this directory. Setting up the database...")
                first_time_functions(conn)
                print("Setup complete. Updating now.")
                update_databases(conn, LASTFM_USER, LASTFM_API_KEY)
    else:
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
    cursor.execute("DELETE FROM albums WHERE num_tracks = 1 AND scrobble_count = 2 AND saved IS NULL AND release_length < 15 AND tracks_mb < 3")
    deleted_album_rows += cursor.rowcount
    cursor.execute("DELETE FROM albums WHERE num_tracks = 2 AND scrobble_count = 2 AND saved IS NULL AND release_length < 15 AND tracks_mb < 3")
    deleted_album_rows += cursor.rowcount
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
    cursor = conn.cursor()
    function_names = ["fetch_timestamp_lastfm", "fetch_timestamp_spotify", "spotify_access_token", "save_recent_saved_albums", 
                      "parse_and_insert_saved_albums", "set_last_update_timestamp_spotify", "create_new_tracks_table",
                      "create_new_playlist_table", "create_new_albums_table", "insert_scrobbles_into_new_playlist", 
                      "populate_new_tracks_table", "populate_new_albums_table", "update_album_track_counts", 
                      "update_album_scrobble_counts", "set_last_update_timestamp_lastfm", "append_and_update_albums", 
                      "update_albums_with_missing_ids", "update_last_played", "update_artist_and_album_urls", 
                      "delete_unwanted_albums_and_artists", "update data from spotify", "update_albums_with_lastfm_release_years", 
                      "update_album_mbid", "update_release_info", "update spotify album length", "update_rym_genres", 
                      "update_albums_with_cover_arts", "update_artists_with_images", "drop_tables"]
    for func_name in function_names:
        cursor.execute("INSERT OR IGNORE INTO executed_functions (function_name, executed) VALUES (?, 0)", (func_name,))
    conn.commit()
    
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
    execute_if_not_done("update spotify album length", update_album_durations, conn)
    execute_if_not_done( "update_rym_genres" , update_rym_genres, conn, use_scraperapi=USE_SCRAPER)
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
    """adds a lastfm last_pdate timestamp based on the last song in playlist"""
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

def add_current_timestamp_to_updates(conn):
    """adds a spotify last_update timestamp based on the current time"""
    cursor = conn.cursor()
    
    # Get the current timestamp as a Unix timestamp
    current_timestamp = int(time.time())
    
    # Insert current timestamp into the 'updates' table
    insert_query = """
    INSERT INTO updates (source, last_update)
    VALUES (?, ?)
    """
    cursor.execute(insert_query, ('spotify', current_timestamp))
    conn.commit()
    print(f"Successfully saved the current Unix timestamp ({current_timestamp}) to the updates table.")


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
        spotify_id, TEXT,
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
    cursor = conn.cursor()

    function_names = ["setup database", "fetch scrobbles", "add latest scrobble timestamp", "clean album names in playlist",
                      "populate tracks table", "populate albums table", "update scrobble count", "save saved albums",
                      "add spotify update timestamp", "clean saved albums", "updated saved spotify albums", 
                      "delete incomplete albums", "delete unwanted albums", "update_last_played", "remove duplicate albums"]
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
    token = get_spotify_access_token(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI, SPOTIFY_SCOPE)
    execute_if_not_done( "save saved albums", save_spotify_saved_albums_to_db, conn, token)
    execute_if_not_done( "add spotify update timestamp", add_current_timestamp_to_updates, conn)
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
                parts = line.strip().split(" - ", 1)  # only split on the first dash
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

    words = ['edition', 'anniversary', 'bonus', 'reissue', 'issue', 'deluxe', 'remaster', 'remastered', 'version']
    pattern = r'(\[.*(' + '|'.join(words) + ').*\]|\(.*(' + '|'.join(words) + ').*\)|\{.*(' + '|'.join(words) + ').*\})'
    modified_album_name = re.sub(pattern, '', album_name, flags=re.IGNORECASE)

    return modified_album_name.strip()

def get_with_retry(url, headers, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()  # raises an HTTPError if the response status isn't '200 OK'
            return response
        except (requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
            print(f"Error occurred: {e}, retrying...")
            retries += 1
    return None











