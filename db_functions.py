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
from functions import *

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

# # Marks albums in the database as saved if they exist in the saved_albums table.
# def update_saved_spotify_albums(conn):
#     conn.create_function("IGNORE_PARENTHESIS_AND_BRACKETS", 2, ignore_parentheses_and_brackets)
#     cursor = conn.cursor()
#     cursor.execute("""UPDATE albums
#                       SET saved = 'saved'
#                       WHERE EXISTS (SELECT 1 FROM saved_albums
#                                     WHERE IGNORE_PARENTHESIS_AND_BRACKETS(albums.album_name, saved_albums.album_name)
#                                     AND albums.artist_name = saved_albums.artist_name)
#                    """)
#     conn.commit()

def update_saved_spotify_albums(conn):
    cursor = conn.cursor()

    ALBUM_ID = 0
    ALBUM_NAME = 1
    ARTIST_NAME = 2

    # Query all saved albums
    cursor.execute("SELECT * FROM saved_albums")
    saved_albums = cursor.fetchall()

    # Query all albums
    cursor.execute("SELECT * FROM albums")
    existing_albums = cursor.fetchall()

    for saved_album in saved_albums:
        saved_album_name = saved_album[ALBUM_NAME]
        saved_artist_name = saved_album[ARTIST_NAME]

        # Check if the album already exists in the albums table
        album = next((album for album in existing_albums 
                      if ignore_parentheses_and_brackets(album[ALBUM_NAME], saved_album_name) 
                      and album[ARTIST_NAME+1].lower() == saved_artist_name.lower()), None)

        if album is None:
            # Album does not exist in the albums table, so insert it
            cursor.execute("""
                INSERT INTO albums (album_name, artist_name, saved)
                VALUES (?, ?, 'saved')
            """, (saved_album_name, saved_artist_name))
        else:
            # Album exists in the albums table, so update it
            cursor.execute("""
                UPDATE albums 
                SET saved = 'saved'
                WHERE album_id = ?
            """, (album[ALBUM_ID],))

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
            if existing_scrobble_count != None:
                updated_scrobble_count = existing_scrobble_count + scrobble_count
            elif existing_scrobble_count == None:
                updated_scrobble_count = scrobble_count
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

    min_tracks_played = config['criteria_to_keep']['min_tracks_played']
    min_scrobbled = config['criteria_to_keep']['min_scrobbled']
    min_length = config['criteria_to_keep']['min_length']
    min_tracks = config['criteria_to_keep']['min_tracks']

    # Delete albums with num_tracks = 1 and scrobble_count = 1, unless saved is not NULL
    cursor.execute("DELETE FROM albums WHERE num_tracks = 1 AND scrobble_count = 1 AND saved IS NULL AND (release_length < 30 OR release_length IS NULL)")
    deleted_album_rows = cursor.rowcount
    cursor.execute("""
        DELETE FROM albums WHERE num_tracks < ? AND scrobble_count < ? AND saved IS NULL AND release_length < ? AND tracks_mb < ?
    """, (min_tracks_played, min_scrobbled, min_length, min_tracks))
    deleted_album_rows += cursor.rowcount

    if config['criteria_to_keep']['singles'] == "yes":
        cursor.execute("DELETE FROM albums WHERE release_type IS ('Single' OR 'single') AND saved IS NULL")
        deleted_album_rows += cursor.rowcount
    if config['criteria_to_keep']['eps'] == "yes":
        cursor.execute("DELETE FROM albums WHERE release_type IS ('EP' OR 'ep') AND saved IS NULL")
        deleted_album_rows += cursor.rowcount
    if config['criteria_to_keep']['compilations'] == "yes":
        cursor.execute("DELETE FROM albums WHERE release_type IS ('Compilation' OR 'compilation') AND saved IS NULL")
        deleted_album_rows += cursor.rowcount
    print(f"Deleted {deleted_album_rows} unwanted albums")

    # Delete artists not found in the albums table
    cursor.execute('''DELETE FROM artists WHERE NOT EXISTS 
                      (SELECT 1 FROM albums WHERE albums.artist_id = artists.artist_id)''')
    
    deleted_artist_rows = cursor.rowcount
    print(f"Deleted {deleted_artist_rows} orphaned artists")

    conn.commit()    

def delete_unwanted_albums_again(conn):
    cursor = conn.cursor()

    min_tracks_played = config['criteria_to_keep']['min_tracks_played']
    min_scrobbled = config['criteria_to_keep']['min_scrobbled']
    min_length = config['criteria_to_keep']['min_length']
    min_tracks = config['criteria_to_keep']['min_tracks']

    # Delete albums with num_tracks = 1 and scrobble_count = 1, unless saved is not NULL
    cursor.execute("DELETE FROM albums WHERE num_tracks = 1 AND scrobble_count = 1 AND saved IS NULL AND (release_length < 30 OR release_length IS NULL)")
    deleted_album_rows = cursor.rowcount
    cursor.execute("""
        DELETE FROM albums WHERE num_tracks < ? AND scrobble_count < ? AND saved IS NULL AND release_length < ? AND tracks_mb < ?
    """, (min_tracks_played, min_scrobbled, min_length, min_tracks))
    deleted_album_rows += cursor.rowcount

    if config['criteria_to_keep']['singles'] == "yes":
        cursor.execute("DELETE FROM albums WHERE release_type IS ('Single' OR 'single') AND saved IS NULL")
        deleted_album_rows += cursor.rowcount
    if config['criteria_to_keep']['eps'] == "yes":
        cursor.execute("DELETE FROM albums WHERE release_type IS ('EP' OR 'ep') AND saved IS NULL")
        deleted_album_rows += cursor.rowcount
    if config['criteria_to_keep']['compilations'] == "yes":
        cursor.execute("DELETE FROM albums WHERE release_type IS ('Compilation' OR 'compilation') AND saved IS NULL")
        deleted_album_rows += cursor.rowcount
    print(f"Deleted {deleted_album_rows} unwanted albums")

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

def clean_new_album_names(conn):
    # Create a new cursor object
    cursor = conn.cursor()

    # Fetch all the album names from the albums table
    cursor.execute("SELECT album_id, album_name FROM new_albums")
    albums = cursor.fetchall()

    # Iterate through the albums and clean their names using remove_special_editions
    for album in albums:
        album_id = album[0]
        original_album_name = album[1]
        cleaned_album_name = remove_special_editions(original_album_name)

        # Update the album name in the albums table
        cursor.execute("UPDATE new_albums SET album_name=? WHERE album_id=?", (cleaned_album_name, album_id))

    # Commit the changes to the database
    conn.commit()

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
    num_duplicates_removed = 0

    # For each unique album, find all fuzzy matches from the original albums
    for unique_album in unique_albums:
        matched_albums = find_fuzzy_matches(unique_album, albums)

        # Determine which album to keep and update
        album_to_keep = determine_album_to_keep(unique_album, matched_albums)

        if album_to_keep is not None:
            # Update the album_to_keep in the database
            update_album_in_database(conn, album_to_keep)

            # Delete the other duplicate albums from the database
            num_deleted = delete_duplicate_albums(conn, matched_albums, album_to_keep)
            num_duplicates_removed += num_deleted

    conn.commit()
    print(f"Removed {num_duplicates_removed} duplicate albums.")

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
    album_names = [(album[1], album[6] == 'saved', album[0]) for album in matched_albums]
    
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
    num_deleted = 0
    for album_id in album_ids_to_delete:
        cursor.execute("DELETE FROM albums WHERE album_id = ?", (album_id,))
        num_deleted += 1

    return num_deleted

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

# Stores an intermediate result in the intermediate_results table
def store_intermediate_result(conn, key, value):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO intermediate_results (key, value)
        VALUES (?, ?)
    """, (key, value))
    conn.commit()

# Compares two strings after removing any text within parentheses or brackets.
def ignore_parentheses_and_brackets(string1, string2):
    pattern = r' ?[\(\[].*?[\)\]]'
    cleaned_string1 = re.sub(pattern, '', string1).lower()
    cleaned_string2 = re.sub(pattern, '', string2).lower()
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


def update_artist_genres(conn):

    cursor= conn.cursor()

    # Fetch album genres and associated artists
    cursor.execute("SELECT artist_name, genre FROM albums WHERE genre IS NOT NULL")
    album_genres = cursor.fetchall()

    for artist, album_genre in album_genres:
        # Fetch the existing genres for this artist
        cursor.execute("SELECT genre FROM artists WHERE artist_name = ?", (artist,))
        existing_genre =  cursor.fetchone()

        if existing_genre is not None:
            existing_genre = existing_genre[0]

        # If the artist has no genres yet, add the album's genre directly
        if existing_genre is None or existing_genre == '':
            cursor.execute("UPDATE artists SET genre = ? WHERE artist_name = ?", (album_genre, artist))
        else:
            # If the artist already has genres, append the album's genre if it's not already there
            existing_genres = set(existing_genre.split(","))
            album_genres = set(album_genre.split(","))
            merged_genres = existing_genres.union(album_genres)
            merged_genre_string = ",".join(merged_genres)
            cursor.execute("UPDATE artists SET genre = ? WHERE artist_name = ?", (merged_genre_string, artist))

    conn.commit()
    # conn.close()


def update_artist_scrobbles(conn):
    # Connect to the SQLite database
    cursor = conn.cursor()

    # Fetch all artist names
    cursor.execute("SELECT artist_name FROM artists")
    artist_names = cursor.fetchall()

    for artist in artist_names:
        # Fetch scrobble counts of all albums related to this artist
        cursor.execute("SELECT SUM(scrobble_count) FROM albums WHERE artist_name = ?", (artist[0],))
        scrobble_count = cursor.fetchone()

        if scrobble_count[0] is not None:
            # Update scrobble count in artists table
            cursor.execute("UPDATE artists SET scrobble_count = ? WHERE artist_name = ?", (scrobble_count[0], artist[0]))

    conn.commit()


def update_artist_new_scrobbles(conn):
    # Connect to the SQLite database
    cursor = conn.cursor()

    # Fetch all artist names
    cursor.execute("SELECT DISTINCT artist_name FROM new_albums")
    artist_names = cursor.fetchall()

    for artist in artist_names:
        # Fetch scrobble counts of all albums related to this artist
        cursor.execute("SELECT SUM(scrobble_count) FROM albums WHERE artist_name = ?", (artist[0],))
        scrobble_count = cursor.fetchone()

        if scrobble_count[0] is not None:
            # Update scrobble count in artists table
            cursor.execute("UPDATE artists SET scrobble_count = ? WHERE artist_name = ?", (scrobble_count[0], artist[0]))

    conn.commit()


def update_database_from_csv(conn):

    albumdat = input('Please enter the name of the album data CSV file (e.g. album_data.csv): ')
    artistdat = input('Please enter the name of the artist data CSV file (e.g. artist_data.csv): ')

    try:
        cursor = conn.cursor()

        # Read the CSV file using pandas. If your CSV file is tab-delimited, use '\t' as the delimiter
        df = pd.read_csv(albumdat, delimiter='\t')

        # Iterate over each row in the DataFrame
        for idx, row in df.iterrows():
            try:
                # Update each row in the database
                cursor.execute("""
                    UPDATE albums
                    SET
                        release_year = COALESCE(release_year, ?),
                        genre = COALESCE(genre, ?),
                        release_type = COALESCE(release_type, ?),
                        cover_art_url = COALESCE(cover_art_url, ?),
                        spotify_url = COALESCE(spotify_url, ?),
                        mbid = COALESCE(mbid, ?),
                        country = COALESCE(country, ?),
                        release_length = COALESCE(release_length, ?),
                        spotify_id = COALESCE(spotify_id, ?)
                    WHERE album_name = ? AND artist_name = ?
                """, (row['release_year'], row['genre'], row['release_type'], row['cover_art_url'], row['spotify_url'], row['mbid'], row['country'], row['release_length'], row['spotify_id'], row['album_name'], row['artist_name']))
            except sqlite3.Error as e:
                print(f"An error occurred when updating row {idx}: {e}")

        conn.commit()
        print("Done importing album data from CSV!")
    except Exception as e:
        print(f"An error occurred: {e}")

    try:
        cursor = conn.cursor()

        # Read the CSV file using pandas. If your CSV file is tab-delimited, use '\t' as the delimiter
        df = pd.read_csv(artistdat, delimiter='\t')

        # Iterate over each row in the DataFrame
        for idx, row in df.iterrows():
            try:
                # Update each row in the database
                cursor.execute("""
                    UPDATE artists
                    SET
                        spotify_url = COALESCE(spotify_url, ?)
                    WHERE artist_name = ?
                """, (row['spotify_url'], row['artist_name']))
            except sqlite3.Error as e:
                print(f"An error occurred when updating row {idx}: {e}")

        # Commit the changes and close the connection
        conn.commit()
        print("Done importing artist data from CSV!")
    except Exception as e:
        print(f"An error occurred: {e}")

def download_latest_csv(url, filename):
    response = requests.get(url)
    with open(filename, 'wb') as file:
        file.write(response.content)
        
def update_database_from_github_csv(conn):
    # Hardcoded URLs of the album and artist data CSV files on GitHub
    albumdat_url = 'https://raw.githubusercontent.com/7anooch/MusicLibrary-data/main/album_data_20230909.csv'
    artistdat_url = 'https://raw.githubusercontent.com/7anooch/MusicLibrary-data/main/artist_data_20230909.csv'
    
    # Set the names of the local files to be downloaded
    albumdat = "album_data_20230909.csv"
    artistdat = "artist_data_20230909.csv"

    # Download the CSV files
    download_latest_csv(albumdat_url, albumdat)
    download_latest_csv(artistdat_url, artistdat)

    cursor = conn.cursor()

    album_counter = 0
    artist_counter = 0

    try:
        # Read the CSV file using pandas. If your CSV file is tab-delimited, use '\t' as the delimiter
        df = pd.read_csv(albumdat, delimiter='\t')

        # Iterate over each row in the DataFrame
        for idx, row in df.iterrows():
            try:
                # Update each row in the database
                cursor.execute("""
                    UPDATE albums
                    SET
                        release_year = COALESCE(release_year, ?),
                        genre = COALESCE(genre, ?),
                        release_type = COALESCE(release_type, ?),
                        cover_art_url = COALESCE(cover_art_url, ?),
                        spotify_url = COALESCE(spotify_url, ?),
                        mbid = COALESCE(mbid, ?),
                        country = COALESCE(country, ?),
                        release_length = COALESCE(release_length, ?),
                        spotify_id = COALESCE(spotify_id, ?)
                    WHERE album_name = ? AND artist_name = ?
                """, (row['release_year'], row['genre'], row['release_type'], row['cover_art_url'], row['spotify_url'], row['mbid'], row['country'], row['release_length'], row['spotify_id'], row['album_name'], row['artist_name']))

                if cursor.rowcount > 0:  # if an album was updated
                    album_counter += 1

            except sqlite3.Error as e:
                print(f"An error occurred when updating row {idx}: {e}")

        conn.commit()
        print(f"Done importing album data from CSV! {album_counter} albums found.")
    except Exception as e:
        print(f"An error occurred: {e}")

    try:
        # Read the CSV file using pandas. If your CSV file is tab-delimited, use '\t' as the delimiter
        df = pd.read_csv(artistdat, delimiter='\t')

        # Iterate over each row in the DataFrame
        for idx, row in df.iterrows():
            try:
                # Update each row in the database
                cursor.execute("""
                    UPDATE artists
                    SET
                        spotify_url = COALESCE(spotify_url, ?)
                    WHERE artist_name = ?
                """, (row['spotify_url'], row['artist_name']))

                if cursor.rowcount > 0:  # if an artist was updated
                    artist_counter += 1

            except sqlite3.Error as e:
                print(f"An error occurred when updating row {idx}: {e}")

        # Commit the changes and close the connection
        conn.commit()
        print(f"Done importing artist data from CSV! {artist_counter} artists found.")
    except Exception as e:
        print(f"An error occurred: {e}")


