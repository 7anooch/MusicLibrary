import requests, spotipy, base64, os, webbrowser, sqlite3, json, schedule, time, datetime, pylast, re
import musicbrainzngs, discogs_client, requests.exceptions, sys, urllib.parse
import matplotlib.pyplot as plt
from spotipy.oauth2 import SpotifyClientCredentials
from urllib.parse import urlencode, urlparse, parse_qs
from functions import *
from lastfm_functions import *
from musicbrainz_functions import *
from spotify_functions import *
from rym_functions import *
from rapidfuzz import fuzz, process 

with open('keys.json', 'r') as f:
    config = json.load(f)

# API keys and authentication
SPOTIFY_CLIENT_SECRET = config['spotify']['client_secret']
SPOTIFY_CLIENT_ID = config['spotify']['client_id']
LASTFM_API_KEY = config['lastfm']['api_key']
LASTFM_SECRET = config['lastfm']['secret']
LASTFM_USER = config['lastfm']['user']
SPOTIFY_REDIRECT_URI = config['spotify']['redirect_uri']
SPOTIFY_SCOPE = config['spotify']['scope']
db_name = config['database']['name']
SCRAPERAPI_KEY = config['scraperapi']['key']

if config['scraperapi']['use'] == 'yes':
    USE_SCRAPER = True
else:
    USE_SCRAPER = False


# discogs_client.user_agent = "MusicLibrary/0.1"  # Replace with your app name and version
d = discogs_client.Client("MusicLibrary/0.1", user_token=config['discogs']['token'])
musicbrainzngs.set_useragent("YourAppName", "0.1", "7anooch@gmail.com")


# db_path = "lastfm_dump.db"  # Replace with the path to your SQLite database file
#db_name = "MusicLibrary.db"
conn = sqlite3.connect(db_name, isolation_level=None)
conn.create_function("IGNORE_PARENTHESIS_AND_BRACKETS", 2, ignore_parentheses_and_brackets)

#fetch_scrobbles_and_save_to_db(conn)
#(LASTFM_API_KEY, LASTFM_USER, conn)
#delete_duplicate_scrobbles(conn)
 

cursor = conn.cursor()
#cursor.execute("SELECT album_id, spotify_url FROM albums")
#album_data = cursor.fetchall()
#for album_id, spotify_url in album_data:
#    spotify_uri = url_to_uri(spotify_url)
#    cursor.execute("UPDATE albums SET spotify_uri = ? WHERE album_id = ?", (spotify_uri, album_id))

#add_latest_timestamp_to_updates(conn)

# update_album_mbid(conn)


# update_rym_genres(conn, use_scraperapi=USE_SCRAPER)
# update_release_info(conn)
# update_albums_with_cover_arts(conn, LASTFM_API_KEY)
# update_artists_with_images(conn)
# update_albums_with_lastfm_release_years(conn, LASTFM_API_KEY)
# stoken = get_spotify_access_token(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI, SPOTIFY_SCOPE)
# is_spotify_token_valid(stoken)
# #update_missing_album_data(conn, stoken)
# update_spotify_data(conn, stoken)





# Commit the changes to the database
conn.commit()

#spotify_access_token = get_spotify_access_token(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI, SPOTIFY_SCOPE)
#update_missing_album_data(conn, spotify_access_token)

# if should_execute_function(conn):
#     update_databases(conn, LASTFM_USER, LASTFM_API_KEY)
#     set_last_executed_date(conn, datetime.datetime.now())

#conn.close()

if __name__ == "__main__":
    main()
    table_name = 'tracks'
    num_entries = count_entries_in_table(table_name)
    print(f"The {table_name} table has {num_entries} entries.")
    table_name = 'saved_albums'
    num_entries = count_entries_in_table(table_name)
    print(f"The {table_name} table has {num_entries} entries.")
    table_name = 'albums'
    num_entries = count_entries_in_table(table_name)
    print(f"The {table_name} table has {num_entries} entries.")
    table_name = 'artists'
    num_entries = count_entries_in_table(table_name)
    print(f"The {table_name} table has {num_entries} entries.")
    

