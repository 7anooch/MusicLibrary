import requests, spotipy, base64, os, webbrowser, sqlite3, json, schedule, time, datetime, pylast, re
import musicbrainzngs, discogs_client, requests.exceptions, sys, urllib.parse, argparse
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

if config['spotify']['use'] == 'yes':
    use_spotify = True
elif config['spotify']['use'] == 'no':
    use_spotify = False


# discogs_client.user_agent = "MusicLibrary/0.1"  # Replace with your app name and version
d = discogs_client.Client("MusicLibrary/0.1", user_token=config['discogs']['token'])
musicbrainzngs.set_useragent("MusicLibrary", "0.1", "youremail@gmail.com")

conn = sqlite3.connect(db_name, isolation_level=None)
conn.create_function("IGNORE_PARENTHESIS_AND_BRACKETS", 2, ignore_parentheses_and_brackets)


cursor = conn.cursor()
#cursor.execute("SELECT album_id, spotify_url FROM albums")
#album_data = cursor.fetchall()
#for album_id, spotify_url in album_data:
#    spotify_uri = url_to_uri(spotify_url)
#    cursor.execute("UPDATE albums SET spotify_uri = ? WHERE album_id = ?", (spotify_uri, album_id))


# Commit the changes to the database
conn.commit()

# if should_execute_function(conn):
#     update_databases(conn, LASTFM_USER, LASTFM_API_KEY)
#     set_last_executed_date(conn, datetime.datetime.now())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--update', action='store_true', help='update the database')
    parser.add_argument('--import_csv', action='store_true', help='import data from csv')
    parser.add_argument('--reset', action='store_true', help='import data from csv')
    args = parser.parse_args()
    main(args.update, args.import_csv, args.reset)
    if check_if_table_exists(conn, "albums"):
        if use_spotify:
            table_name = 'saved_albums'
            num_entries = count_entries_in_table(table_name)
            print(f"The {table_name} table has {num_entries} entries.")
        table_name = 'albums'
        num_entries = count_entries_in_table(table_name)
        print(f"The {table_name} table has {num_entries} entries.")
        table_name = 'artists'
        num_entries = count_entries_in_table(table_name)
        print(f"The {table_name} table has {num_entries} entries.")

conn.close() 

