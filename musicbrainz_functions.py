import requests, spotipy, base64, webbrowser, json, sqlite3, schedule, time, pylast, re, random
import musicbrainzngs, discogs_client, requests.exceptions, urllib.parse, unicodedata, datetime, os
from urllib.parse import urlencode, urlparse, parse_qs
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
from requests.exceptions import ConnectTimeout
from pylast import PyLastError
from rapidfuzz import fuzz, process 
from lastfm_functions import *


with open('keys.json', 'r') as f:
    config = json.load(f)

db_name = config['database']['name']
d = discogs_client.Client("MusicLibrary/0.1", user_token=config['discogs']['token'])
MUSICBRAINZ_API_URL = "https://musicbrainz.org/ws/2/"

# NOT CURRENTLY USED
def fetch_musicbrainz_artist_data(artist_name):
    """ Fetches artist data from MusicBrainz based on artist name """
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

# NOT CURRENTLY USED
def get_musicbrainz_genres(artist_name, album_name):
    """ Fetches genres for the specified artist and album from MusicBrainz """
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

# NOT CURRENTLY USED
def update_albums_with_genres(conn):
    """ Update album genres and cover art using MusicBrainz data """
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

# NOT CURRENTLY USED
def fetch_cover_art_url(release_id):
    """ Get cover art URL using MusicBrainz release ID """
    cover_art_url = f'https://coverartarchive.org/release/{release_id}/front'
    response = requests.head(cover_art_url)
    
    if response.status_code == 200:
        return cover_art_url
    else:
        print(f"No cover art found for release ID: {release_id}")
        return None

# NOT CURRENTLY USED
def get_musicbrainz_cover_art_url(artist, album):
    """ Get MusicBrainz cover art URL using artist and album names """
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

# NOT CURRENTLY USED
def get_cover_art_url(artist_name, album_name):
    """ Get cover art URL using MusicBrainz data and artist and album names """
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

# NOT CURRENTLY USED
def get_musicbrainz_release_year(artist_name, album_name):
    """ Fetches the release year for a given artist and album using the MusicBrainz API """
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

# NOT CURRENTLY USED
def update_albums_with_release_years(conn):
    """ Updates the release_year column for all albums in the database."""
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

def search_release_mbid(artist_name, album_name, max_retries=3):
    """ Searches for MusicBrainz release MBID using artist and album names """
    with open("missing_mbid.txt", "a+") as missing_file:
        missing_file.seek(0)
        missing_entries = missing_file.readlines()
        entry = f"{artist_name} - {album_name}\n"
        if entry in missing_entries:
          #  print(f"Skipping {artist_name} - {album_name} (already in missing_mbid.txt)")
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
 
def update_album_mbid(conn):
    """ Updates the MusicBrainz release MBID for albums in the database"""
    cursor = conn.cursor()
    cursor.execute("SELECT album_id, artist_name, album_name FROM albums WHERE mbid IS NULL")
    albums = cursor.fetchall() # Fetch all albums from the database
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

def update_release_info(conn):
    """ Updates missing release information of albums in the database using MusicBrainz API"""
    musicbrainzngs.set_useragent("MusicLibrary", "0.1", "YourAppURL") # Configure the MusicBrainz client with your app information

    # Get all albums from the database that are missing release_type, country, release_length, or tracks_mb
    cursor = conn.cursor()
    cursor.execute("SELECT album_id, mbid FROM albums WHERE release_type IS NULL OR country IS NULL OR release_length IS NULL OR tracks_mb IS NULL")
    albums = cursor.fetchall()

    # Counter for batch commits
    update_count = 0

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
            release_type = release_group.get("primary-type", "album")

            # Get the country of release
            country = release.get("country", "XW")

            # Calculate the release length (sum of track durations) in minutes and the total number of tracks
            release_length = 0
            total_tracks = 0
            for medium in release["medium-list"]:
                for track in medium["track-list"]:
                    if "length" in track:
                        release_length += int(track["length"])
                    total_tracks += 1

            release_length = int(release_length / 60000)  # Convert milliseconds to minutes and cast to integer

            # Update the 'release_type', 'country', 'release_length', and 'tracks_mb' columns in the 'albums' table
            cursor.execute("""
                UPDATE albums 
                SET release_type = COALESCE(release_type, ?),
                country = COALESCE(country, ?),
                release_length = COALESCE(release_length, ?),
                tracks_mb = COALESCE(tracks_mb, ?)
                WHERE album_id = ?
            """, (release_type, country, release_length, total_tracks, album_id))

            update_count += 1
            print(f"Updated release info for album {album_id}: type={release_type}, country={country}, length={release_length} minutes, tracks={total_tracks}")

            if update_count % 25 == 0:  # If update_count is a multiple of 25
                conn.commit()  # Commit the changes to the database
                print(f"Committed updates for {update_count} albums.")

        except musicbrainzngs.ResponseError as err:
            print(f"Response error with album {album_id}, MBID {mbid}: {err}")
        except musicbrainzngs.MusicBrainzError as e:
            print(f"Error fetching release info for album {album_id}: {e}")

    if update_count % 25 != 0:  # If the last batch was less than 25
        conn.commit()  # Commit the remaining updates
        print(f"Committed updates for {update_count} albums.")




# ----- Discogs functions -----

# NOT CURRENTLY USED
def get_discogs_cover_art_url(artist_name, album_name):
    """ Fetches the album cover art URL from the Discogs API for a given artist and album."""
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

# NOT CURRENTLY USED
def get_discogs_artist_image_url(artist_name):
    """ Fetches the artist image URL from the Discogs API for a given artist. """
    results = d.search(artist_name, type='artist')

    if results and results.pages > 0:
        best_match = results[0]

        if 'thumb' in best_match.data:
            return best_match.data['thumb']
    return None