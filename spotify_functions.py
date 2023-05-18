import requests, spotipy, base64, webbrowser, json, sqlite3, schedule, time, pylast, re, random
import musicbrainzngs, discogs_client, requests.exceptions, urllib.parse, unicodedata, datetime, os
from spotipy.oauth2 import SpotifyClientCredentials
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.parse import quote_plus
from requests.exceptions import ConnectTimeout
from pylast import PyLastError
from rapidfuzz import fuzz, process 

with open('keys.json', 'r') as f:
	config = json.load(f)

SPOTIFY_CLIENT_SECRET = config['spotify']['client_secret']
SPOTIFY_CLIENT_ID = config['spotify']['client_id']
SPOTIFY_REDIRECT_URI = config['spotify']['redirect_uri']
SPOTIFY_SCOPE = config['spotify']['scope']
db_name = config['database']['name']


def get_spotify_authorization_code(client_id, redirect_uri, scope):
	""" Gets Spotify authorization code by opening a web browser for user authentication"""
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

def get_spotify_access_token(client_id, client_secret, redirect_uri, scope):
	""" Gets Spotify access token using the authorization code"""
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

# check
def fetch_spotify_saved_albums(access_token, limit=50, offset=0, retries=3, delay=5):
	""" Fetches saved albums from the user's Spotify account with retry logic and a delay between attempts """
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

# check
def save_spotify_saved_albums_to_db(conn, access_token):
	""" Saves the user's saved albums from Spotify to the database"""
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

# check
def get_recent_saved_albums(spotify_token, after_timestamp):
	""" Fetches recently saved albums from Spotify"""
	headers = {
		'Authorization': f'Bearer {spotify_token}'
	}
	params = {
		'limit': 50,
		'after': after_timestamp
	}
	response = requests.get('https://api.spotify.com/v1/me/albums', headers=headers, params=params)
	return response.json()

# check
def update_saved_albums(conn, recent_albums):
	""" Updates the saved_albums table with recent albums from Spotify"""
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

# check
def parse_saved_albums(response_json):
	""" Extract artist and album names from Spotify response JSON"""
	albums = []
	for item in response_json['items']:
		album = item['album']
		artist_name = album['artists'][0]['name']
		album_name = album['name']
		albums.append((artist_name, album_name))
	return albums

# check
def search_spotify(query, query_type, spotify_token, max_retries=3, delay=2):
	""" Search Spotify with query and query_type using the given access token"""
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

def update_artist_and_album_urls(conn, spotify_token):
	""" Updates artist and album Spotify URLs in the database"""
	cursor = conn.cursor()
	cursor.execute("SELECT artist_id, artist_name, spotify_url FROM artists WHERE spotify_url IS NULL")
	artists = cursor.fetchall()

	headers = {
		'Authorization': f'Bearer {spotify_token}'
	}

	x = 0
	for artist_id, artist_name, artist_spotify_url in artists:
		artist_search_url = f'https://api.spotify.com/v1/search?q={urllib.parse.quote(artist_name)}&type=artist&limit=1'
		try:
			artist_response = requests.get(artist_search_url, headers=headers)
			artist_data = artist_response.json()

			if x % 10:
				time.sleep(1)
			if 'artists' in artist_data and artist_data['artists']['items']:
				artist_spotify_url = artist_data['artists']['items'][0]['external_urls']['spotify']
				cursor.execute("UPDATE artists SET spotify_url=? WHERE artist_id=?", (artist_spotify_url, artist_id))
				print(f"Updated Spotify URL for artist {artist_name}: {artist_spotify_url}")

		except Exception as e:
			print(f"Error fetching Spotify URL for artist {artist_name}: {e}")
		x += 1
	cursor.execute("SELECT album_id, artist_name, album_name, spotify_url FROM albums WHERE spotify_url IS NULL")
	albums = cursor.fetchall()

	for album_id, artist_name, album_name, album_spotify_url in albums:
		album_search_url = f'https://api.spotify.com/v1/search?q=album:{urllib.parse.quote(str(album_name))}%20artist:{urllib.parse.quote(str(artist_name))}&type=album&limit=1'

#        album_search_url = f'https://api.spotify.com/v1/search?q=album:{urllib.parse.quote(album_name)}%20artist:{urllib.parse.quote(artist_name)}&type=album&limit=1'
		try:
			album_response = requests.get(album_search_url, headers=headers)
			album_data = album_response.json()

			if x % 10:
				time.sleep(1)
			if 'albums' in album_data and album_data['albums']['items']:
				album_spotify_url = album_data['albums']['items'][0]['external_urls']['spotify']
				cursor.execute("UPDATE albums SET spotify_url=? WHERE album_id=?", (album_spotify_url, album_id))
				print(f"Updated Spotify URL for album {artist_name} - {album_name}: {album_spotify_url}")

		except Exception as e:
			print(f"Error fetching Spotify URL for album {artist_name} - {album_name}: {e}")
		x += 1
	conn.commit()
	print("Updated artist and album URLs.")


def update_missing_album_data(conn, spotify_access_token):
	""" Updates missing album data (release year, cover art) from Spotify API """
	cursor = conn.cursor() # Get albums with missing release year or cover art
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

def get_album_info(spotify_access_token, spotify_album_id):
	""" Fetches album info (release year, cover art URL) from Spotify API"""
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

# NOT USED CURRENTLY
def url_to_uri(spotify_url):
	""" Converts Spotify URL to Spotify URI"""
	if spotify_url is None:
		return None
	return spotify_url.replace("https://open.spotify.com/album/", "spotify:album:")

def save_recent_saved_albums(conn, token, last_update):
	""" Saves recently saved albums from the Spotify API to the intermediate_results table."""
	response_json = get_recent_saved_albums(token, last_update)
	cursor = conn.cursor()
	cursor.execute("""
	INSERT OR REPLACE INTO intermediate_results (key, value)
	VALUES (?, ?)
	""", ("recent_saved_albums", json.dumps(response_json)))
	conn.commit()

# very confused by thus function? ckeck
def parse_and_insert_saved_albums(conn):
	""" Parses and inserts saved albums from Spotify API response"""
	cursor = conn.cursor()
	cursor.execute("SELECT value FROM intermediate_results WHERE key = ?", ("recent_saved_albums",))
	response_json = json.loads(cursor.fetchone()[0])
	new_saved_albums = parse_saved_albums(response_json)
	insert_new_saved_albums(conn, new_saved_albums)

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
		if x % 4:
			time.sleep(2)
		try:
			artist_response = requests.get(artist_search_url, headers=headers)
			if artist_response.status_code != 200:
				print(f"Error fetching Spotify URL for artist {artist_name}: Status {artist_response.status_code}, {artist_response.text}")
				continue
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
		if x % 3:
			time.sleep(2)
		try:
			album_response = requests.get(album_search_url, headers=headers)
			if album_response.status_code != 200:
				print(f"Error fetching Spotify URL for album {album_name}: Status {album_response.status_code}, {album_response.text}")
				continue
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

def is_spotify_token_valid(access_token):
	headers = {
		'Authorization': f'Bearer {access_token}'
	}
	response = requests.get('https://api.spotify.com/v1/me', headers=headers)

	if response.status_code == 200:
		return print('True')
	else:
		return print('False')