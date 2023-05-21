import requests, base64, webbrowser, json, sqlite3, schedule, time, pylast, re, random
import requests.exceptions, urllib.parse, unicodedata, datetime, os
from urllib.parse import urlencode, urlparse, parse_qs
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
from requests.exceptions import ConnectTimeout
from pylast import PyLastError
from rymscraper import rymscraper
from rapidfuzz import fuzz, process 
import selenium.common.exceptions


with open('keys.json', 'r') as f:
	config = json.load(f)

db_name = config['database']['name']
SCRAPERAPI_KEY = config['scraperapi']['key']

if config['scraperapi']['use'] == 'yes':
	USE_SCRAPER = True
else:
	USE_SCRAPER = False


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

def get_genres_from_rym(rym_url, use_scraperapi=True):
	""" Fetches genres from RateYourMusic using given RYM URL"""
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

def get_rym_genres(artist_name, album_name, network, retry_attempts=3, retry_delay=5):
    """ Gets genres for an album from RateYourMusic"""
    search_query = f"{artist_name} - {album_name}"

    for attempt in range(retry_attempts):
        try:
            album_infos = network.get_album_infos(name=search_query)

            if "Genres" in album_infos:
                genres = album_infos["Genres"]
                genres = genres.replace('\n', ', ')  # Replace line breaks with a comma and space
                return genres
            else:
                print(f"No 'Genres' found in album info for {artist_name} - {album_name}")
                return None

        except AttributeError as e:
            print(f"AttributeError occurred while fetching RYM genres for {artist_name} - {album_name}: {e}")
            return None
        except IndexError as e:
            print(f"IndexError occurred while fetching RYM genres for {artist_name} - {album_name}: {e}")
            return None
        except TypeError as e:
            print(f"TypeError occurred while fetching RYM genres for {artist_name} - {album_name}: {e}")
            return None
        except selenium.common.exceptions.WebDriverException as e:
            if attempt < retry_attempts - 1:
                print(f"Attempt {attempt+1} of {retry_attempts} failed with WebDriverException: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"All attempts failed for {artist_name} - {album_name}. Please check your network connection and ensure rateyourmusic.com is online.")
                return None


# NOT CURRENTLY USED
def search_album_on_rym(artist, album_title):
	""" Searches for an album on RateYourMusic and returns its URL"""
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

def update_rym_genres(conn, use_scraperapi=False):
	""" Updates albums with RYM genres if not already set"""
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

		if x % 4 == 0 and use_scraperapi==False:
			time.sleep(15)
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
		elif use_scraperapi==False:
			skipped_albums.add((artist_name, album_name))
			save_skipped_albums_list(skipped_albums_file, skipped_albums)
		else:
			print(f"Couldn't fetch RYM genres for {artist_name} - {album_name}")
			skipped_albums.add((artist_name, album_name))
			save_skipped_albums_list(skipped_albums_file, skipped_albums)

	network.browser.close()
	network.browser.quit()


def generate_rym_url(conn, artist, album_title, release_type, album_id, num_tracks):
	""" Generates two RYM URLs for a given album (with dashes and underscores) """
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

def clean_for_rym_url(text):
	""" Normalizes and cleans up text for use in RYM URL generation"""
	replacements = [
		("!", ""),          # Remove exclamation marks
		("&", "and"),       # Replace '&' with 'and'
		(",", ""),          # Remove commas
		(":", ""),
		(";", ""),          
		("'", ""),          
		("...", "-"),         
		(".", ""),          
		(" - ", ""),
		("-", "-"), 
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



def remove_special_editions(album_name):
    if album_name is None:
        return ""

    words = ['edition', 'anniversary', 'bonus', 'reissue', 'issue', 'deluxe', 'remaster', 'remastered', 'version']
    pattern = r'(\[.*(' + '|'.join(words) + ').*\]|\(.*(' + '|'.join(words) + ').*\)|\{.*(' + '|'.join(words) + ').*\})'
    modified_album_name = re.sub(pattern, '', album_name, flags=re.IGNORECASE)

    return modified_album_name.strip()
