import sqlite3
import csv

# connect to your database
conn = sqlite3.connect('MusicLibrary.db')
cursor = conn.cursor()

# select the required data
cursor.execute("""
    SELECT album_name, artist_name, release_year, genre, 
           release_type, cover_art_url, spotify_url, 
           mbid, country, release_length, tracks_mb, spotify_id 
    FROM albums
""")

# fetch all the data
data = cursor.fetchall()

# specify the headers
headers = ["album_name", "artist_name", "release_year", "genre",
           "release_type", "cover_art_url", "spotify_url",
           "mbid", "country", "release_length", "tracks_mb", "spotify_id"]

# write the data to a CSV file
with open('album_data.csv', 'w', newline='') as f:
    writer = csv.writer(f, delimiter='\t') 
    writer.writerow(headers)
    writer.writerows(data)


cursor.execute("""
    SELECT artist_name, artist_image, spotify_url
    FROM artists
""")

# fetch all the data
data = cursor.fetchall()

# specify the headers
headers = ["artist_name", "artist_image", "spotify_url"]

# write the data to a CSV file
with open('artist_data.csv', 'w', newline='') as f:
    writer = csv.writer(f, delimiter='\t') 
    writer.writerow(headers)
    writer.writerows(data)

print('Data successfully saved to album_data.csv!')

# don't forget to close the connection
conn.close()

