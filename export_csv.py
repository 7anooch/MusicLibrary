import sqlite3
import csv

# connect to your database
conn = sqlite3.connect('MusicLibrary.db')
cursor = conn.cursor()

# select the required data
cursor.execute("""
    SELECT album_name, artist_name, release_year, genre, 
           release_type, cover_art_url, spotify_url, 
           mbid, country, release_length, spotify_id 
    FROM albums
""")

# fetch all the data
data = cursor.fetchall()

# specify the headers
headers = ["album_name", "artist_name", "release_year", "genre",
           "release_type", "cover_art_url", "spotify_url",
           "mbid", "country", "release_length", "spotify_id", "tracks_mb"]

# write the data to a CSV file
with open('album_data.csv', 'w', newline='') as f:
    writer = csv.writer(f, delimiter='\t') 
    writer.writerow(headers)
    writer.writerows(data)

print('Data successfully saved to album_data.csv!')

# don't forget to close the connection
conn.close()

