import sqlite3

class DatabaseHandler:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = self.connect_db()

    def connect_db(self):
        return sqlite3.connect(self.db_path)

    def get_albums(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT artist_name, album_name, genres, release_year, scrobble_count FROM albums")
        return cursor.fetchall()

    def close(self):
        self.conn.close()
