import sqlite3
from movie_repository import MovieRepository

class Database(MovieRepository):
    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect("movies.sqlite")
        conn.row_factory = sqlite3.Row

        return conn

    def close(self):
        conn: sqlite3.Connection = self.connect()
        if conn:
            conn.close()
