from typing import Any, List, Optional
from connect_db import Database
from contextlib import closing

from objects import Category
from objects import Movie

db = Database()
conn = db.connect()


def make_category(row: Any) -> Category:
    return Category(row["categoryID"], row["categoryName"])


def make_movie(row: Any) -> Movie:
    return Movie(row["movieID"], row["name"], row["year"], row["minutes"], make_category(row))


def get_categories() -> List[Category]:
    query = '''SELECT categoryID, name as categoryName FROM Category'''
    with closing(conn.cursor()) as c:
        c.execute(query)
        results = c.fetchall()

    categories: List[Category] = []
    for row in results:
        categories.append(make_category(row))
    return categories


def get_category(category_id: int) -> Optional[Category]:
    query = '''SELECT categoryID, name as categoryName from CATEGORY WHERE categoryID = ?'''
    with closing(conn.cursor()) as c:
        c.execute(query, (category_id,))
        row = c.fetchone()
        if row:
            return make_category(row)
        else:
            return None


def make_movie_list(results) -> List[Movie]:
    movies: List[Movie] = []
    for row in results:
        movies.append(make_movie(row))
    return movies
