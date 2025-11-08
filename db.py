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
    query = '''SELECT categoryID, name as categoryName from Category WHERE categoryID = ?'''
    with closing(conn.cursor()) as c:
        c.execute(query, (category_id,))
        row: Any = c.fetchone()
        if row:
            return make_category(row)
        else:
            return None


def make_movie_list(results) -> List[Movie]:
    movies: List[Movie] = []
    for row in results:
        movies.append(make_movie(row))
    return movies


def get_movies_by_category(category_id: int) -> List[Movie]:
    query = '''SELECT movieID, m.categoryID, c.name AS categoryName, m.name AS title, year, minutes AS runtime 
               FROM Movie m JOIN Category c on c.categoryID = m.categoryID 
               WHERE m.categoryID = ?'''
    with closing(conn.cursor()) as c:
        c.execute(query, (category_id,))
        results = c.fetchall()

    return make_movie_list(results)


def get_movies_by_year(year: int) -> List[Movie]:
    query = '''SELECT movieID, categoryID, c.categoryName AS cateogryName, m.name AS title, year, minutes AS runtime 
               FROM Movie m JOIN Category c on c.categoryID = m.categoryID
               WHERE year = ?'''
    with closing(conn.cursor()) as c:
        c.execute(query, (year,))
        results = c.fetchall()

    return make_movie_list(results)


def add_movie(movie: Movie) -> None:
    sql = '''INSERT INTO Movie m (categoryID, name, year, minutes) VALUES (?, ?, ?, ?)'''
    with closing(conn.cursor()) as c:
        c.execute(sql, (movie.category.id, movie.name, movie.year, movie.minutes))
    conn.commit()


def delete_movie(movie_id: int) -> None:
    sql = '''DELETE FROM Movie WHERE movieID = ?'''
    with closing(conn.cursor()) as c:
        c.execute(sql,  (movie_id,))
        conn.commit()
