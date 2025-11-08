# python
"""
db.py
11/7/2025
Rob Ranf - rob.ranf@bellevuecollege.edu

Database access helpers for Movie/Category objects.

This module provides simple functions to construct domain objects from
database rows and perform common queries (list/get/add/delete). The
functions expect DB rows to be mapping-like objects (e.g. sqlite3.Row)
with specific keys described in each function's docstring.

Note: SQL column aliases used in queries must match the keys documented
below (for example: 'movieID', 'name', 'year', 'minutes', 'categoryID',
'categoryName').
"""
import logging
from typing import Any, List, Optional
from connect_db import Database
from contextlib import closing

from objects import Category
from objects import Movie

db = Database()
conn = db.connect()


def make_category(row: Any) -> Category:
    """
    Create a Category object from a database row.

    Args:
        row: Mapping-like DB row that must provide:
            - 'categoryID' (int)
            - 'categoryName' (str)

    Returns:
        Category: constructed Category instance.
    """
    return Category(row["categoryID"], row["categoryName"])


def make_movie(row: Any) -> Movie:
    """
    Create a Movie object from a database row.

    Args:
        row: Mapping-like DB row that must provide:
            - 'movieID' (int)
            - 'name' (str)
            - 'year' (int)
            - 'minutes' (int)
            - 'categoryID' (int)
            - 'categoryName' (str)

    Returns:
        Movie: constructed Movie instance. The category is created by
        calling :func:`make_category` with the same row.
    """
    return Movie(row["movieID"], row["name"], row["year"], row["minutes"], make_category(row))


def make_movie_list(results) -> List[Movie]:
    """
    Convert an iterable of DB rows into a list of Movie objects.

    Args:
        results: iterable of mapping-like DB rows. Each row must contain
                 the keys required by :func:`make_movie`.

    Returns:
        List[Movie]: list of Movie objects.
    """
    movies: List[Movie] = []
    for row in results:
        movies.append(make_movie(row))
    return movies


def get_categories() -> List[Category]:
    """
    Retrieve all categories from the database.

    Returns:
        List[Category]: list of Category objects.

    SQL behavior:
        Expects the query to return columns named 'categoryID' and
        'categoryName' (alias as needed).
    """
    query = '''SELECT categoryID, name as categoryName FROM Category'''
    with closing(conn.cursor()) as c:
        c.execute(query)
        results = c.fetchall()

    categories: List[Category] = []
    for row in results:
        categories.append(make_category(row))
    return categories


def get_category(category_id: int) -> Optional[Category]:
    """
    Retrieve a single category by ID.

    Args:
        category_id: ID of the category to fetch.

    Returns:
        Optional[Category]: Category object if found, otherwise None.

    SQL behavior:
        Query must return 'categoryID' and 'categoryName'.
    """
    query = '''SELECT categoryID, name as categoryName from Category WHERE categoryID = ?'''
    with closing(conn.cursor()) as c:
        c.execute(query, (category_id,))
        row: Any = c.fetchone()
        if row:
            return make_category(row)
        else:
            return None





def get_movies_by_category(category_id: int) -> List[Movie]:
    """
    Retrieve movies that belong to a specific category.

    Args:
        category_id: ID of the category to filter by.

    Returns:
        List[Movie]: list of Movie objects.

    SQL behavior:
        Query should return columns named:
            - 'movieID', 'name', 'year', 'minutes'
            - plus 'categoryID' and 'categoryName' for the category part.
        Ensure column aliases in the SQL match these keys.
    """
    query = '''SELECT m.movieID, m.categoryID, c.name AS categoryName, m.name, m.year, m.minutes
               FROM Movie m JOIN Category c on c.categoryID = m.categoryID 
               WHERE m.categoryID = ?'''
    with closing(conn.cursor()) as c:
        c.execute(query, (category_id,))
        results = c.fetchall()

    return make_movie_list(results)


def get_movies_by_year(year: int) -> List[Movie]:
    """
    Retrieve movies released in a given year.

    Args:
        year: release year to filter by.

    Returns:
        List[Movie]: list of Movie objects.

    SQL behavior:
        Query should return columns named:
            - 'movieID', 'name', 'year', 'minutes'
            - plus 'categoryID' and 'categoryName' for the category part.
        Ensure column aliases in the SQL match these keys.
    """
    query = '''SELECT movieID, m.categoryID, c.name AS categoryName, m.name, m.year, m.minutes 
               FROM Movie m JOIN Category c on c.categoryID = m.categoryID
               WHERE year = ?'''
    with closing(conn.cursor()) as c:
        c.execute(query, (year,))
        results = c.fetchall()

    return make_movie_list(results)


def add_movie(movie: Movie) -> None:
    """
    Insert a new movie into the database.

    Args:
        movie: Movie object to insert. Required attributes:
            - movie.category.id
            - movie.name
            - movie.year
            - movie.minutes

    Side effects:
        Commits the transaction on success.

    Raises:
        SQLite3 DatabaseError if insertion fails.

    SQL behavior:
        INSERT statement must use valid SQL for the target DB; current
        SQL must not include an alias after the table name.
    """
    if not getattr(movie, "category", None) or getattr(movie.category, "id", None) is None:
        raise ValueError("movie.category.id is required")
    if not getattr(movie, "name", None):
        raise ValueError("movie.name is required")
    if not getattr(movie, "year", None):
        raise ValueError("movie.year is required")
    if not getattr(movie, "minutes", None):
        raise ValueError("movie.minutes is required")

    sql = '''INSERT INTO Movie (categoryID, name, year, minutes) VALUES (?, ?, ?, ?)'''
    try:
        with closing(conn.cursor()) as c:
            c.execute(sql, (movie.category.id, movie.name, movie.year, movie.minutes))
        conn.commit()
    except conn.DatabaseError as e:
        try:
            conn.rollback()
        except conn.OperationalError:
            logging.exception("Failed to roll back transaction after insert failure")
        logging.exception(f"Failed to insert movie {e}")
        raise


def delete_movie(movie_id: int) -> None:
    """
    Delete a movie by ID.

    Args:
        movie_id: ID of the movie to delete.

    Side effects:
        Commits the transaction on success.

    Raises:
        SQLite3 DatabaseError if deletion fails.
    """
    sql = '''DELETE FROM Movie WHERE movieID = ?'''
    try:
        with closing(conn.cursor()) as c:
            c.execute(sql,  (movie_id,))
            conn.commit()
    except conn.DatabaseError as e:
        try:
            conn.rollback()
        except conn.OperationalError:
            logging.exception("Failed to roll back delete transaction after delete failure")
        logging.exception(f"Failed to delete movie {e}")
        raise

