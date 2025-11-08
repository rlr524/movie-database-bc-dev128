from typing import List, Optional
import db
from connect_db import Database
from objects import Movie, Category


def display_welcome() -> None:
    print("The Movie List Program")
    print()
    display_menu()


def display_menu() -> None:
    print("COMMAND MENU")
    print("cat  - View movies by category")
    print("year - View movies by year")
    print("add  - Add a movie")
    print("del  - Delete a movie")
    print("exit - Exit the program")
    print()


def display_categories() -> None:
    print("CATEGORIES")
    categories: List[Category] = db.get_categories()
    for category in categories:
        print(f"{category.id}. {category.name}")
    print()


def display_movies(movies, title_term) -> None:
    print(f"MOVIES - {title_term}")
    print(f"{'ID':4}{'Name':40}{'Year':6}{'Mins':6}{'Category':10}")
    print("-" * 65)
    for movie in movies:
        print(f"{movie.id:<4d}{movie.name:40}{movie.year:<6d}{movie.minutes:<6d}{movie.category.name:10}")
    print()


def get_int(prompt) -> Optional[int]:
    while True:
        try:
            return int(input(prompt))
        except ValueError:
            print("Invalid whole number. Please try again.\n")


def display_movies_by_category() -> None:
    category_id = get_int("Category ID: ")
    category = db.get_category(category_id)

    if category is None:
        print("There is no category with that ID.\n")
    else:
        print()
        movies = db.get_movies_by_category(category_id)
        display_movies(movies, category.name.upper())


def display_movies_by_year() -> None:
    year = get_int("Year: ")
    print()
    movies = db.get_movies_by_year(year)
    display_movies(movies, year)


def add_movie() -> None:
    name        = input("Name: ")
    year        = get_int("Year: ")
    minutes     = get_int("Minutes: ")
    category_id = get_int("Category ID: ")

    category = db.get_category(category_id)
    if category is None:
        print("There is no category with that ID. Movie not added.\n")
    else:
        movie = Movie(name=name, year=year, minutes=minutes, category=category)
        db.add_movie(movie)
        print(f"{name} was added to the database.\n")


def delete_movie() -> None:
    movie_id = get_int("Movie ID: ")
    db.delete_movie(movie_id)
    print(f"Movie ID {movie_id} was deleted from the database.\n")


def main():
    database = Database()
    database.connect()
    display_welcome()
    display_categories()
    while True:
        command = input("Command: ").lower()
        if command == "cat":
            display_movies_by_category()
        elif command == "year":
            display_movies_by_year()
        elif command == "add":
            add_movie()
        elif command == "del":
            delete_movie()
        elif command == "exit":
            break
        else:
            print("Not a valid command, please try again.\n")
    database.close()
    print("Thank you for visiting!")


if __name__ == "__main__":
    main()


