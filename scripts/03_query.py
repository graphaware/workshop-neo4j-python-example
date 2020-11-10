import time
from neo4j import GraphDatabase
import sys
import csv


class ReadFromGraph(object):

    def __init__(self, uri, user, password, database):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)
        self.__database = database

    def close(self):
        self._driver.close()

    def query_and_print(self):
        query = """
            MATCH (movie:Movie)-[r:HAS_GENRE]-(genre)
            RETURN movie.title as title, collect(genre.genre) as genres
            LIMIT 10
        """

        with self._driver.session(database=self.__database) as session:
            for movie in session.run(query):
                movie_title = movie["title"]
                movie_genres = movie["genres"]
                print(movie_title, "-", movie_genres)

    def query_and_print_by_title(self, title):
        query = """
            MATCH (movie:Movie {title: $title})-[r:HAS_GENRE]-(genre)
            RETURN movie.title as title, collect(genre.genre) as genres
            LIMIT 10
        """

        with self._driver.session(database=self.__database) as session:
            for movie in session.run(query, {"title": title}):
                movie_title = movie["title"]
                movie_genres = movie["genres"]
                print(movie_title, "-", movie_genres)


def strip(string): return ''.join([c if 0 < ord(c) < 128 else ' ' for c in string])


def execute_no_exception(session, query):
    try:
        session.run(query)
    except Exception as e:
        pass


if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "pippo1"
    database = "neo4j"
    querying = ReadFromGraph(uri=uri, user=user, password=password, database=database)
    querying.query_and_print()
    print("----------------------")
    querying.query_and_print_by_title(title="Quantum of Solace (2008)")
    end = time.time() - start
    print("Time to complete:", end)