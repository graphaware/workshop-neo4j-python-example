import time
from neo4j import GraphDatabase
import sys
import csv


class CSVImport(object):

    def __init__(self, uri, user, password, database):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)
        self.__database = database

    def close(self):
        self._driver.close()

    def import_movies(self, file):
        print("import movies")
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self._driver.session(database=self.__database) as session:
                execute_no_exception(session, "CREATE CONSTRAINT ON (a:Movie) ASSERT a.movieId IS UNIQUE; ")
                execute_no_exception(session, "CREATE CONSTRAINT ON (a:Genre) ASSERT a.genre IS UNIQUE; ")

                tx = session.begin_transaction()

                i = 0;
                j = 0;
                for row in reader:
                    try:
                        if row:
                            movie_id = strip(row[0])
                            title = strip(row[1])
                            genres = strip(row[2])
                            query = """
                            CREATE (movie:Movie {movieId: $movieId, title: $title})
                            with movie
                            UNWIND $genres as genre
                            MERGE (g:Genre {genre: genre})
                            MERGE (movie)-[:HAS_GENRE]->(g)
                            """
                            tx.run(query, {"movieId": movie_id, "title": title, "genres": genres.split("|")})
                            i += 1
                            j += 1

                        if i == 1000:
                            tx.commit()
                            print(j, "movies processed")
                            i = 0
                            tx = session.begin_transaction()

                    except Exception as e:
                        print(e, row, reader.line_num)

                tx.commit()
                print(j, "lines processed")


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
    base_path = "../data/ml-latest-small"
    if len(sys.argv) > 1:
        base_path = sys.argv[1]
    importing = CSVImport(uri=uri, user=user, password=password, database=database)
    importing.import_movies(file=base_path + "/movies.csv")
    end = time.time() - start
    print("Time to complete:", end)
    importing.close()