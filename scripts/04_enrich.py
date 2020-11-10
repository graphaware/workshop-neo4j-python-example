import time
from neo4j import GraphDatabase
import sys
import csv
from imdb import IMDb


class Enrich(object):

    def __init__(self, uri, user, password, database):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)
        self.__database = database
        self._ia = IMDb()

    def close(self):
        self._driver.close()

    def import_movie_details(self, file):
        print("Importing details of movies")
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self._driver.session(database=self.__database) as session:
                execute_no_exception(session, "CREATE CONSTRAINT ON (a:Person) ASSERT a.name IS UNIQUE;")
                tx = session.begin_transaction()
                i = 0;
                j = 0;
                for row in reader:
                    try:
                        if row:
                            movie_id = strip(row[0])
                            imdb_id = strip(row[1])
                            movie = self._ia.get_movie(imdb_id)
                            process_movie_info(movie_info=movie, tx=tx, movie_id=movie_id)
                            i += 1
                            j += 1

                        if i == 10:
                            tx.commit()
                            print(j, "movie details imported")
                            i = 0
                            tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row, reader.line_num)

                tx.commit()
                print(j, "lines processed")

def execute_no_exception(session, query):
    try:
        session.run(query)
    except Exception as e:
        pass

def process_movie_info(tx, movie_info, movie_id):
    query = """
    MATCH (movie:Movie {movieId: $movieId})
    SET movie.plot = $plot
    FOREACH (director IN $directors | MERGE (d:Person {name: director}) SET d:Director MERGE (d)-[:DIRECTED]->(movie))
    FOREACH (actor IN $actors | MERGE (d:Person {name: actor}) SET d:Actor MERGE (d)-[:ACTS_IN]->(movie))
    FOREACH (producer IN $producers | MERGE (d:Person {name: producer}) SET d:Producer MERGE (d)-[:PRODUCES]->(movie))
    FOREACH (writer IN $writers | MERGE (d:Person {name: writer}) SET d:Writer MERGE (d)-[:WRITES]->(movie))
    FOREACH (genre IN $genres | MERGE (g:Genre {genre: genre}) MERGE (movie)-[:HAS_GENRE]->(g))
    """
    directors = []
    for director in movie_info['directors']:
        if 'name' in director.data:
            directors.append(director['name'])

    genres = ''
    if 'genres' in movie_info:
        genres = movie_info['genres']

    actors = []
    for actor in movie_info['cast']:
        if 'name' in actor.data:
            actors.append(actor['name'])

    writers = []
    for writer in movie_info['writers']:
        if 'name' in writer.data:
            writers.append(writer['name'])

    producers = []
    for producer in movie_info['producers']:
        producers.append(producer['name'])

    plot = ''
    if 'plot outline' in movie_info:
        plot = movie_info['plot outline']

    tx.run(query, {"movieId": movie_id, "directors": directors, "genres": genres, "actors": actors, "plot": plot,
                   "writers": writers, "producers": producers})


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
    enriching = Enrich(uri=uri, user=user, password=password, database=database)
    enriching.import_movie_details(file=base_path + "/links.csv")
    end = time.time() - start
    print("Time to complete:", end)