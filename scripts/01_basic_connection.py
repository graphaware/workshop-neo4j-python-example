import time
from neo4j import GraphDatabase
import sys


class BasicConnection(object):

    def __init__(self, uri, user, password, database):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)
        self.__database = database

    def close(self):
        self._driver.close()

    def test_connection(self):
        with self._driver.session(database=self.__database) as session:
            session.run("call dbms.procedures()")


if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "pippo1"
    database = "neo4j"
    connecting = BasicConnection(uri=uri, user=user, password=password, database=database)
    connecting.test_connection()
    end = time.time() - start
    print("Time to complete:", end)
    connecting.close()