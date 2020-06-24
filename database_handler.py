import json
import psycopg2
import psycopg2.extras
import pandas as pd


def get_database_access(path):
    """
    Reads a file.

    Expected dict format:

    {
        "database_name": {
            "host": "database-db.host.net",
            "user": "user",
            "password": "1234",
            "database": "database_name",
            "port": 5432
        },
    }

    Parameters
    ----------
    :path: recieves access_information.json path

    Returns
    ----------
    A dictionary with databases access information
    """
    database_file_name = path
    with open(database_file_name, "r") as database_file:
        database_access = json.load(database_file)
    return database_access


class DatabaseHandler:
    """
    Handler for execute queries in a given the database
    """

    def __init__(self, access_information):
        """Class atributes

        Parameters
        ----------
        :access_information: databases access dictionary
        {
            "database_name": {
                "host": "database-db.host.net",
                "user": "user",
                "password": "1234",
                "database": "database_name",
                "port": 5432
            },
        }
        """
        self._host = access_information["host"]
        self._port = access_information.get("port", 5432)
        self._user = access_information["user"]
        self._password = access_information["password"]
        self._database = access_information["database"]
        self._connection = self._connect()

    @property
    def connection(self):
        """
        Connection attribute
        """
        return self._connection

    def _connect(self):
        """Establish connection with the database
        """
        connection_parameters = {
            "host": self._host,
            "port": self._port,
            "dbname": self._database,
            "user": self._user,
            "password": self._password
        }
        return psycopg2.connect(
            **connection_parameters, connect_timeout=10)

    def _reconnect(self):
        """Reconnect to database, if connection is closed
        """
        if self._connection.closed > 0:
            self._connection = self._connect()

    def close(self):
        """Close the connection
        """
        self._connection.close()

    def cursor(self):
        """Create cursors
        """
        return self.connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor)

    def db_connector(func):
        """
        Check the connection before making query,
        connect if disconnected

        Parameters
        ----------
        :func: Database related function which uses
        DatabaseHandler connection
        """
        def with_connection(self, *args, **kwargs):
            self._reconnect()
            try:
                result = func(self, *args, **kwargs)
            except Exception as error:
                print(f"Error: {error}")

            return result
        return with_connection

    @db_connector
    def fetch(self, query, params=None, max_tries=5):
        """
        Fetch query results

        Parameters
        ----------
        :query: Database related function which uses
        DatabaseHandler connection
        :params: Query params
        :max_tries: Max number of query retries

        Returns
        ----------
        Query results as a list of dicts
        """
        attempt_no = 0
        while attempt_no < max_tries:
            attempt_no += 1
            cursor = self.cursor()
            try:
                with self.connection:
                    with cursor:
                        cursor.execute(query, params)
                        return cursor.fetchall()
            except Exception as error:
                print(f"ERROR: In psycopg.cursor.fetchall(): {error}")
        return []

    @db_connector
    def query_to_df(self, sql, params=None, max_tries=5):
        """
        Create a pandas DataFrame object from a query result

        Parameters
        ----------
        :sql: query statements
        :params: a list or a tuple of parameters that will
        be passed to the query execution
        :max_tries: number of query retries in the case of failure

        Returns
        ----------
        Pandas DataFrame object
        """
        attempt_no = 0
        while attempt_no < max_tries:
            cursor = self.cursor()
            attempt_no += 1
            try:
                with self.connection:
                    with cursor:
                        return pd.read_sql_query(sql, self, params=params)
            except Exception as error:
                print(f"Query to DataFrame error: {error}.")


def load_query(path) -> str:
    """
    Load query from .sql file

    Parameters
    ----------
    :query: file.sql path

    Returns
    ----------
    String content of query file
    """
    with open(path, "r") as query_file:
        return query_file.read()
