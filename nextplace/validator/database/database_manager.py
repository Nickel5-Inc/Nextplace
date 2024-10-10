import sqlite3
from typing import Tuple
import os
from threading import RLock

"""
Helper class manager connections to the SQLite database
"""


class DatabaseManager:

    def __init__(self):
        data_dir = "data"
        db_version = 1
        os.makedirs(data_dir, exist_ok=True)  # Ensure data directory exists
        self.db_path = f'{data_dir}/validator_v{db_version}.db'  # Set db path
        db_dir = os.path.dirname(self.db_path)
        self.lock = RLock()  # Reentrant lock for thread safety
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)  # Create db dir
        self._check_and_migrate_ids()

    def _check_and_migrate_ids(self):
        ids_table_query_str = """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='ids'
        """
        ids_result = self.query(ids_table_query_str)
        ids_exists = ids_result is not None and len(ids_result) > 0

        predictions_table_query_str = """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='predictions'
        """
        predictions_result = self.query(predictions_table_query_str)
        predictions_exists = predictions_result is not None and len(predictions_result) > 0

        if predictions_exists and not ids_exists:
            self._migrate_prediction_ids()

    def _migrate_prediction_ids(self):
        distinct_ids_query = """
            SELECT DISTINCT(nextplace_id)
            FROM predictions
        """
        ids_results = self.query(distinct_ids_query)

        insert_query = """
            INSERT INTO ids (nextplace_id)
            VALUES(?)
        """
        values = [(result[0],) for result in ids_results]
        self.query_and_commit_many(insert_query, values)


    def query(self, query: str) -> list:
        """
        Get all results of a query from the database
        Args:
            query: a query string

        Returns:
            All rows matching the query
        """
        rows = []
        cursor, db_connection = self.get_cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        finally:
            cursor.close()
            db_connection.close()
            return rows

    def query_with_values(self, query: str, values: tuple) -> list:
        """
        Get all results of a query from the database
        Args:
            query: a query string
            values: a tuple of values

        Returns:
            All rows matching the query
        """
        rows = []
        cursor, db_connection = self.get_cursor()
        try:
            cursor.execute(query, values)
            rows = cursor.fetchall()
        finally:
            cursor.close()
            db_connection.close()
            return rows

    def query_and_commit(self, query: str) -> None:
        """
        Use for updating the database
        Args:
            query: query string

        Returns:
            None
        """
        cursor, db_connection = self.get_cursor()
        try:
            cursor.execute(query)
            db_connection.commit()
        finally:
            cursor.close()
            db_connection.close()

    def query_and_commit_with_values(self, query: str, values: tuple) -> None:
        """
        Use for updating the database
        Args:
            query: query string
            values: a tuple of values

        Returns:
            None
        """
        cursor, db_connection = self.get_cursor()
        try:
            cursor.execute(query, values)
            db_connection.commit()
        finally:
            cursor.close()
            db_connection.close()

    def query_and_commit_many(self, query: str, values: list[tuple]) -> None:
        """
        Use for updating the database with many rows at once
        Args:
            query: query string
            values: a list of tuples

        Returns:
            None
        """
        cursor, db_connection = self.get_cursor()
        try:
            cursor.executemany(query, values)
            db_connection.commit()
        finally:
            cursor.close()
            db_connection.close()

    def get_cursor(self) -> Tuple[sqlite3.Cursor, sqlite3.Connection]:
        """
        Get a cursor and connection reference from the database
        Returns:
            cursor & connection objects
        """
        db_connection = self.get_db_connection()
        cursor = db_connection.cursor()
        return cursor, db_connection

    def get_db_connection(self) -> sqlite3.Connection:
        """
        Get a reference to the database
        Returns:
            A database connection
        """
        return sqlite3.connect(self.db_path)

    def delete_all_sales(self) -> None:
        """
        Delete all rows from the sales table
        Returns:
            None
        """
        self.query_and_commit('DELETE FROM sales')

    def delete_all_properties(self) -> None:
        """
        Delete all rows from the sales table
        Returns:
            None
        """
        self.query_and_commit('DELETE FROM properties')

    def get_size_of_table(self, table_name: str):
        query_str = f"SELECT COUNT(*) FROM {table_name}"
        result = self.query(query_str)
        return result[0][0]
