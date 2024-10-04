import sqlite3
from typing import Tuple
import os
from threading import RLock
from datetime import datetime, timezone

"""
Helper class manager connections to the SQLite database
"""


class DatabaseManager:

    def __init__(self):
        os.makedirs('data', exist_ok=True)  # Ensure data directory exists
        self.db_path = f'data/miner.db'  # Set db path
        db_dir = os.path.dirname(self.db_path)
        self.lock = RLock()  # Reentrant lock for thread safety
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)  # Create db dir
        self.setup_table()

    def setup_table(self):
        now = datetime.now(timezone.utc)
        create_table_string = """
            CREATE TABLE IF NOT EXISTS synapse (
                id INTEGER PRIMARY KEY,
                timestamp DATETIME
            )
        """
        add_entry_query = f"""
            INSERT OR REPLACE INTO synapse (id, timestamp)
            VALUES (1, '{now}')
        """
        with self.lock:
            self.query_and_commit(create_table_string)
            self.query_and_commit(add_entry_query)

    def get_synapse_timestamp(self) -> str or None:
        """
        Get the timestamp from the synapse table

        Returns:
            The timestamp stored in the table, or None if no row exists
        """
        with self.lock:
            cursor, db_connection = self.get_cursor()
            try:
                cursor.execute("SELECT timestamp FROM synapse")
                row = cursor.fetchone()
                if row:
                    return row[0]  # Return the first (and only) column, which is the timestamp
                return None
            finally:
                cursor.close()
                db_connection.close()

    def update_synapse_timestamp(self) -> None:
        """
        Update the timestamp in the synapse table to current time. Call when the miner receives a synapse

        Returns:
            None
        """
        timestamp = datetime.now(timezone.utc)
        query_str = f"""
            UPDATE synapse
            SET timestamp = '{timestamp}'
            WHERE id = 1
        """
        with self.lock:
            self.query_and_commit(query_str)

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