import sqlite3
from typing import Tuple
import csv

db_path = f'validator_v1.db'  # Set db path here


def get_cursor() -> Tuple[sqlite3.Cursor, sqlite3.Connection]:
    """
    Get a cursor and connection reference from the database
    Returns:
        cursor & connection objects
    """
    db_connection = get_db_connection()
    cursor = db_connection.cursor()
    return cursor, db_connection


def get_db_connection() -> sqlite3.Connection:
    """
    Get a reference to the database
    Returns:
        A database connection
    """
    return sqlite3.connect(db_path)


def query(query_str: str) -> list[tuple]:
    """
    Get all results of a query from the database
    Args:
        query_str: a query string

    Returns:
        All rows matching the query
    """
    rows = []
    cursor, db_connection = get_cursor()
    try:
        cursor.execute(query_str)
        rows = cursor.fetchall()
    finally:
        cursor.close()
        db_connection.close()
        return rows


def get_scored_predictions():
    query_str = """
            SELECT nextplace_id, miner_hotkey, prediction_timestamp, predicted_sale_price, predicted_sale_date
            FROM predictions
            WHERE scored=1
        """
    results = query(query_str)
    csv_data = []
    for result in results:
        entry = {
            'nextplaceId': result[0],
            'minerHotkey': result[1],
            'minerColdkey': None,
            'predictionDate': result[2],
            'predictedSalePrice': result[3],
            'predictedSaleDate': result[4]
        }
        csv_data.append(entry)

    csv_file_name = 'scored_predictions.csv'

    with open(csv_file_name, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=csv_data[0].keys())
        writer.writeheader()  # Write the headers
        writer.writerows(csv_data)  # Write the rows


def get_miner_scores():
    '''
    miner_hotkey TEXT PRIMARY KEY,
    lifetime_score REAL,
    total_predictions INTEGER,
    last_update_timestamp DATETIME
    '''
    query_str = """
            SELECT *
            FROM miner_scores
        """
    results = query(query_str)
    csv_data = []
    for result in results:
        entry = {
            'hotkey': result[0],
            'score': result[1],
            'predictions': result[2],
            'last_update': result[3],
        }
        csv_data.append(entry)

    csv_file_name = 'miner_scores.csv'

    with open(csv_file_name, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=csv_data[0].keys())
        writer.writeheader()  # Write the headers
        writer.writerows(csv_data)  # Write the rows


if __name__ == "__main__":
    get_scored_predictions()
    get_miner_scores()
