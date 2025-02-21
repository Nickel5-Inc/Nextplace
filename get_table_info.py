import sqlite3
from typing import Tuple
import csv

db_path = f'data/validator_v1.db'


def get_cursor() -> Tuple[sqlite3.Cursor, sqlite3.Connection]:
    db_connection = get_db_connection()
    cursor = db_connection.cursor()
    return cursor, db_connection


def get_db_connection() -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def query(query_str: str) -> list[tuple]:
    rows = []
    cursor, db_connection = get_cursor()
    try:
        cursor.execute(query_str)
        rows = cursor.fetchall()
    finally:
        cursor.close()
        db_connection.close()
        return rows


def get_sizes_of_predictions_tables() -> dict[str, int]:
    predictions_tables_query = query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'predictions_%'")
    predictions_tables = [table[0][len("predictions_"):] for table in predictions_tables_query]
    tables_and_sizes = {}
    for table in predictions_tables:
        size = query(f"SELECT COUNT(*) FROM {table}")
        if size and len(size) > 0:
            tables_and_sizes[table] = size[0][0]
    return tables_and_sizes


def write_to_csv(tables_and_sizes: dict, formatted_scores: list):
    with open("output.csv", mode="w", newline="") as file:
        writer = csv.writer(file)

        # Write tables_and_sizes
        writer.writerow(["Table Name", "Size"])
        for table, size in tables_and_sizes.items():
            writer.writerow([table, size])

        writer.writerow([])  # Empty row for separation

        # Write formatted_scores
        writer.writerow(["Hotkey", "Date", "Score", "Total Predictions"])
        for row in formatted_scores:
            writer.writerow([row['hotkey'], row['date'], row['score'], row['total_predictions']])


def main():
    tables_and_sizes = get_sizes_of_predictions_tables()
    scores = query("SELECT miner_hotkey, date, score, total_predictions FROM daily_scores")
    formatted_scores = [{'hotkey': x[0], 'date': x[1], 'score': x[2], 'total_predictions': x[3]} for x in scores]
    write_to_csv(tables_and_sizes, formatted_scores)


if __name__ == '__main__':
    main()
