import sqlite3
import threading
from typing import Dict, List, Tuple
from collections import defaultdict
from datetime import datetime, timezone
import bittensor as bt
from nextplace.validator.utils.contants import ISO8601


class ScoringCalculator:

    def __init__(self, database_manager, sold_homes_api):
        self.database_manager = database_manager
        self.sold_homes_api = sold_homes_api
        self.current_thread = threading.current_thread().name
        
    def process_scorable_predictions(self, scorable_predictions: list, miner_hotkey: str) -> None:
        """
        Score miner predictions in bulk
        """
        cursor, db_connection = self.database_manager.get_cursor()
        try:
            miner_scores = self._fetch_current_miner_score(cursor, miner_hotkey)
            new_scores = self._calculate_new_scores(scorable_predictions)
            self._update_miner_scores(cursor, miner_scores, new_scores)
            db_connection.commit()
            bt.logging.info(f"| {self.current_thread} | 🎯 Scored {len(scorable_predictions)} predictions for hotkey {miner_hotkey}")
        finally:
            cursor.close()
            db_connection.close()

    def _fetch_current_miner_score(self, cursor: sqlite3.Cursor, miner_hotkey: str) -> Dict[str, Dict[str, float]]:
        query_str = f"""
            SELECT miner_hotkey, lifetime_score, total_predictions
            WHERE miner_hotkey='{miner_hotkey}'
            FROM miner_scores
        """
        cursor.execute(query_str)
        return {row[0]: {'lifetime_score': row[1], 'total_predictions': row[2]} for row in cursor.fetchall()}

    def _get_num_sold_homes(self) -> int:
        num_sold_homes = self.database_manager.get_size_of_table('sales')
        bt.logging.info(f"| {self.current_thread} | 🥳 Received {num_sold_homes} sold homes")
        return num_sold_homes

    def _calculate_new_scores(self, scorable_predictions: List[Tuple]) -> Dict[str, Dict[str, float]]:
        new_scores = defaultdict(lambda: {'total_score': 0, 'new_predictions': 0})

        for prediction in scorable_predictions:
            miner_hotkey, predicted_price, predicted_date, actual_price, actual_date = prediction
            score = self.calculate_score(actual_price, predicted_price, actual_date, predicted_date)

            new_scores[miner_hotkey]['total_score'] += score
            new_scores[miner_hotkey]['new_predictions'] += 1

        return new_scores

    def _update_miner_scores(self, cursor, miner_scores: Dict[str, Dict[str, float]], new_scores: Dict[str, Dict[str, float]]) -> None:
        updates = []
        inserts = []
        for miner_hotkey, data in new_scores.items():
            if miner_hotkey in miner_scores:
                old_score = miner_scores[miner_hotkey]['lifetime_score']
                old_predictions = miner_scores[miner_hotkey]['total_predictions']
                new_total_score = (old_score * old_predictions) + data['total_score']
                new_total_predictions = old_predictions + data['new_predictions']
                new_lifetime_score = new_total_score / new_total_predictions
                updates.append((new_lifetime_score, new_total_predictions, miner_hotkey))
            else:
                new_lifetime_score = data['total_score'] / data['new_predictions']
                inserts.append((miner_hotkey, new_lifetime_score, data['new_predictions']))

        now = datetime.now(timezone.utc).strftime(ISO8601)
        cursor.executemany(f'''
            UPDATE miner_scores 
            SET lifetime_score = ?, total_predictions = ?, last_update_timestamp = '{now}'
            WHERE miner_hotkey = ?
        ''', updates)

        cursor.executemany(f'''
            INSERT INTO miner_scores (miner_hotkey, lifetime_score, total_predictions, last_update_timestamp)
            VALUES (?, ?, ?, '{now}')
        ''', inserts)

    def calculate_score(self, actual_price: str, predicted_price: str, actual_date: str, predicted_date: str):
        # Convert date strings to datetime objects
        actual_date = datetime.strptime(actual_date, ISO8601).date()
        predicted_date = datetime.strptime(predicted_date, "%Y-%m-%d").date()

        # Calculate the absolute difference in days
        date_difference = abs((actual_date - predicted_date).days)

        # Score based on date accuracy (14 points max, 1 point deducted per day off)
        date_score = (max(0, 14 - date_difference) / 14) * 100

        # Calculate price accuracy
        price_difference = abs(float(actual_price) - float(predicted_price)) / float(actual_price)
        price_score = max(0, 100 - (price_difference * 100))

        # Combine scores (86% weight to price, 14% weight to date)
        final_score = (price_score * 0.86) + (date_score * 0.14)

        return final_score
