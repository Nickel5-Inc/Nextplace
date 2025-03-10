import threading
from typing import Dict, List, Tuple
from datetime import datetime, timezone
import bittensor as bt
from nextplace.validator.utils.contants import ISO8601


class ScoringCalculator:

    def __init__(self, database_manager, sold_homes_api):
        self.database_manager = database_manager
        self.sold_homes_api = sold_homes_api

    def process_scorable_predictions(self, scorable_predictions: list, miner_hotkey: str) -> None:
        """
        Score miner predictions in bulk
        """
        current_thread = threading.current_thread().name
        new_scores = self._calculate_new_scores(scorable_predictions)
        if new_scores['new_predictions'] == 0:
            bt.logging.info(f"| {current_thread} | 📰 Miner '{miner_hotkey}' had only invalid scores, likely due to invalid date formatting.")
            return

        self._add_to_daily_scores(new_scores, miner_hotkey)
        bt.logging.info(f"| {current_thread} | 🎯 Scored {len(scorable_predictions)} predictions for hotkey '{miner_hotkey}'")

    def _add_to_daily_scores(self, new_scores: dict, miner_hotkey: str) -> None:
        """
        Add new scores to daily_scores table.
        Args:
            new_scores: dict of new scores.
            miner_hotkey: miner's hotkey.

        Returns:
            None
        """
        current_thread = threading.current_thread().name
        today = datetime.now(timezone.utc).date()
        bt.logging.info(f"| {current_thread} | 📅 Updating daily_scores for {today}")

        existing_daily_score_query = """
            SELECT score, total_predictions 
            FROM daily_scores 
            WHERE miner_hotkey = ? AND date = ?
        """
        values = (miner_hotkey, today)

        with self.database_manager.lock:
            results = self.database_manager.query_with_values(existing_daily_score_query, values)

        if results and len(results) > 0:  # Update existing Miner score
            result = results[0]
            old_score = result[0]
            old_predictions = result[1]
            new_total_score = (old_score * old_predictions) + new_scores['total_score']
            new_total_predictions = old_predictions + new_scores['new_predictions']
            new_daily_score = new_total_score / new_total_predictions

            update_query = """
                UPDATE daily_scores 
                SET score = ?, total_predictions = ? 
                WHERE miner_hotkey = ? AND date = ?
            """
            update_values = (new_daily_score, new_total_predictions, miner_hotkey, today)

            with self.database_manager.lock:
                self.database_manager.query_and_commit_with_values(update_query, update_values)

            bt.logging.info(f"| {current_thread} | ⭐ Updated daily score. Score: {new_daily_score}, Total Scored: {new_total_predictions}")

        else:  # No scores for this Miner yet
            score = new_scores['total_score'] / new_scores['new_predictions']
            insert_query = """
                INSERT INTO daily_scores (miner_hotkey, date, score, total_predictions) 
                VALUES (?, ?, ?, ?)
            """
            insert_values = (miner_hotkey, today, score, new_scores['new_predictions'])

            with self.database_manager.lock:
                self.database_manager.query_and_commit_with_values(insert_query, insert_values)
            bt.logging.info(f"| {current_thread} | ⭐ Added daily score. Score: {score}, Total Scored: {new_scores['new_predictions']}")

    def _get_num_sold_homes(self) -> int:

        current_thread = threading.current_thread().name
        num_sold_homes = self.database_manager.get_size_of_table('sales')
        bt.logging.info(f"| {current_thread} | 🥳 Received {num_sold_homes} sold homes")
        return num_sold_homes

    def _calculate_new_scores(self, scorable_predictions: List[Tuple]) -> Dict[str, float]:
        new_scores = {'total_score': 0, 'new_predictions': 0}

        for prediction in scorable_predictions:
            miner_hotkey, predicted_price, predicted_date, actual_price, actual_date = prediction
            score = self.calculate_score(actual_price, predicted_price, actual_date, predicted_date, miner_hotkey)

            if score is not None:
                new_scores['total_score'] += score
                new_scores['new_predictions'] += 1

        return new_scores

    def calculate_score(self, actual_price: str, predicted_price: str, actual_date: str, predicted_date: str, miner_hotkey: str):
        # Convert date strings to datetime objects
        actual_date = datetime.strptime(actual_date, ISO8601).date()
        current_thread = threading.current_thread().name

        try:
            predicted_date = datetime.strptime(predicted_date, "%Y-%m-%d").date()
        except ValueError:
            bt.logging.info(f"| {current_thread} | Received invalid date format from '{miner_hotkey}'. Ignoring prediction.")
            return None

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
