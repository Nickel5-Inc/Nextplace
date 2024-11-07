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
        miner_score = self._fetch_current_miner_score(miner_hotkey)
        new_scores = self._calculate_new_scores(scorable_predictions)
        if miner_score is not None:
            self._update_miner_score(miner_score, new_scores, miner_hotkey)
        else:
            self._handle_new_miner_score(miner_hotkey, new_scores)
        current_thread = threading.current_thread().name
        bt.logging.info(f"| {current_thread} | 🎯 Scored {len(scorable_predictions)} predictions for hotkey '{miner_hotkey}'")

    def _update_miner_score(self, miner_score: Dict[str, float], new_scores: Dict[str, float], miner_hotkey: str) -> None:
        """
        Update scores for a miner
        Args:
            miner_score: Miner's existing score data
            new_scores: Miner's new score data
            miner_hotkey: Miner's hotkey

        Returns:
            None
        """
        now = datetime.now(timezone.utc).strftime(ISO8601)
        old_score = miner_score['lifetime_score']
        old_predictions = miner_score['total_predictions']
        new_total_score = (old_score * old_predictions) + new_scores['total_score']
        new_total_predictions = old_predictions + new_scores['new_predictions']
        new_lifetime_score = new_total_score / new_total_predictions

        values = (new_lifetime_score, new_total_predictions, now)
        with self.database_manager.lock:
            self.database_manager.query_and_commit_with_values(f'''
                UPDATE miner_scores 
                SET lifetime_score = ?, total_predictions = ?, last_update_timestamp = ?
                WHERE miner_hotkey = '{miner_hotkey}'
            ''', values)

    def _handle_new_miner_score(self, miner_hotkey: str, new_scores: dict) -> None:
        """
        Add scores for a miner without any scores yet
        Args:
            miner_hotkey: Hotkey of the miner
            new_scores: Miner's new score data

        Returns:
            None
        """
        now = datetime.now(timezone.utc).strftime(ISO8601)
        lifetime_score = new_scores['total_score'] / new_scores['new_predictions']
        query_str = f"""
                        INSERT INTO miner_scores (miner_hotkey, lifetime_score, total_predictions, last_update_timestamp)
                        VALUES (?, ?, ?, ?)
                    """
        values = (miner_hotkey, lifetime_score, new_scores['new_predictions'], now)
        with self.database_manager.lock:
            self.database_manager.query_and_commit_with_values(query_str, values)

    def _fetch_current_miner_score(self, miner_hotkey: str) -> Dict[str, float] or None:
        """
        Retrieve scores for a miner
        Args:
            miner_hotkey: Hotkey of the miner

        Returns:
            Miners scores or None
        """
        current_thread = threading.current_thread().name
        query_str = f"""
            SELECT miner_hotkey, lifetime_score, total_predictions
            FROM miner_scores
            WHERE miner_hotkey = '{miner_hotkey}'
            LIMIT 1
        """
        with self.database_manager.lock:
            results = self.database_manager.query(query_str)
        if len(results) > 0:  # Update existing Miner score
            bt.logging.debug(f"| {current_thread} | 🦉 Found existing scores for miner with hotkey '{miner_hotkey}'")
            result = results[0]
            return {'lifetime_score': result[1], 'total_predictions': result[2]}
        else:  # No scores for this Miner yet
            bt.logging.debug(f"| {current_thread} | 🐦‍⬛ Found no existing scores for miner with hotkey '{miner_hotkey}'")
            return None

    def _get_num_sold_homes(self) -> int:

        current_thread = threading.current_thread().name
        num_sold_homes = self.database_manager.get_size_of_table('sales')
        bt.logging.info(f"| {current_thread} | 🥳 Received {num_sold_homes} sold homes")
        return num_sold_homes

    def _calculate_new_scores(self, scorable_predictions: List[Tuple]) -> Dict[str, float]:
        new_scores = {'total_score': 0, 'new_predictions': 0}

        for prediction in scorable_predictions:
            miner_hotkey, predicted_price, predicted_date, actual_price, actual_date = prediction
            score = self.calculate_score(actual_price, predicted_price, actual_date, predicted_date)

            if score is not None:
                new_scores['total_score'] += score
                new_scores['new_predictions'] += 1

        return new_scores

    def calculate_score(self, actual_price: str, predicted_price: str, actual_date: str, predicted_date: str):
        # Convert date strings to datetime objects
        actual_date = datetime.strptime(actual_date, ISO8601).date()

        try:
            predicted_date = datetime.strptime(predicted_date, "%Y-%m-%d").date()
        except ValueError:
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
