import threading
from datetime import datetime, timedelta
import bittensor as bt
from nextplace.validator.database.database_manager import DatabaseManager

"""
Use this class when calculating weights
"""


class TimeGatedScorer:
    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager
        self.score_date_cutoff = 28
        self.consistency_window_duration = 5

    def score(self, miner_hotkey: str) -> float:
        """
        Score the miner
        :param miner_hotkey: hotkey for the miner being scored
        Returns:
            The score for the miner
        """
        # ToDo Handle the case where there are no daily_scores for this hotkey
        current_thread = threading.current_thread().name
        consistency_window_score = self._get_consistency_window_score(miner_hotkey)
        bt.logging.debug(f"| {current_thread} | 🪲 Found consistency window score: {consistency_window_score}")
        return 1.0

    def _get_consistency_window_percent(self, miner_hotkey: str) -> float:
        """
        Get the consistency window percentage
        Args:
            miner_hotkey: hotkey for the miner being scored

        Returns:
            Percentage [0.0, 100.0]
        """
        current_thread = threading.current_thread().name
        oldest_prediction_date = self._get_oldest_prediction_date(miner_hotkey)
        if oldest_prediction_date is None:
            return 100.0
        today = datetime.today().date()
        difference = today - oldest_prediction_date
        bt.logging.debug(f"| {current_thread} | 🪲 Found difference between today and oldest prediction: {difference}")
        if difference < self.consistency_window_duration:
            return 100.0
        # ToDo Scale the return between 51.0 and 100.0 where self.consistency_window_duration -> 100.0 and self.score_date_cutoff = 28 -> 51.0


    def _get_oldest_prediction_date(self, miner_hotkey: str) -> datetime.date or None:
        """
        Get the date of the oldest prediction
        Args:
            miner_hotkey: hotkey for the miner being scored

        Returns:
            Date object representing the date of the oldest prediction
        """
        query_string = "SELECT date FROM active_miners WHERE miner_hotkey = ? ORDER BY date DESC LIMIT 1"
        values = (miner_hotkey, )
        with self.database_manager.lock:
            results = self.database_manager.query_with_values(query_string, values)
        if results is None or len(results) == 0:
            return None
        return results[0][0]

    def _get_consistency_window_score(self, miner_hotkey: str) -> float:
        """
        Retrieve the past scores for the miner
        :param miner_hotkey: hotkey for the miner being scored
        Returns:
            list of relevant historic scores for the miner
        """
        current_thread = threading.current_thread().name
        consistency_window_cutoff = self._get_consistency_window_start_date()
        query_string = "SELECT score, total_predictions FROM daily_scores WHERE miner_hotkey = ? AND date <= ?"
        values = (miner_hotkey, consistency_window_cutoff)
        with self.database_manager.lock:
            results = self.database_manager.query_with_values(query_string, values)
        if results is not None and len(results) > 0:
            bt.logging.debug(f"| {current_thread} | 🪲 Found consistency window cutoff '{consistency_window_cutoff}'")
            total_predictions = 0
            total_score = 0
            for result in results:
                total_score += result[0]
                total_predictions += result[1]
            return total_score / total_predictions

        bt.logging.debug(f"| {current_thread} | 🪲 Found no consistency window predictions for hotkey '{miner_hotkey}'")
        return 0.0

    def _get_non_consistency_window_score(self, miner_hotkey: str) -> float:
        current_thread = threading.current_thread().name
        past_scores = self._get_past_scores(miner_hotkey)
        if len(past_scores) == 0:
            bt.logging.debug(f"| {current_thread} | 🪲 Found no non-consistency window predictions for hotkey '{miner_hotkey}'")
            return 0.0

    def _get_past_scores(self, miner_hotkey: str) -> list:
        """
        Retrieve the past scores for the miner
        :param miner_hotkey: hotkey for the miner being scored
        Returns:
            list of relevant historic scores for the miner
        """
        current_thread = threading.current_thread().name
        date_cutoff = self._get_score_cutoff_date()
        consistency_window_cutoff = self._get_consistency_window_start_date()
        query_string = "SELECT date, score, total_predictions FROM daily_scores WHERE miner_hotkey = ? AND date <= ? AND date > ?"
        values = (miner_hotkey, date_cutoff, consistency_window_cutoff)
        with self.database_manager.lock:
            results = self.database_manager.query_with_values(query_string, values)
        if results is not None and len(results) > 0:
            bt.logging.debug(f"| {current_thread} | 🪲 Found date cutoff '{date_cutoff}'")
            return results

        bt.logging.debug(f"| {current_thread} | 🪲 Found no historic predictions for hotkey '{miner_hotkey}'")
        return []

    def _get_consistency_window_start_date(self) -> datetime.date:
        current_thread = threading.current_thread().name
        today = datetime.today().date()
        date_cutoff = today - timedelta(days=int(self.consistency_window_duration))
        bt.logging.debug(f"| {current_thread} | 🪲 Found consistency window cutoff '{date_cutoff}'")
        return date_cutoff

    def _get_score_cutoff_date(self) -> datetime.date:
        current_thread = threading.current_thread().name
        today = datetime.today().date()
        date_cutoff = today - timedelta(days=int(self.score_date_cutoff))
        bt.logging.debug(f"| {current_thread} | 🪲 Found date cutoff '{date_cutoff}'")
        return date_cutoff
