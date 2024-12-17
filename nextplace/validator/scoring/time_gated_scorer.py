import statistics
import threading
from datetime import datetime, timedelta, timezone
import bittensor as bt
from nextplace.validator.database.database_manager import DatabaseManager

"""
Use this class when calculating weights
"""
TABLE_NAME = "daily_scores"


class TimeGatedScorer:
    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager
        self.score_date_cutoff = 21
        self.consistency_window_duration = 5
        self.min_consistency_window_percent = 51.0

    def score(self, miner_hotkey: str) -> float:
        """
        Score the miner
        :param miner_hotkey: hotkey for the miner being scored
        Returns:
            The score for the miner
        """
        # ToDo Handle the case where there are no daily_scores for this hotkey
        current_thread = threading.current_thread().name
        bt.logging.debug(f"| {current_thread} | 🪲 Scoring miner '{miner_hotkey}'")

        oldest_prediction_date = self._get_oldest_prediction_date(miner_hotkey)

        consistency_window_percent = self._get_consistency_window_percent(oldest_prediction_date)
        bt.logging.debug(f"| {current_thread} | 🪲 🪟📊 Found consistency window percent: {consistency_window_percent}")

        consistency_window_score = self._get_consistency_window_score(miner_hotkey)
        bt.logging.debug(f"| {current_thread} | 🪲 🪟💎 Found consistency window score: {consistency_window_score}")

        size_of_non_consistency_window = self.get_size_of_non_consistency_window(oldest_prediction_date)
        non_consistency_window_score = self._get_non_consistency_window_score(miner_hotkey, size_of_non_consistency_window)

        bt.logging.debug(f"| {current_thread} | 🪲 🪟🎯 Found non-consistency window score: {non_consistency_window_score}")
        non_consistency_window_percent = 100.0 - consistency_window_percent
        calculated_score = ((consistency_window_score * consistency_window_percent) / 100) + ((non_consistency_window_score * non_consistency_window_percent) / 100)
        bt.logging.debug(f"| {current_thread} | 🪲 🏆 Calculated score: {calculated_score}")

        return (consistency_window_score * consistency_window_percent) + (non_consistency_window_score * non_consistency_window_percent)

    def get_size_of_non_consistency_window(self, oldest_prediction_date: datetime.date or None) -> int:
        if oldest_prediction_date is None:
            return 0
        today = datetime.now(timezone.utc).date()
        difference = (today - oldest_prediction_date).days
        if difference <= self.consistency_window_duration:
            return 0
        return min((self.score_date_cutoff - self.consistency_window_duration), (difference - self.consistency_window_duration))


    def _get_consistency_window_percent(self, oldest_prediction_date: datetime.date or None) -> float:
        """
        Get the consistency window percentage
        Args:
            miner_hotkey: hotkey for the miner being scored

        Returns:
            Percentage [0.0, 100.0]
        """
        current_thread = threading.current_thread().name
        max_consistency_window_percent = 100.0
        bt.logging.debug(f"| {current_thread} | 🪲 👵🏻 Found oldest daily_score prediction: {oldest_prediction_date}")
        if oldest_prediction_date is None:
            return max_consistency_window_percent
        today = datetime.now(timezone.utc).date()
        difference = (today - oldest_prediction_date).days
        bt.logging.debug(f"| {current_thread} | 🪲 ⩇⩇:⩇⩇ Found difference between today and oldest prediction: {difference}")
        if difference <= self.consistency_window_duration:
            return max_consistency_window_percent
        if difference >= self.score_date_cutoff:
            return self.min_consistency_window_percent
        return max_consistency_window_percent + ((self.min_consistency_window_percent - max_consistency_window_percent) * (difference - self.consistency_window_duration)) / (self.score_date_cutoff - self.consistency_window_duration)

    def _get_oldest_prediction_date(self, miner_hotkey: str) -> datetime.date or None:
        """
        Get the date of the oldest prediction
        Args:
            miner_hotkey: hotkey for the miner being scored

        Returns:
            Date object representing the date of the oldest prediction
        """
        query_string = f"SELECT date FROM {TABLE_NAME} WHERE miner_hotkey = ? ORDER BY date LIMIT 1"
        values = (miner_hotkey, )
        with self.database_manager.lock:
            results = self.database_manager.query_with_values(query_string, values)

        if results is None or len(results) == 0:
            return None
        result = results[0][0]
        return datetime.strptime(result, "%Y-%m-%d").date()

    def _get_consistency_window_score(self, miner_hotkey: str) -> float:
        """
        Retrieve the past scores for the miner
        :param miner_hotkey: hotkey for the miner being scored
        Returns:
            list of relevant historic scores for the miner
        """
        current_thread = threading.current_thread().name
        consistency_window_cutoff = self._get_consistency_window_start_date()
        query_string = f"SELECT score, total_predictions FROM {TABLE_NAME} WHERE miner_hotkey = ? AND date >= ?"
        values = (miner_hotkey, consistency_window_cutoff)
        with self.database_manager.lock:
            results = self.database_manager.query_with_values(query_string, values)
        if results is not None and len(results) > 0:
            bt.logging.debug(f"| {current_thread} | 🪲 🪟🔪 Found consistency window cutoff '{consistency_window_cutoff}'")
            total_predictions = 0
            total_score = 0
            for result in results:
                total_score += (result[0] * result[1])
                total_predictions += result[1]
            score = total_score / total_predictions
            score_scalar = self._get_score_scalar(total_predictions)
            return score * score_scalar

        bt.logging.debug(f"| {current_thread} | 🪲 🪟🚫 Found no consistency window predictions for hotkey '{miner_hotkey}'")
        return 0.0

    def _get_score_scalar(self, prediction_volume: int) -> float:
        """
        Get the scalar for the score based on the prediction volume
        Args:
            prediction_volume: number of predictions

        Returns:
            The scalar, a float
        """
        if prediction_volume < 5:
            return 0.7
        elif prediction_volume < 10:
            return 0.725
        elif prediction_volume < 15:
            return 0.75
        elif prediction_volume < 20:
            return 0.8
        elif prediction_volume < 25:
            return 0.85
        else:
            return 1

    def _get_non_consistency_window_score(self, miner_hotkey: str, size_of_non_consistency_window: int) -> float:
        """
        Get the score for the non consistency window
        Args:
            miner_hotkey: this miner's hotkey
            size_of_non_consistency_window: number of days in the non-consistency window

        Returns:
            the score for the non-consistency window
        """
        current_thread = threading.current_thread().name
        past_scores = self._get_past_scores(miner_hotkey)
        if len(past_scores) == 0:
            print(f"| {current_thread} | 🪲 🪟🚫 Found no non-consistency window predictions for hotkey '{miner_hotkey}'")
            return 0.0

        today = datetime.now(timezone.utc).date()
        all_scores = []
        for result in past_scores:
            date, score, total_predictions = result
            date = datetime.strptime(date, "%Y-%m-%d").date()
            days_back = (today - date).days - self.consistency_window_duration
            score_scalar = self.calculate_day_weight(size_of_non_consistency_window, days_back)
            for i in range(score_scalar):
                all_scores.append(score)
            print(
                f"| {current_thread} | 🪲 🪟⚖️ Found score scalar {score_scalar} for {days_back} days back, score: {score}")
        final_score = statistics.mean(all_scores)
        print(
            f"| {current_thread} | 🪲 🪟⭐ Found {final_score} for non-consistency window score for hotkey '{miner_hotkey}'")
        return final_score

    def calculate_day_weight(self, size_of_non_consistency_window: int, days_back: int) -> int:
        """
        Calculate the weight of a day based on the number of days in the non-consistency window
        Args:
            size_of_non_consistency_window: number of days in the non-consistency window
            days_back: the number of days since this prediction

        Returns:
            The weight for the prediction
        """
        # Get the number of days since the end of the consistency window
        days_back = days_back - self.consistency_window_duration
        if days_back < 1 or days_back > size_of_non_consistency_window:
            current_thread = threading.current_thread().name
            bt.logging.trace(f"| {current_thread} | ❗ days_back '{days_back}' is out of range for size of window {size_of_non_consistency_window}")
            return 0.0

        maximum_scalar = 100
        minimum_scalar = 5

        # Map the range [1, size_of_non_consistency_window] to [maximum_scalar, minimum_scalar]
        return int(maximum_scalar - ((days_back - 1) * ((maximum_scalar - minimum_scalar) / (size_of_non_consistency_window - 1))))

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
        query_string = f"SELECT date, score, total_predictions FROM {TABLE_NAME} WHERE miner_hotkey = ? AND date < ? AND date >= ?"
        values = (miner_hotkey, consistency_window_cutoff, date_cutoff)
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
        bt.logging.debug(f"| {current_thread} | 🪲 🪟🗡 Found consistency window cutoff '{date_cutoff}'")
        return date_cutoff

    def _get_score_cutoff_date(self) -> datetime.date:
        current_thread = threading.current_thread().name
        today = datetime.today().date()
        date_cutoff = today - timedelta(days=int(self.score_date_cutoff))
        bt.logging.debug(f"| {current_thread} | 🪲 📅🗡 Found date cutoff '{date_cutoff}'")
        return date_cutoff
