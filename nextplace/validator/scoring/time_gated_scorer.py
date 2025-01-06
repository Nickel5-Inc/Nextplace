import statistics
import threading
from datetime import datetime, timedelta, timezone
from nextplace.validator.database.database_manager import DatabaseManager
import bittensor as bt


class TimeGatedScorer:
    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager
        self.score_date_cutoff = 21
        self.consistency_window_duration = 5
        self.min_consistency_window_percent = 51.0

    def score(self, miner_hotkey: str) -> float:
        """
        Score the miner
        Args:
            miner_hotkey: date of the oldest prediction for this miner

        Returns:
            The score for the miner
        """
        # Get the oldest prediction in daily_scores for this miner
        oldest_prediction_date = self._get_oldest_prediction_date(miner_hotkey)

        # Handle the case where this miner has no scored predictions
        if oldest_prediction_date is None:
            return 0.0

        # Get consistency window metrics
        consistency_window_percent = self._get_consistency_window_percent(oldest_prediction_date)
        consistency_window_score, score_scalar = self._get_consistency_window_score(miner_hotkey)

        # Get non-consistency window metrics
        non_consistency_window_percent = 100.0 - consistency_window_percent
        size_of_non_consistency_window = self.get_size_of_non_consistency_window(oldest_prediction_date)
        non_consistency_window_score = self._get_non_consistency_window_score(miner_hotkey, size_of_non_consistency_window, non_consistency_window_percent)

        # Scale each set scores based on hyperparameters, scalar
        calculated_score = ((consistency_window_score * consistency_window_percent) / 100) + ((non_consistency_window_score * non_consistency_window_percent) / 100)
        final_score = calculated_score * score_scalar

        # current_thread = threading.current_thread().name
        # bt.logging.debug(f"| {current_thread} | 🚩 Miner '{miner_hotkey}' received weighted score: {final_score}")

        return final_score

    def get_size_of_non_consistency_window(self, oldest_prediction_date: datetime.date) -> int:
        """
        Calculate the size of the non-consistency window
        Args:
            oldest_prediction_date: date of the oldest prediction for this miner

        Returns:
            The number of days within the non-consistency window
        """
        today = datetime.now(timezone.utc).date()  # Get today's date
        difference = (today - oldest_prediction_date).days  # Calculate the difference
        if difference <= self.consistency_window_duration:  # No non-consistency window days for this miner
            return 0

        # If reg'd for than score_date_cutoff, return diff between consistency window duration and cutoff, else return diff between reg date and conistency window length
        return min((self.score_date_cutoff - self.consistency_window_duration), (difference - self.consistency_window_duration))


    def _get_consistency_window_percent(self, oldest_prediction_date: datetime.date) -> float:
        """
        Get the consistency window percentage
        Args:
            miner_hotkey: hotkey for the miner being scored

        Returns:
            Percentage [0.0, 100.0]
        """
        max_consistency_window_percent = 100.0  # Define the max percent
        today = datetime.now(timezone.utc).date()  # Today's date
        difference = (today - oldest_prediction_date).days  # Number of days within consistency window where there are score predictions

        # If miner reg'd for less than consistency window days, return max
        if difference <= self.consistency_window_duration:
            return max_consistency_window_percent

        # If miner reg'd for more than score date cutoff days, return min
        if difference >= self.score_date_cutoff:
            return self.min_consistency_window_percent

        # Scale the percent based on number of days reg'd
        return max_consistency_window_percent + ((self.min_consistency_window_percent - max_consistency_window_percent) * (difference - self.consistency_window_duration)) / (self.score_date_cutoff - self.consistency_window_duration)

    def _get_oldest_prediction_date(self, miner_hotkey: str) -> datetime.date or None:
        """
        Get the date of the oldest prediction
        Args:
            miner_hotkey: hotkey for the miner being scored

        Returns:
            Date object representing the date of the oldest prediction
        """

        # Query
        query_string = f"SELECT date FROM daily_scores WHERE miner_hotkey = ? ORDER BY date LIMIT 1"
        values = (miner_hotkey, )
        with self.database_manager.lock:
            results = self.database_manager.query_with_values(query_string, values)

        # Handle invalid query results
        if results is None or len(results) == 0:
            return None
        result = results[0][0]  # Parse result

        # Format result as a Date
        return datetime.strptime(result, "%Y-%m-%d").date()

    def _get_consistency_window_score(self, miner_hotkey: str) -> tuple[float, float]:
        """
        Retrieve the past scores for the miner
        :param miner_hotkey: hotkey for the miner being scored
        Returns:
            list of relevant historic scores for the miner
        """
        consistency_window_cutoff = self._get_consistency_window_start_date()  # Get cutoff

        # Query
        query_string = f"SELECT score, total_predictions FROM daily_scores WHERE miner_hotkey = ? AND date >= ?"
        values = (miner_hotkey, consistency_window_cutoff)
        with self.database_manager.lock:
            results = self.database_manager.query_with_values(query_string, values)

        # Handle invalid query results
        if results is None or len(results) == 0:
            return 0.0, 0.0  # Return a score of 0 if no results.

        # Calculate actual score by re-building daily score sums
        all_predictions = 0
        total_score = 0
        for result in results:  # Iterate query results
            score, day_total_predictions = result  # Parse row
            total_score += (score * day_total_predictions)  # Rebuild daily score sum
            all_predictions += day_total_predictions  # Sum daily predictions
        score = total_score / all_predictions  # Calculate true average for time range
        score_scalar = self._get_score_scalar(all_predictions)  # Calculate scalar for low prediction volume
        return score, score_scalar  # Scale the score

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
            return 0.9
        else:
            return 1

    def _get_non_consistency_window_score(self, miner_hotkey: str, size_of_non_consistency_window: int, non_consistency_window_percent: float) -> float:
        """
        Get the score for the non consistency window
        Args:
            miner_hotkey: this miner's hotkey
            size_of_non_consistency_window: number of days in the non-consistency window

        Returns:
            the score for the non-consistency window
        """
        current_thread = threading.current_thread().name
        past_scores = self._get_past_scores(miner_hotkey)  # Get all past score within non-consistency window

        # If no scored predictions in the window, return 0
        if len(past_scores) == 0:
            return 0.0

        today = datetime.now(timezone.utc).date()  # Get today's date
        all_scores = []  # List to hold score copies for averaging
        for result in past_scores:  # Iterate query results
            date, score, total_predictions = result  # Parse row
            date = datetime.strptime(date, "%Y-%m-%d").date()  # Format date from row
            days_back = (today - date).days - self.consistency_window_duration  # Calculate how long ago this day was
            score_scalar = self.calculate_day_weight(size_of_non_consistency_window, days_back)  # Get scalar based on day

            # Add *n* copies of this score to the list, where *n* is the value of the scalar
            for i in range(score_scalar):
                all_scores.append(score)

        final_score = statistics.mean(all_scores)
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

        # Handle invalid values for days_back
        if days_back < 1 or days_back > size_of_non_consistency_window:
            current_thread = threading.current_thread().name
            bt.logging.trace(f"| {current_thread} | ❗ days_back '{days_back}' is out of range for size of window {size_of_non_consistency_window}")
            return 0

        # Define hyperparameters
        maximum_scalar = 100  # Maximum scalar value
        minimum_scalar = 5  # Minimum scalar value

        if size_of_non_consistency_window == 1:
            return maximum_scalar

        # Map the range [1, size_of_non_consistency_window] to [maximum_scalar, minimum_scalar]
        return int(maximum_scalar - ((days_back - 1) * ((maximum_scalar - minimum_scalar) / (size_of_non_consistency_window - 1))))

    def _get_past_scores(self, miner_hotkey: str) -> list:
        """
        Retrieve the past scores for the miner
        Args:
            miner_hotkey hotkey for the miner being scored
        Returns:
            list of relevant historic scores for the miner
        """
        date_cutoff = self.get_score_cutoff_date()  # Calculate date cutoff
        consistency_window_cutoff = self._get_consistency_window_start_date()  # Calculate consistency window start

        # Query finds all scores between the date cutoff and the start of the consistency window
        query_string = f"SELECT date, score, total_predictions FROM daily_scores WHERE miner_hotkey = ? AND date < ? AND date >= ?"
        values = (miner_hotkey, consistency_window_cutoff, date_cutoff)
        with self.database_manager.lock:
            results = self.database_manager.query_with_values(query_string, values)

        # Handle invalid query results
        if results is None or len(results) == 0:
            return []

        # Return valid query results
        return results

    def _get_consistency_window_start_date(self) -> datetime.date:
        """
        Calculate the start date for the consistency window
        Returns:
            The date of the beginning of the consistency window
        """
        today = datetime.now(timezone.utc).date()  # Today's date
        return today - timedelta(days=int(self.consistency_window_duration))

    def get_score_cutoff_date(self) -> datetime.date:
        """
        Calculate the start date for the score window
        Returns:
            The date of the beginning of the scoring window
        """
        today = datetime.now(timezone.utc).date()  # Today's date
        return today - timedelta(days=int(self.score_date_cutoff))