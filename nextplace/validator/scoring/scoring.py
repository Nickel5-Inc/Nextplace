from datetime import datetime, timezone, timedelta
from typing import List

import bittensor as bt
import threading
from nextplace.validator.scoring.scoring_calculator import ScoringCalculator
from nextplace.validator.api.sold_homes_api import SoldHomesAPI
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.utils.contants import ISO8601

"""
Helper class manages scoring Miner predictions
"""


class Scorer:

    def __init__(self, database_manager: DatabaseManager, markets: list[dict[str, str]]):
        self.database_manager = database_manager
        self.markets = markets
        self.sold_homes_api = SoldHomesAPI(database_manager, markets)
        self.scoring_calculator = ScoringCalculator(database_manager, self.sold_homes_api)
        self.current_thread = threading.current_thread().name

    def run_score_predictions(self) -> None:
        """
        RUN IN THREAD
        Ingest sold homes since oldest unscored prediction, JOIN `sales` and `predictions` table, score predictions
        Returns:
            None
        """
        with self.database_manager.lock:
            num_predictions = self.database_manager.get_size_of_table('predictions')
        if num_predictions == 0:
            bt.logging.trace(f"| {self.current_thread} | No predictions yet, nothing to score.")
            return
        self.sold_homes_api.get_sold_properties()  # Update the `sales` table
        with self.database_manager.lock:
            num_sales = self.database_manager.get_size_of_table('sales')
            bt.logging.info(f"| {self.current_thread} | 🛒 Ingested {num_sales} sold homes since our oldest prediction. Checking for overlapping predictions.")
        self.score_predictions()
        with self.database_manager.lock:
            self._cleanup()

    def _cleanup(self) -> None:
        """
        Clean up after scoring. Delete all rows from sales table, clean out old predictions
        Returns: None
        """
        self.database_manager.delete_all_sales()
        self._clear_out_old_predictions()
        bt.logging.info(f"| {self.current_thread} | ✅ Finished updating scores")

    def _clear_out_old_predictions(self) -> None:
        """
        Remove predictions that were scored more than 5 days ago
        Returns:
            None
        """
        max_days = 21
        today = datetime.now(timezone.utc)
        min_date = (today - timedelta(days=max_days)).strftime(ISO8601)
        bt.logging.trace(f"| {self.current_thread} | ✘ Deleting predictions older than {min_date}")
        query_str = f"""
                        DELETE FROM predictions
                        WHERE prediction_timestamp < '{min_date}'
                    """
        self.database_manager.query_and_commit(query_str)

    def score_predictions(self):
        """
        Query to get scorable predictions that haven't been scored yet
        Returns:
            list of query results
        """
        ids = self._get_ids()
        bt.logging.trace(f"| {self.current_thread} | 🔎 Found {len(ids)} potential homes for scoring")

        for candidate_id in ids:  # Iterate homes

            query_str = f"""
                SELECT predictions.property_id, predictions.miner_hotkey, predictions.predicted_sale_price, predictions.predicted_sale_date
                FROM predictions
                JOIN sales ON predictions.nextplace_id = sales.nextplace_id
                WHERE predictions.nextplace_id = '{candidate_id}'
                AND predictions.prediction_timestamp < sales.sale_date
                AND (predictions.scored = 0 OR predictions.scored = FALSE OR predictions.scored IS NULL)
            """

            with self.database_manager.lock:  # Acquire lock
                scorable_predictions = self.database_manager.query(query_str)  # Get scorable predictions for this home
                if len(scorable_predictions) > 0:
                    self.scoring_calculator.process_scorable_predictions(scorable_predictions)  # Score predictions for this home

        # Delete ids from table
        delete_query = f"""
            DELETE FROM ids
            WHERE nextplace_id IN ({ids})
        """
        with self.database_manager.lock:
            self.database_manager.query_and_commit(delete_query)

    def _get_ids(self) -> List[str]:
        """
        Retrieve all nextplace_ids that are present in both the `ids` table and the `sales` table
        Returns:
            list of ids
        """
        query_str = """
            SELECT sales.nextplace_id
            FROM sales
            JOIN ids ON sales.nextplace_id = ids.nextplace_id
        """
        with self.database_manager.lock:
            results = self.database_manager.query(query_str)
        return [result[0] for result in results]
                