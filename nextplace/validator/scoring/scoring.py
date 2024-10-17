import sqlite3
from datetime import datetime, timezone, timedelta
from time import sleep
import bittensor
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

    def __init__(self, database_manager: DatabaseManager, markets: list[dict[str, str]], metagraph: bittensor.Metagraph):
        self.metagraph = metagraph
        self.database_manager = database_manager
        self.markets = markets
        self.sold_homes_api = SoldHomesAPI(database_manager, markets)
        self.scoring_calculator = ScoringCalculator(database_manager, self.sold_homes_api)
        self.current_thread = threading.current_thread().name

    def run_score_thread(self) -> None:
        """
        Run the scoring thread
        Returns:
            None
        """
        thread_name = threading.current_thread().name

        # ToDo Migrate predictions

        while True:

            # Update the `sales` table
            self.sold_homes_api.get_sold_properties()

            # Update sales table
            for hotkey in self.metagraph.hotkeys:

                table_name = f"predictions_{hotkey}"  # Build table name

                # Check if preds table exists. If not, continue
                with self.database_manager.lock:
                    table_exists = self.database_manager.table_exists(table_name)
                if not table_exists:
                    continue

                try:
                    # Score predictions
                    self.score_predictions(table_name, hotkey)
                    self._clear_out_old_predictions(table_name)
                except sqlite3.OperationalError as e:
                    bt.logging.trace(f"| {thread_name} | 🏖️ SQLITE operational error: {e}. Note that this is may be caused by miner deregistration while trying to score the deregistered miner, in which case it is not a bug.")

                sleep(60)  # Sleep thread for 2 minutes

            with self.database_manager.lock:
                self.database_manager.delete_all_sales()  # Clear out sales table

    def score_predictions(self, table_name: str, miner_hotkey: str) -> None:
        """
        Query to get scorable predictions that haven't been scored yet
        Returns:
            list of query results
        """

        query_str = f"""
            SELECT {table_name}.nextplace_id, {table_name}.miner_hotkey, {table_name}.predicted_sale_price, {table_name}.predicted_sale_date, {table_name}.prediction_timestamp, {table_name}.market, sales.sale_price, sales.sale_date
            FROM {table_name}
            JOIN sales ON {table_name}.nextplace_id = sales.nextplace_id
            AND {table_name}.prediction_timestamp < sales.sale_date
        """

        with self.database_manager.lock:  # Acquire lock
            scorable_predictions = self.database_manager.query(query_str)  # Get scorable predictions for this home
        if len(scorable_predictions) > 0:
            scoring_data = [(x[1], x[2], x[3], x[6], x[7]) for x in scorable_predictions]
            with self.database_manager.lock:  # Acquire lock
                self.scoring_calculator.process_scorable_predictions(scoring_data, miner_hotkey)  # Score predictions for this home
            # ToDo Send predictions to website!
            self._move_predictions_to_scored(scoring_data)
            self._remove_scored_predictions_from_miner_predictions_table(table_name, scorable_predictions)

    def _move_predictions_to_scored(self, scorable_predictions: list[tuple]) -> None:
        """
        Move scorable predictions to scored predictions_table
        Args:
            scorable_predictions: list of all scorable predictions from database

        Returns:
            None
        """
        query_str = """
            INSERT OR IGNORE INTO scored_predictions
            (nextplace_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market, sale_price, sale_date, score_timestamp)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        now = datetime.now(timezone.utc).strftime(ISO8601)
        values = [(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], now) for x in scorable_predictions]
        with self.database_manager.lock:  # Acquire lock
            self.database_manager.query_and_commit_many(query_str, values)  # Execute query

    def _remove_scored_predictions_from_miner_predictions_table(self, table_name: str, scorable_predictions: list[tuple]) -> None:
        """
        Remove all scored predictions from miner's predictions table
        Args:
            table_name: name of the miner table
            scorable_predictions: list of scored predictions

        Returns:
            None
        """
        row_ids = [x[0] for x in scorable_predictions]
        formatted_ids = ','.join(f"'{str(nextplace_id)}'" for nextplace_id in row_ids)
        query_str = f"""
            DELETE FROM {table_name} WHERE nextplace_id in ({formatted_ids})
        """
        with self.database_manager.lock:  # Acquire lock
            self.database_manager.query_and_commit(query_str)  # Execute query

    def _cleanup(self, table_name: str) -> None:
        """
        Clean up after scoring. Delete all rows from sales table, clean out old predictions
        Returns: None
        """
        self.database_manager.delete_all_sales()
        self._clear_out_old_predictions(table_name)

    def _clear_out_old_predictions(self, table_name: str) -> None:
        """
        Remove predictions that were scored more than 5 days ago
        Returns:
            None
        """
        max_days = 21
        today = datetime.now(timezone.utc)
        min_date = (today - timedelta(days=max_days)).strftime(ISO8601)
        bt.logging.trace(f"| {self.current_thread} | ✘ Deleting predictions older than {min_date}")

        # Clear out predictions table
        query_str = f"""
                        DELETE FROM {table_name}
                        WHERE prediction_timestamp < '{min_date}'
                    """
        with self.database_manager.lock:
            self.database_manager.query_and_commit(query_str)
                