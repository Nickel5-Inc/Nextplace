import sqlite3
from datetime import datetime, timezone, timedelta
from time import sleep
import bittensor
import bittensor as bt
import threading
from nextplace.validator.scoring.scoring_calculator import ScoringCalculator
from nextplace.validator.api.sold_homes_api import SoldHomesAPI
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.utils.contants import ISO8601, build_miner_predictions_table_name
import requests

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
        bt.logging.trace(f"| {thread_name} | 🏁 Beginning scoring thread")

        self._check_and_migrate_predictions()

        while True:

            # Update the `sales` table
            self.sold_homes_api.get_sold_properties()

            bt.logging.trace(f"| {thread_name} | 🚀 Beginning metagraph hotkey iteration")
            # Update sales table
            for hotkey in self.metagraph.hotkeys:

                table_name = build_miner_predictions_table_name(hotkey)

                # Check if predictions table exists for this hotkey. If not, continue
                with self.database_manager.lock:
                    table_exists = self.database_manager.table_exists(table_name)
                if not table_exists:
                    continue

                bt.logging.trace(f"| {thread_name} | ⛏️ Scoring miner with hotkey '{hotkey}'")

                try:
                    self.score_predictions(table_name, hotkey)  # Score predictions
                    self._clear_out_old_predictions(table_name)  # Remove old predictions from miner's table
                except sqlite3.OperationalError as e:
                    bt.logging.trace(f"| {thread_name} | 🏖️ SQLITE operational error: {e}. Note that this is may be caused by miner deregistration while trying to score the deregistered miner, in which case it is not a bug.")

                sleep(120)  # Sleep thread for 2 minutes

            with self.database_manager.lock:
                self.database_manager.delete_all_sales()  # Clear out sales table
                self._clear_out_old_predictions('scored_predictions')  # Clear out old scored predictions

    def _check_and_migrate_predictions(self) -> None:
        """
        Migrate predictions if we need to

        Returns:
            None
        """
        with self.database_manager.lock:
            predictions_table_exists = self.database_manager.table_exists('predictions')
            if not predictions_table_exists:
                return

            all_table_query = "SELECT name FROM sqlite_master WHERE type='table'"
            all_tables = [x[0] for x in self.database_manager.query(all_table_query)]  # Get all tables in database
            miner_predictions_tables_exist = any("predictions_" in s for s in all_tables)  # Check if we have any tables that start with "predictions_"

            if miner_predictions_tables_exist:
                return

            thread_name = threading.current_thread().name
            bt.logging.trace(f"| {thread_name} | 💾 Migrating predictions, this may take a while...")

            all_hotkeys_in_predictions = self.database_manager.query("SELECT DISTINCT(miner_hotkey) FROM predictions")
            for idx, miner_hotkey in enumerate(all_hotkeys_in_predictions):

                # Build table name
                table_name = build_miner_predictions_table_name(miner_hotkey)

                # Create and index table
                create_str = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        nextplace_id TEXT,
                        miner_hotkey TEXT,
                        predicted_sale_price REAL,
                        predicted_sale_date TEXT,
                        prediction_timestamp TEXT,
                        market TEXT,
                        PRIMARY KEY (nextplace_id, miner_hotkey)
                    )
                """
                idx_str = f"CREATE INDEX IF NOT EXISTS idx_prediction_timestamp ON {table_name}(prediction_timestamp)"
                self.database_manager.query_and_commit(create_str)
                self.database_manager.query_and_commit(idx_str)

                # Get predictions
                miner_predictions = self.database_manager.query(f"""
                    SELECT nextplace_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market
                    FROM predictions
                    WHERE miner_hotkey='{miner_hotkey}'
                """)

                # Migrate predictions
                insert_query = f"""
                    INSERT OR IGNORE INTO {table_name}
                    (nextplace_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market),
                    VALUES(?, ?, ?, ?, ?, ?)
                """
                self.database_manager.query_and_commit_many(insert_query, miner_predictions)

                percent_done = round(((idx + 1) / len(all_hotkeys_in_predictions)) * 100, 2)
                bt.logging.trace(f"| {thread_name} | 📩 {percent_done}% done migrating predictions")

    def score_predictions(self, table_name: str, miner_hotkey: str) -> None:
        """
        Query to get scorable predictions that haven't been scored yet
        Returns:
            list of query results
        """
        current_thread = threading.current_thread().name
        scorable_predictions = self._get_scorable_predictions(table_name)
        bt.logging.trace(f"| {current_thread} | 🏅 Found {len(scorable_predictions)} predictions to score")
        if len(scorable_predictions) > 0:
            scoring_data = [(x[1], x[2], x[3], x[6], x[7]) for x in scorable_predictions]
            self.scoring_calculator.process_scorable_predictions(scoring_data, miner_hotkey)  # Score predictions for this home
            self._send_data_to_website(scorable_predictions)  # Send data to website
            self._move_predictions_to_scored(scoring_data)  # Move scored predictions to scored_predictions table
            self._remove_scored_predictions_from_miner_predictions_table(table_name, scorable_predictions)  # Drop scored predictions from miner predictions table

    def _get_scorable_predictions(self, table_name: str) -> list[tuple]:
        """
        Retrieve scorable predictions for current miner
        Args:
            table_name: name of miner's predictions table

        Returns:
            List of scorable predictions
        """
        query_str = f"""
            SELECT {table_name}.nextplace_id, {table_name}.miner_hotkey, {table_name}.predicted_sale_price, {table_name}.predicted_sale_date, {table_name}.prediction_timestamp, {table_name}.market, sales.sale_price, sales.sale_date
            FROM {table_name}
            JOIN sales ON {table_name}.nextplace_id = sales.nextplace_id
            AND {table_name}.prediction_timestamp < sales.sale_date
        """

        with self.database_manager.lock:  # Acquire lock
            scorable_predictions = self.database_manager.query(query_str)  # Get scorable predictions for this home
        return scorable_predictions

    def _send_data_to_website(self, scored_predictions: list[tuple]) -> None:
        """
        Send scored prediction data to the NextPlace website
        Args:
            scored_predictions: list of scored predictions

        Returns:
            None
        """
        thread_name = threading.current_thread().name
        bt.logging.trace(f"| {thread_name} | 🎢 Attempting to send {len(scored_predictions)} scored predictions to NextPlace website")

        formatted_predictions = [(x[0], x[1], None, x[4], x[2], x[3]) for x in scored_predictions]
        data_to_send = []

        for prediction in formatted_predictions:

            nextplace_id, miner_hotkey, miner_coldkey, prediction_date, predicted_sale_price, predicted_sale_date = prediction
            prediction_date_parsed = self.parse_iso_datetime(prediction_date) if isinstance(prediction_date, str) else prediction_date
            predicted_sale_date_parsed = self.parse_iso_datetime(predicted_sale_date) if isinstance(predicted_sale_date, str) else predicted_sale_date

            if prediction_date_parsed is None or predicted_sale_date_parsed is None:
                bt.logging.trace(f"| {thread_name} | 🏃🏻‍♂️ Skipping prediction {nextplace_id} due to date parsing error.")
                continue

            prediction_date_iso = prediction_date_parsed.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            predicted_sale_date_iso = predicted_sale_date_parsed.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

            data_dict = {
                "nextplaceId": nextplace_id,
                "minerHotKey": miner_hotkey,
                "minerColdKey": miner_coldkey if miner_coldkey else "DummyColdkey",
                "predictionDate": prediction_date_iso,
                "predictedSalePrice": predicted_sale_price,
                "predictedSaleDate": predicted_sale_date_iso
            }
            data_to_send.append(data_dict)

        if not data_to_send:
            bt.logging.trace(f"| {thread_name} | Ø No valid predictions to send to Nextplace site after parsing.")
            return

        bt.logging.info(f"| {thread_name} | ➠ Sending {len(data_to_send)} predictions to the website")

        headers = {
            'Accept': '*/*',
            'Content-Type': 'application/json'
        }

        try:
            response = requests.post(
                "https://dev-nextplace-api.azurewebsites.net/Predictions",
                json=data_to_send,
                headers=headers
            )
            response.raise_for_status()
            bt.logging.info(f"| {thread_name} | ✅ Data sent to Nextplace site successfully.")

        except requests.exceptions.HTTPError as e:
            bt.logging.warning(f"| {thread_name} | ❗ HTTP error occurred: {e}. No data was sent to the Nextplace site.")
            if e.response is not None:
                bt.logging.warning(f"| {thread_name} | ❗ Error sending data to site. Response content: {e.response.text}")
        except requests.exceptions.RequestException as e:
            bt.logging.warning(f"| {thread_name} | ❗ Error sending data to site. An error occurred while sending data: {e}. No data was sent to the Nextplace site.")

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

    def _remove_scored_predictions_from_miner_predictions_table(self, table_name: str, scored_predictions: list[tuple]) -> None:
        """
        Remove all scored predictions from miner's predictions table
        Args:
            table_name: name of the miner table
            scorable_predictions: list of scored predictions

        Returns:
            None
        """
        row_ids = [x[0] for x in scored_predictions]
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

    def parse_iso_datetime(self, datetime_str: str):
        """
        Parses an ISO 8601 datetime string, handling strings that end with 'Z'.
        Returns a naive datetime object (without timezone info).
        """
        try:
            if datetime_str.endswith('Z'):
                datetime_str = datetime_str.rstrip('Z')
                dt = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')
                return dt
            else:
                return datetime.fromisoformat(datetime_str)
        except ValueError as e:
            bt.logging.info(f"| {self.thread_name} | ❗ Error in sending data. Trying to parse datetime string '{datetime_str}': {e}")
            return None
                