import sqlite3
from datetime import datetime, timezone, timedelta
from time import sleep
import bittensor as bt
import threading
from nextplace.validator.scoring.scoring_calculator import ScoringCalculator
from nextplace.validator.api.sold_homes_api import SoldHomesAPI
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.utils.contants import ISO8601, build_miner_predictions_table_name
from nextplace.validator.website_data.website_communicator import WebsiteCommunicator
import requests

"""
Helper class manages scoring Miner predictions
"""


class Scorer:

    def __init__(self, database_manager: DatabaseManager, markets: list[dict[str, str]], metagraph):
        self.metagraph = metagraph
        self.database_manager = database_manager
        self.markets = markets
        self.sold_homes_api = SoldHomesAPI(database_manager, markets)
        self.scoring_calculator = ScoringCalculator(database_manager, self.sold_homes_api)
        self.sales_timer = datetime.now(timezone.utc)

    def run_score_thread(self) -> None:
        """
        Run the scoring thread
        Returns:
            None
        """
        thread_name = threading.current_thread().name
        bt.logging.trace(f"| {thread_name} | 🏁 Beginning scoring thread")

        # If no sales, get them
        with self.database_manager.lock:
            number_of_sales = self.database_manager.get_size_of_table('sales')
        if number_of_sales == 0:
            self.sold_homes_api.get_sold_properties()  # Get recently sold homes
            now = datetime.now(timezone.utc)
            self.sales_timer = now

        while True:

            # Refresh the `sales` table every 12ish hours
            now = datetime.now(timezone.utc)
            if now - self.sales_timer > timedelta(hours=12):
                bt.logging.trace(f"| {thread_name} | 🏷️ Time to refresh recently sold homes")
                self.sales_timer = now
                self.database_manager.delete_all_sales()  # Clear out sales table
                self.sold_homes_api.get_sold_properties()  # Get recently sold homes

            bt.logging.trace(f"| {thread_name} | 🚀 Beginning metagraph hotkey iteration")
            miners = [hotkey for uid, hotkey in enumerate(self.metagraph.hotkeys) if self.metagraph.S[uid] < 1000.0]

            for hotkey in miners:  # Iterate metagraph hotkeys

                table_name = build_miner_predictions_table_name(hotkey)  # Get name of this miner's predictions table

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
                self._clear_out_old_predictions('scored_predictions')  # Clear out old scored predictions

    def score_predictions(self, table_name: str, miner_hotkey: str) -> None:
        """
        Query to get scorable predictions that haven't been scored yet
        Returns:
            list of query results
        """
        current_thread = threading.current_thread().name
        scorable_predictions = self._get_scorable_predictions(table_name)
        if len(scorable_predictions) > 0:
            bt.logging.trace(f"| {current_thread} | 🏅 Found {len(scorable_predictions)} predictions to score")
            scoring_data = [(x[1], x[2], x[3], x[6], x[7]) for x in scorable_predictions]
            self.scoring_calculator.process_scorable_predictions(scoring_data, miner_hotkey)  # Score predictions for this home
            self._send_data_to_website(scorable_predictions)  # Send data to website
            self._move_predictions_to_scored(scorable_predictions)  # Move scored predictions to scored_predictions table
            self._remove_scored_predictions_from_miner_predictions_table(table_name, scorable_predictions)  # Drop scored predictions from miner predictions table

        # Check if they have any scored predictions. If not, check if *any* validator has scored predictions for them.
        else:
            bt.logging.trace(f"| {current_thread} | 0️⃣ Found no new predictions to score")
            with self.database_manager.lock:
                query = "SELECT COUNT(*) FROM daily_scores WHERE miner_hotkey = ?"
                values = (miner_hotkey, )
                query_result = self.database_manager.query_with_values(query, values)
            if query_result is None or len(query_result) == 0:
                bt.logging.debug(f"| {current_thread} | ❗ Error querying for {miner_hotkey}'s scored predictions")
                return
            number_of_days_with_scores = query_result[0][0]
            if number_of_days_with_scores == 0:  # This miner has no scored predictions in our db (their scores is 0)
                bt.logging.trace(f"| {current_thread} | 🔊 Found no scored predictions. Checking if another validator has any scored predictions.")
                avg_score_from_other_valis = self._get_miner_score_data_from_webserver(miner_hotkey)
                if avg_score_from_other_valis > 0:  # Other validators have scores for this miner
                    # Insert consensus score from other valis into our db for ONE SINGLE score
                    today = datetime.now(timezone.utc).date()  # Get today's date
                    query_str = f"""
                        INSERT INTO daily_scores (miner_hotkey, date, score, total_predictions)
                        VALUES (?, ?, ?, ?)
                    """
                    values = (miner_hotkey, today, avg_score_from_other_valis, 1)
                    with self.database_manager.lock:
                        self.database_manager.query_and_commit_with_values(query_str, values)

    def _get_miner_score_data_from_webserver(self, miner_hotkey: str) -> int:
        current_thread = threading.current_thread().name
        url = "https://dev-nextplace-api.azurewebsites.net/Miner/Stats"
        params = {
            "MinerHotKey": miner_hotkey
        }

        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            if len(data) == 0:  # No validator has data for this miner
                return 0
            try:
                avg_score = int(data[0]["miner"]["avgScore"])
                bt.logging.trace(f"| {current_thread} | 📌 Found score consensus from other valis: {avg_score}")
                return avg_score
            except (KeyError, TypeError, IndexError) as e:
                bt.logging.trace(f"| {current_thread} | ❗ Failed to parse response: {data}\nError: {e}")
                return 0
        else:
            bt.logging.trace(f"| {current_thread} | ❗ Failed to retrieve data. Status code: {response.status_code}")
            bt.logging.trace(f"| {current_thread} | ❗ Response:", response.text)
            return 0

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
            AND DATE({table_name}.prediction_timestamp) < DATE(sales.sale_date)
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

        formatted_predictions = [(x[0], x[1], None, x[4], x[2], x[3], x[6], x[7]) for x in scored_predictions]
        data_to_send = []

        for prediction in formatted_predictions:

            nextplace_id, miner_hotkey, miner_coldkey, prediction_date, predicted_sale_price, predicted_sale_date, sale_price, sale_date = prediction
            score = self.scoring_calculator.calculate_score(sale_price, predicted_sale_price, sale_date, predicted_sale_date, miner_hotkey)
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
                "predictionScore": score,
                "predictionDate": prediction_date_iso,
                "predictedSalePrice": predicted_sale_price,
                "predictedSaleDate": predicted_sale_date_iso,
            }
            data_to_send.append(data_dict)

        if not data_to_send:
            bt.logging.trace(f"| {thread_name} | Ø No valid predictions to send to Nextplace site after parsing.")
            return

        website_communicator = WebsiteCommunicator("Predictions")
        website_communicator.send_data(data=data_to_send)

    def _move_predictions_to_scored(self, scored_predictions: list[tuple]) -> None:
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
        values = [(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], now) for x in scored_predictions]
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
        current_thread = threading.current_thread().name
        max_days = 21
        today = datetime.now(timezone.utc)
        min_date = (today - timedelta(days=max_days)).strftime(ISO8601)
        bt.logging.trace(f"| {current_thread} | ✘ Deleting predictions older than {min_date} from table '{table_name}'")

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
        thread_name = threading.current_thread().name
        try:
            if datetime_str.endswith('Z'):
                datetime_str = datetime_str.rstrip('Z')
                dt = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')
                return dt
            else:
                return datetime.fromisoformat(datetime_str)
        except ValueError as e:
            bt.logging.info(f"| {thread_name} | ❗ Error in sending data. Trying to parse datetime string '{datetime_str}': {e}")
            return None
                