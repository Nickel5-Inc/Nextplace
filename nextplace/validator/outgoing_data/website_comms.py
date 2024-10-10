import threading

from nextplace.validator.database.database_manager import DatabaseManager
import requests
from datetime import datetime
import bittensor as bt


class WebsiteProcessor:

    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize with the provided DatabaseManager instance.
        """
        self.db_manager = db_manager
        self.thread_name = threading.current_thread().name

    def send_data(self):
        """
        Send unsent predictions to the endpoint and update already_sent flag.
        """
        self.process_scored_predictions()
        unsent_predictions = self.get_unsent_predictions()
        if not unsent_predictions:
            bt.logging.info(f"| {self.thread_name} | 🤖 No new predictions to send to Nextplace website.")
            return

        data_to_send = []
        for prediction in unsent_predictions:
            nextplace_id, miner_hotkey, miner_coldkey, prediction_date, predicted_sale_price, predicted_sale_date = prediction

            prediction_date_parsed = self.parse_iso_datetime(prediction_date) if isinstance(prediction_date, str) else prediction_date
            predicted_sale_date_parsed = self.parse_iso_datetime(predicted_sale_date) if isinstance(predicted_sale_date, str) else predicted_sale_date

            if prediction_date_parsed is None or predicted_sale_date_parsed is None:
                bt.logging.trace(f"| {self.thread_name} | 🏃🏻‍♂️ Skipping prediction {nextplace_id} due to date parsing error.")
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
            bt.logging.trace(f"| {self.thread_name} | Ø No valid predictions to send to Nextplace site after parsing.")
            return

        bt.logging.info(f"| {self.thread_name} | ➠ Data being sent: {data_to_send}")

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
            bt.logging.info(f"| {self.thread_name} | ✅ Data sent to Nextplace site successfully.")

            self.update_already_sent(unsent_predictions)
        except requests.exceptions.HTTPError as e:
            bt.logging.warning(f"| {self.thread_name} | ❗ HTTP error occurred: {e}. No data was sent to the Nextplace site.")
            if e.response is not None:
                bt.logging.warning(f"| {self.thread_name} | ❗ Error sending data to site. Response content: {e.response.text}")
        except requests.exceptions.RequestException as e:
            bt.logging.warning(f"| {self.thread_name} | ❗ Error sending data to site. An error occurred while sending data: {e}. No data was sent to the Nextplace site.")

    def process_scored_predictions(self):
        """
        Process scored predictions and insert them into website_comms.
        """
        with self.db_manager.lock:
            scored_predictions = self.get_scored_predictions()
            for prediction in scored_predictions:
                self.insert_website_comms(prediction)

    def get_scored_predictions(self):
        """
        Retrieve scored predictions.
        """
        query = '''
            SELECT nextplace_id,
                   miner_hotkey,
                   predicted_sale_price,
                   predicted_sale_date,
                   prediction_timestamp
            FROM predictions
            WHERE scored = 1
        '''
        return self.db_manager.query(query)

    def insert_website_comms(self, prediction):
        """
        Insert a prediction into the website_comms table if it doesn't already exist.
        """
        nextplace_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_date = prediction
        miner_coldkey = None  # Assuming miner_coldkey is not available

        insert_query = '''
            INSERT OR IGNORE INTO website_comms (
                nextplace_id,
                miner_hotkey,
                miner_coldkey,
                prediction_date,
                predicted_sale_price,
                predicted_sale_date,
                already_sent
            ) VALUES (?, ?, ?, ?, ?, ?, 0)
        '''
        values = (nextplace_id, miner_hotkey, miner_coldkey, prediction_date, predicted_sale_price, predicted_sale_date)
        self.db_manager.query_and_commit_with_values(insert_query, values)

    def get_unsent_predictions(self):
        """
        Retrieve predictions from website_comms where already_sent = 0.
        """
        query = '''
            SELECT nextplace_id,
                   miner_hotkey,
                   miner_coldkey,
                   prediction_date,
                   predicted_sale_price,
                   predicted_sale_date
            FROM website_comms
            WHERE already_sent = 0
        '''
        with self.db_manager.lock:
            results = self.db_manager.query(query)
        return results

    def update_already_sent(self, predictions):
        """
        Update the already_sent flag to 1 for the given predictions.
        """
        update_query = '''
            UPDATE website_comms
            SET already_sent = 1
            WHERE nextplace_id = ? AND miner_hotkey = ?
        '''
        values = [(prediction[0], prediction[1]) for prediction in predictions]
        with self.db_manager.lock:
            self.db_manager.query_and_commit_many(update_query, values)

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
