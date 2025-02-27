import threading
import time
import requests
import csv
import bittensor as bt
from nextplace.validator.database.database_manager import DatabaseManager

API_KEY = "tao-44658c1e-7dce-41ef-ac6b-15942665440e:ef1cc958"

class TopMinerPreds:

    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager

    def _store_preds(self):
        thread_name = threading.current_thread().name
        hotkey = self._get_top_miner_hotkey()
        bt.logging.info(f"| {thread_name} | Found top miner: '{hotkey}'")
        self._write_preds_to_disc(hotkey)

    def _get_top_miner_hotkey(self) -> str:
        url = "https://api.taostats.io/api/metagraph/latest/v1?netuid=48&limit=1&order=incentive_desc"
        headers = {
            "accept": "application/json",
            "Authorization": "tao-44658c1e-7dce-41ef-ac6b-15942665440e:ef1cc958"
        }
        response = requests.get(url, headers=headers)
        body = response.json()
        return body['data'][0]['hotkey']['ss58']

    def _write_preds_to_disc(self, hotkey: str):
        with self.database_manager.lock():
            result = self.database_manager.query(f"SELECT nextplace_id, predicted_sale_price, predicted_sale_date FROM predictions_{hotkey}")
        csv_file_path = "top_preds.csv"
        with open(csv_file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["nextplace_id", "predicted_sale_price", "predicted_sale_date"])
            writer.writerows(result)
        thread_name = threading.current_thread().name
        bt.logging.info(f"| {thread_name} | Finished writing to file")

    def run(self):
        # RUN IN THREAD
        thread_name = threading.current_thread().name
        while True:
            bt.logging.info(f"| {thread_name} | Getting top preds")
            self._store_preds()
            time.sleep(60 * 60 * 24)  # Sleep for 1 day
