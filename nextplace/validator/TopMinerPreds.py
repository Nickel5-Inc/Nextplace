import schedule
import time
import requests
import csv
from nextplace.validator.database.database_manager import DatabaseManager

API_KEY = "tao-44658c1e-7dce-41ef-ac6b-15942665440e:ef1cc958"

class TopMinerPreds:

    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager

    def _store_preds(self):
        hotkey = self._get_top_miner_hotkey()
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

    def run(self):

        def job():
            print("Running JOB")
            self._store_preds()
            print("Predictions stored successfully.")

        # Schedule to run every 24 hours
        schedule.every(24).hours.do(job)

        # Keep the script running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
