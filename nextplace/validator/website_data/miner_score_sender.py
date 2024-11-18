from datetime import timezone, datetime
from sqlite3 import OperationalError

from nextplace.validator.database.database_manager import DatabaseManager
import threading
import bittensor as bt

from nextplace.validator.utils.contants import ISO8601
from nextplace.validator.website_data.website_communicator import WebsiteCommunicator


class MinerScoreSender:

    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager

    def send_miner_scores_to_website(self) -> None:
        """
        RUN IN THREAD
        Send miner scores to website
        Returns:
            None
        """
        current_thread = threading.current_thread().name

        with self.database_manager.lock:
            active_miners = self.database_manager.query(f"SELECT miner_hotkey FROM active_miners")

        data_to_send = []

        now = datetime.now(timezone.utc).strftime(ISO8601)
        for hotkey in active_miners:
            hotkey = hotkey[0]
            with self.database_manager.lock:
                result = self.database_manager.query(f"SELECT lifetime_score, total_predictions, last_update_timestamp FROM miner_scores WHERE miner_hotkey='{hotkey}'")
                score = result[0][0] if len(result) == 1 else 0
                num_predictions = result[0][1] if len(result) == 1 else 0
                last_update_timestamp = result[0][2] if len(result) == 1 else now
                try:
                    total_predictions = self.database_manager.get_size_of_table(f"predictions_{hotkey}")
                except OperationalError:
                    total_predictions = 0
                data_to_send.append({
                    "minerHotKey": hotkey,
                    "minerColdKey": "N/A",
                    "minerScore": score,
                    "numPredictions": num_predictions,
                    "scoreGenerationDate": last_update_timestamp,
                    "totalPredictions": total_predictions,
                })

        bt.logging.info(f"| {current_thread} | ⛵ Sending {len(data_to_send)} miner scores to website")
        website_communicator = WebsiteCommunicator("/Miner/Scores")
        website_communicator.send_data(data=data_to_send)
