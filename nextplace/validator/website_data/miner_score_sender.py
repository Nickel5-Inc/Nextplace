from datetime import timezone, datetime
from sqlite3 import OperationalError

from nextplace.validator.database.database_manager import DatabaseManager
import threading
import bittensor as bt

from nextplace.validator.scoring.time_gated_scorer import TimeGatedScorer
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
        data_to_send = []

        now = datetime.now(timezone.utc).strftime(ISO8601)
        time_gated_scorer = TimeGatedScorer(self.database_manager)
        with self.database_manager.lock:
            hotkeys = self.database_manager.query("SELECT DISTINCT(miner_hotkey) FROM daily_scores")
        hotkeys = [x[0] for x in hotkeys]
        for hotkey in hotkeys:
            with self.database_manager.lock:
                score = time_gated_scorer.score(hotkey)
                results = self.database_manager.query(f"SELECT total_predictions FROM daily_scores WHERE miner_hotkey='{hotkey}'")
                num_predictions = 0
                for result in results:
                    num_predictions += result[0]
                try:
                    total_predictions = self.database_manager.get_size_of_table(f"predictions_{hotkey}")
                except OperationalError:
                    total_predictions = 0
                data_to_send.append({
                    "minerHotKey": hotkey,
                    "minerColdKey": "N/A",
                    "minerScore": score,
                    "numPredictions": num_predictions,
                    "scoreGenerationDate": now,
                    "totalPredictions": total_predictions,
                })

        bt.logging.info(f"| {current_thread} | ⛵ Sending {len(data_to_send)} miner scores to website")
        website_communicator = WebsiteCommunicator("/Miner/Scores")
        website_communicator.send_data(data=data_to_send)
