from datetime import timezone, datetime, timedelta
from sqlite3 import OperationalError

import threading
import bittensor as bt

from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.scoring.time_gated_scorer import TimeGatedScorer
from nextplace.validator.utils.contants import ISO8601
from nextplace.validator.website_data.website_communicator import WebsiteCommunicator


class MinerScoreSender:

    def __init__(self, database_manager: DatabaseManager):
        self.database_manager = database_manager

    def _get_empty_score_date_map(self, score_cutoff_date: datetime.date) -> dict:
        end_date = datetime.today().date()
        current_date = score_cutoff_date
        date_score_map = {}
        while current_date <= end_date:
            date_score_map[current_date.strftime("%Y-%m-%d")] = 0
            current_date += timedelta(days=1)
        return date_score_map

    def get_hotkeys(self) -> list[str]:
        query = """
            SELECT name 
            FROM sqlite_master 
            WHERE type='table' AND name LIKE 'predictions_%';
        """
        with self.database_manager.lock:
            results = self.database_manager.query(query)
        return [table[0][len("predictions_"):] for table in results]

    def send_miner_scores_to_website(self) -> None:
        """
        RUN IN THREAD
        Send miner scores to website
        Returns:
            None
        """
        current_thread = threading.current_thread().name
        data_to_send = []
        bt.logging.trace(f"| {current_thread} | 💾 Gathering miner data for web server")

        now = datetime.now(timezone.utc).strftime(ISO8601)
        time_gated_scorer = TimeGatedScorer(self.database_manager)
        score_cutoff_date = time_gated_scorer.get_score_cutoff_date()
        hotkeys = self.get_hotkeys()
        for hotkey in hotkeys:
            date_score_map = self._get_empty_score_date_map(score_cutoff_date)

            with self.database_manager.lock:
                score = time_gated_scorer.score(hotkey)
                query = "SELECT date, total_predictions FROM daily_scores WHERE miner_hotkey= ? AND date >= ?"
                values = (hotkey, score_cutoff_date)
                results = self.database_manager.query_with_values(query, values)
                num_predictions = 0
                for result in results:
                    date = result[0]
                    total_scored = result[1]
                    date_score_map[date] = total_scored
                    num_predictions += result[1]
                try:
                    total_predictions = self.database_manager.get_size_of_table(f"predictions_{hotkey}")
                except OperationalError:
                    total_predictions = 0
                scored_list = [{'date': key, 'totalScored': value} for key, value in date_score_map.items()]
                scored_list.sort(key=lambda x: x['date'], reverse=True)
                data = {
                    "minerHotKey": hotkey,
                    "minerColdKey": "N/A",
                    "minerScore": score,
                    "numPredictions": num_predictions,
                    "scoreGenerationDate": now,
                    "totalPredictions": total_predictions,
                    "minerDatedScores": scored_list
                }
                data_to_send.append(data)

        bt.logging.info(f"| {current_thread} | ⛵ Sending {len(data_to_send)} miner scores to website")
        website_communicator = WebsiteCommunicator("/Miner/Scores")
        website_communicator.send_data(data=data_to_send)
