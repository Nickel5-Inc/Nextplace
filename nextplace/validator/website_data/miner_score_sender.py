from nextplace.validator.database.database_manager import DatabaseManager
import threading
import bittensor as bt
from nextplace.validator.website_data.website_communicator import WebsiteCommunicator
import configparser
import os


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
        config_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'setup.cfg')
        config = configparser.ConfigParser()
        config.read(config_file_path)
        version = config.get('metadata', 'version', fallback=None)

        bt.logging.trace(f"| {current_thread} | 📂 Using validator version {version}")

        with self.database_manager.lock:
            miner_scores = self.database_manager.query("SELECT miner_hotkey, lifetime_score, total_predictions, last_update_timestamp FROM miner_scores")
        if len(miner_scores) == 0:
            bt.logging.info(f"| {current_thread} | 🔔 No miner scores to send to website")

        with self.database_manager.lock:
            data_to_send = [
                {
                    "minerHotKey": x[0],
                    "minerColdKey": "N/A",
                    "minerScore": x[1],
                    "numPredictions": x[2],
                    "scoreGenerationDate": x[3],
                    "totalPredictions": self.database_manager.get_size_of_table(f"predictions_{x[0]}"),
                    "validatorVersion": version
                }
                for x in miner_scores
            ]

        bt.logging.info(f"| {current_thread} | ⛵ Sending {len(miner_scores)} miner scores to website")
        website_communicator = WebsiteCommunicator("/Miner/Scores")
        website_communicator.send_data(data=data_to_send)
