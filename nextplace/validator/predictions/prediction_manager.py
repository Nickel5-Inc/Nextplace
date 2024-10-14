import threading
from typing import List, Tuple
import bittensor as bt
from datetime import datetime, timezone
from nextplace.protocol import RealEstatePredictions
from nextplace.validator.utils.contants import ISO8601
from nextplace.validator.database.database_manager import DatabaseManager

"""
Helper class manages processing predictions from Miners
"""


class PredictionManager:

    def __init__(self, database_manager: DatabaseManager, metagraph):
        self.database_manager = database_manager
        self.metagraph = metagraph

    def process_predictions(self, responses: List[RealEstatePredictions]) -> None:
        """
        Process predictions from the Miners
        Args:
            responses (list): list of synapses from Miners

        Returns:
            None
        """

        current_thread = threading.current_thread().name
        bt.logging.info(f'| {current_thread} | 📡 Processing Responses')

        if responses is None or len(responses) == 0:
            bt.logging.error(f'| {current_thread} | ❗No responses received')
            return

        current_utc_datetime = datetime.now(timezone.utc)
        timestamp = current_utc_datetime.strftime(ISO8601)
        replace_policy_data_for_ingestion: list[tuple] = []
        ignore_policy_data_for_ingestion: list[tuple] = []
        ids: List[Tuple[str]] = []

        for idx, real_estate_predictions in enumerate(responses):  # Iterate responses

            for prediction in real_estate_predictions.predictions:  # Iterate predictions in each response

                # Only process valid predictions
                if prediction is None or prediction.predicted_sale_price is None or prediction.predicted_sale_date is None:
                    continue

                try:
                    miner_hotkey = self.metagraph.hotkeys[idx]

                    if miner_hotkey is None:
                        continue

                    values = (
                        prediction.nextplace_id,
                        prediction.property_id,
                        miner_hotkey,
                        prediction.predicted_sale_price,
                        prediction.predicted_sale_date,
                        timestamp,
                        prediction.market,
                        False
                    )

                    # Parse force update flag
                    if prediction.force_update_past_predictions:
                        replace_policy_data_for_ingestion.append(values)
                    else:
                        ignore_policy_data_for_ingestion.append(values)

                    ids.append((prediction.nextplace_id,))  # Add to list of id's

                except Exception as e:
                    bt.logging.error(f"| {current_thread} | ❗Failed to process prediction: {e}")

        # Store predictions in the database
        self._handle_ingestion('IGNORE', ignore_policy_data_for_ingestion)
        self._handle_ingestion('REPLACE', replace_policy_data_for_ingestion)
        self._store_ids(ids)

        table_size = self.database_manager.get_size_of_table('predictions')
        bt.logging.trace(f"| {current_thread} | 📢 There are now {table_size} predictions in the database")

    def _handle_ingestion(self, conflict_policy: str, values: list[tuple]) -> None:
        query_str = f"""
            INSERT OR {conflict_policy} INTO predictions 
            (nextplace_id, property_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market, scored)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.database_manager.query_and_commit_many(query_str, values)

    def _store_ids(self, values: list[tuple]) -> None:
        query_str = """
            INSERT OR IGNORE INTO ids
            (nextplace_id)
            VALUES (?)
        """
        self.database_manager.query_and_commit_many(query_str, values)

