from typing import List
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
        bt.logging.info(f'📡 Processing Responses')

        if responses is None or len(responses) == 0:
            bt.logging.error('❗No responses received')
            return

        current_utc_datetime = datetime.now(timezone.utc)
        timestamp = current_utc_datetime.strftime(ISO8601)
        replace_policy_data_for_ingestion: list[tuple] = []
        ignore_policy_data_for_ingestion: list[tuple] = []

        for idx, real_estate_predictions in enumerate(responses):  # Iterate responses

            for prediction in real_estate_predictions.predictions:  # Iterate predictions in each response
                # Only process valid predictions
                if prediction is None or prediction.property_id is None:
                    continue

                try:
                    miner_hotkey = self.metagraph.hotkeys[idx]
                    if miner_hotkey is not None:

                        # Check if predicted_sale_price is None, if so, calculate it using 70% of listing price
                        if prediction.predicted_sale_price is None:
                            listing_price = prediction.price
                            if listing_price:
                                prediction.predicted_sale_price = listing_price * 0.7
                            else:
                                continue

                        # Check if predicted_sale_date is None, if so, use the current date
                        if prediction.predicted_sale_date is None:
                            prediction.predicted_sale_date = current_utc_datetime.strftime('%Y-%m-%d')

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

                except Exception as e:
                    bt.logging.error(f"❗Failed to process prediction: {e}")

        # Store predictions in the database
        self._handle_ingestion('IGNORE', ignore_policy_data_for_ingestion)
        self._handle_ingestion('REPLACE', replace_policy_data_for_ingestion)

        table_size = self.database_manager.get_size_of_table('predictions')
        bt.logging.trace(f"📢 There are now {table_size} predictions in the database")

    def _handle_ingestion(self, conflict_policy: str, values: list[tuple]) -> None:
        query_str = f"""
            INSERT OR {conflict_policy} INTO predictions 
            (nextplace_id, property_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market, scored)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.database_manager.query_and_commit_many(query_str, values)
