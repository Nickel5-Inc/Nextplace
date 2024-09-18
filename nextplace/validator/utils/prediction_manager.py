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
        # TODO: if prediction is empty, record response that is ~30% below listing price
        bt.logging.info(f'Processing Responses')

        if responses is None or len(responses) == 0:
            bt.logging.error('No responses received')
            return


        current_utc_datetime = datetime.now(timezone.utc)
        timestamp = current_utc_datetime.strftime(ISO8601)

        cursor, db_connection = self.database_manager.get_cursor()  # Get cursor & db connection

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
                            # Get price from Synapse
                            if listing_price:
                                prediction.predicted_sale_price = listing_price * 0.7
                            else:
                                # If price in synapse is empty, use properties table
                                try:
                                    cursor.execute("SELECT price FROM properties WHERE nextplace_id = ?", (prediction.nextplace_id,))
                                    result = cursor.fetchone()
                                    if result and result[0]:
                                        listing_price = result[0]
                                        prediction.predicted_sale_price = listing_price * 0.7
                                    else:
                                        bt.logging.warning(f"Listing price not found in properties table for nextplace_id: {prediction.nextplace_id}")
                                        continue
                                except Exception as e:
                                    bt.logging.error(f"Error retrieving listing price from properties table: {e}")
                                    continue
                        
                        # Check if predicted_sale_date is None, if so, use the current date
                        if prediction.predicted_sale_date is None:
                            prediction.predicted_sale_date = current_utc_datetime.strftime('%Y-%m-%d')

                        # Parse force update flag
                        if prediction.force_update_past_predictions:
                            insert_conflict_policy = "REPLACE"
                        else:
                            insert_conflict_policy = "IGNORE"

                        # Store predictions in the database
                        query = f"""
                            INSERT OR {insert_conflict_policy} INTO predictions 
                            (nextplace_id, property_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market, scored)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """
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
                        cursor.execute(query, values)
                except Exception as e:
                    bt.logging.error(f"Failed to process prediction: {e}")

        db_connection.commit()  # Commit to database
        cursor.close()  # Close the cursor
        db_connection.close()  # Close db connection

        table_size = self.database_manager.get_size_of_table('predictions')
        bt.logging.trace(f"There are now {table_size} predictions in the database")
