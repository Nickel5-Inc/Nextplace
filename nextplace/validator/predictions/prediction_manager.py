import threading
from typing import List, Tuple
import bittensor as bt
import json
from datetime import datetime, timezone
from nextplace.protocol import RealEstatePredictions
from nextplace.validator.utils.contants import ISO8601, build_miner_predictions_table_name
from nextplace.validator.database.database_manager import DatabaseManager

"""
Helper class manages processing predictions from Miners
"""


class PredictionManager:

    def __init__(self, database_manager: DatabaseManager, metagraph):
        self.database_manager = database_manager
        self.metagraph = metagraph

    def process_predictions(self, responses: list) -> None:
        """
        Process predictions from the Miners
        Args:
            responses (list): list of synapses from Miners

        Returns:
            None
        """
        current_thread = threading.current_thread().name

        bt.logging.debug(f"| {current_thread} | 🪲 DEBUG Type of Responses: '{type(responses)}'")
        for response in responses:
            bt.logging.debug(f"| {current_thread} | 🪲 DEBUG Inner Type of Responses: '{type(response)}'")

        synapse_id = responses[0][1]

        bt.logging.info(f"| {current_thread} | 📡 Processing Responses for synapse with ID: '{synapse_id}'")

        if responses is None or len(responses) == 0:
            bt.logging.error(f'| {current_thread} | ❗No responses received')
            return

        current_utc_datetime = datetime.now(timezone.utc)
        timestamp = current_utc_datetime.strftime(ISO8601)
        valid_hotkeys = set()

        extract_synapse_data_query = "SELECT nextplace_ids FROM synapse_ids WHERE synapse_id = ?"
        delete_synapse_data_query = "DELETE FROM synapse_ids WHERE synapse_id = ?"
        nextplace_ids_tuple = (synapse_id,)
        with self.database_manager.lock:
            valid_synapse_data = self.database_manager.query_with_values(extract_synapse_data_query, nextplace_ids_tuple)  # Extract synapse data from db
            self.database_manager.query_and_commit_with_values(delete_synapse_data_query, nextplace_ids_tuple)  # Delete synapse data from db

        for idx, response in enumerate(responses):  # Iterate responses

            try:
                miner_hotkey = self.metagraph.hotkeys[idx]

                if miner_hotkey is None:
                    bt.logging.error(f"| {current_thread} | ❗ Failed to find miner_hotkey while processing predictions")
                    continue

                valid_hotkeys.add(miner_hotkey)

                table_name = build_miner_predictions_table_name(miner_hotkey)
                replace_policy_data_for_ingestion: list[tuple] = []
                ignore_policy_data_for_ingestion: list[tuple] = []

                if not valid_synapse_data or len(valid_synapse_data) == 0:
                    bt.logging.info(f"| {current_thread} | ❗ Found invalid synapse id: '{synapse_id}'")
                    return

                valid_nextplace_ids_for_synapse = valid_synapse_data[0][0]
                try:
                    nextplace_id_set = set(json.loads(valid_nextplace_ids_for_synapse))  # Ensure the string is valid JSON
                except json.JSONDecodeError as e:
                    bt.logging.error(f"| {current_thread} | ❗ Failed to decode JSON: {e}")
                    return

                for prediction in response[0].predictions:  # Iterate predictions in each response

                    # Ignore nextplace_id's that weren't sent in the original synapse
                    if prediction.nextplace_id not in nextplace_id_set:
                        bt.logging.error(f"| {current_thread} | ❗ Found invalid nextplace_id for this synapse, ignoring")
                        continue

                    # Only process valid predictions
                    if prediction is None or prediction.predicted_sale_price is None or prediction.predicted_sale_date is None:
                        continue

                    values = (
                        prediction.nextplace_id,
                        miner_hotkey,
                        prediction.predicted_sale_price,
                        prediction.predicted_sale_date,
                        timestamp,
                        prediction.market,
                    )

                    # Parse force update flag
                    if prediction.force_update_past_predictions:
                        replace_policy_data_for_ingestion.append(values)
                    else:
                        ignore_policy_data_for_ingestion.append(values)

                # Store predictions in the database
                self._create_table_if_not_exists(table_name)
                if len(ignore_policy_data_for_ingestion) > 0:
                    self._handle_ingestion('IGNORE', ignore_policy_data_for_ingestion, table_name)
                if len(replace_policy_data_for_ingestion) > 0:
                    self._handle_ingestion('REPLACE', replace_policy_data_for_ingestion, table_name)

            except Exception as e:
                bt.logging.error(f"| {current_thread} | ❗Failed to process prediction: {e}")

        self._track_miners(valid_hotkeys)

    def _track_miners(self, valid_hotkeys: set[str]) -> None:
        formatted = [(x,) for x in valid_hotkeys]
        query_str = """
            INSERT OR IGNORE INTO active_miners
            (miner_hotkey)
            VALUES (?)
        """
        self.database_manager.query_and_commit_many(query_str, formatted)

    def _create_table_if_not_exists(self, table_name: str) -> None:
        """
        Create the predictions table for this miner if it doesn't exist
        Args:
            table_name: miner's table name

        Returns:
            None
        """
        create_str = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        nextplace_id TEXT,
                        miner_hotkey TEXT,
                        predicted_sale_price REAL,
                        predicted_sale_date TEXT,
                        prediction_timestamp TEXT,
                        market TEXT,
                        PRIMARY KEY (nextplace_id, miner_hotkey)
                    )
                """
        idx_str = f"CREATE INDEX IF NOT EXISTS idx_prediction_timestamp ON {table_name}(prediction_timestamp)"
        idx_str_market = f"CREATE INDEX IF NOT EXISTS idx_market ON {table_name}(market)"
        self.database_manager.query_and_commit(create_str)
        self.database_manager.query_and_commit(idx_str)
        self.database_manager.query_and_commit(idx_str_market)

    def _handle_ingestion(self, conflict_policy: str, values: list[tuple], table_name: str) -> None:
        """
        Ingest predictions for a miner
        Args:
            conflict_policy: to ignore new predictions or replace existing predictions
            values: prediction data
            table_name: the miner's prediction table

        Returns:
            None
        """
        query_str = f"""
            INSERT OR {conflict_policy} INTO {table_name} 
            (nextplace_id, miner_hotkey, predicted_sale_price, predicted_sale_date, prediction_timestamp, market)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        self.database_manager.query_and_commit_many(query_str, values)
