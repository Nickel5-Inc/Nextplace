import threading
import bittensor as bt
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.utils.contants import build_miner_predictions_table_name, \
    get_miner_hotkeys_from_predictions_tables


class MinerManager:

    def __init__(self, database_manager: DatabaseManager, metagraph):
        self.database_manager = database_manager
        self.metagraph = metagraph

    def manage_miner_data(self) -> None:
        """
        Remove data for deregistered miners
        Returns:
            None
        """
        current_thread = threading.current_thread().name

        # Build sets
        metagraph_hotkeys = set(self.metagraph.hotkeys)  # Get hotkeys in metagraph
        hotkeys_with_pred_tables = set(get_miner_hotkeys_from_predictions_tables(self.database_manager))

        bt.logging.info(
            f"| {current_thread} | Managing active miners. Found {len(hotkeys_with_pred_tables)} tracked miners and {len(metagraph_hotkeys)} metagraph hotkeys")

        # Set operation
        deregistered_hotkeys = list(
            hotkeys_with_pred_tables.difference(metagraph_hotkeys))  # Deregistered hotkeys are stored, but not in the metagraph

        # If we have recently deregistered miners
        if len(deregistered_hotkeys) > 0:
            bt.logging.info(
                f"| {current_thread} | 🚨 Found {len(deregistered_hotkeys)} deregistered hotkeys. Cleaning out their data.")
            # For all deregistered miners, clear out their predictions & scores. Remove from active_miners table
            tuples = [(x,) for x in deregistered_hotkeys]
            with self.database_manager.lock:
                # Drop predictions tables for deregistered miners
                for hotkey in deregistered_hotkeys:
                    table_name = build_miner_predictions_table_name(hotkey)
                    self.database_manager.query_and_commit(f"DROP TABLE IF EXISTS '{table_name}'")
                self.database_manager.query_and_commit_many("DELETE FROM miner_scores WHERE miner_hotkey = ?", tuples)
                self.database_manager.query_and_commit_many("DELETE FROM active_miners WHERE miner_hotkey = ?", tuples)
                self.database_manager.query_and_commit_many("DELETE FROM daily_scores WHERE miner_hotkey = ?", tuples)
                self.database_manager.query_and_commit_many("DELETE FROM scored_predictions WHERE miner_hotkey = ?", tuples)

        bt.logging.info(f"| {current_thread} | Thread terminating")
