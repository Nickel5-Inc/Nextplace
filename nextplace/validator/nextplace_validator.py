import time
import bittensor as bt
from nextplace.protocol import RealEstateSynapse
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.database.table_initializer import TableInitializer
from nextplace.validator.market.market_manager import MarketManager
from nextplace.validator.market.markets import real_estate_markets
from nextplace.validator.predictions.prediction_manager import PredictionManager
from nextplace.validator.scoring.scoring import Scorer
from nextplace.validator.synapse.synapse_manager import SynapseManager
from nextplace.validator.setting_weights.weights import WeightSetter
from nextplace.validator.utils.contants import build_miner_predictions_table_name
from template.base.validator import BaseValidatorNeuron
import threading
import requests


class RealEstateValidator(BaseValidatorNeuron):
    def __init__(self, config=None):
        super(RealEstateValidator, self).__init__(config=config)
        self.subtensor = bt.subtensor(config=self.config)
        self.markets = real_estate_markets
        self.database_manager = DatabaseManager()
        self.table_initializer = TableInitializer(self.database_manager)
        self.table_initializer.create_tables()  # Create database tables
        self.market_manager = MarketManager(self.database_manager, self.markets)
        self.scorer = Scorer(self.database_manager, self.markets, self.metagraph)
        self.synapse_manager = SynapseManager(self.database_manager)
        self.prediction_manager = PredictionManager(self.database_manager, self.metagraph)
        self.netuid = self.config.netuid
        self.should_step = True
        self.current_thread = threading.current_thread().name

        self.weight_setter = WeightSetter(
            metagraph=self.metagraph,
            wallet=self.wallet,
            subtensor=self.subtensor,
            config=config,
            database_manager=self.database_manager
        )

    def sync_metagraph(self):
        """Sync the metagraph with the latest state from the network"""
        bt.logging.info(f"| {self.current_thread} | 🔗 Syncing metagraph")
        self.metagraph.sync(subtensor=self.subtensor)
        bt.logging.trace(f"| {self.current_thread} | 📈 Metagraph has {len(self.metagraph.hotkeys)} hotkeys")

    def manage_miner_data(self) -> None:
        """
        RUN IN THREAD
        Remove miner data if miner has deregistered
        Store hotkey if miner has registered
        Returns:
            None
        """
        current_thread = threading.current_thread().name

        # Build sets
        metagraph_hotkeys = set(self.metagraph.hotkeys)  # Get hotkeys in metagraph
        with self.database_manager.lock:
            stored_hotkeys = set(row[0] for row in self.database_manager.query("SELECT miner_hotkey FROM active_miners"))  # Get stored hotkeys

        bt.logging.trace(f"| {current_thread} | Managing active miners. Found {len(stored_hotkeys)} tracked miners and {len(metagraph_hotkeys)} metagraph hotkeys")

        # Set operation
        deregistered_hotkeys = list(stored_hotkeys.difference(metagraph_hotkeys))  # Deregistered hotkeys are stored, but not in the metagraph

        # If we have recently deregistered miners
        if len(deregistered_hotkeys) > 0:
            bt.logging.trace(f"| {current_thread} | 🚨 Found {len(deregistered_hotkeys)} deregistered hotkeys. Cleaning out their data.")
            # For all deregistered miners, clear out their predictions & scores. Remove from active_miners table
            tuples = [(x,) for x in deregistered_hotkeys]
            with self.database_manager.lock:
                # Drop predictions tables for deregistered miners
                for hotkey in deregistered_hotkeys:
                    table_name = build_miner_predictions_table_name(hotkey)
                    self.database_manager.query_and_commit(f"DROP TABLE IF EXISTS '{table_name}'")
                self.database_manager.query_and_commit_many("DELETE FROM miner_scores WHERE miner_hotkey = ?", tuples)
                self.database_manager.query_and_commit_many("DELETE FROM active_miners WHERE miner_hotkey = ?", tuples)

        bt.logging.trace(f"| {current_thread} | Thread terminating")

    def send_miner_scores_to_website(self) -> None:
        """
        RUN IN THREAD
        Send miner scores to website
        Returns:
            None
        """
        current_thread = threading.current_thread().name
        with self.database_manager.lock:
            miner_scores = self.database_manager.query("SELECT miner_hotkey, lifetime_score, total_predictions, last_update_timestamp FROM miner_scores")
        if len(miner_scores) == 0:
            bt.logging.info(f"| {current_thread} | 🔔 No miner scores to send to website")

        # ToDo Update obj
        data_to_send = [ { "hotkey": x[0], "score": x[1], "num_predictions": x[2], "last_update_timestamp": x[3] } for x in miner_scores ]

        bt.logging.info(f"| {current_thread} | ⛵ Sending {len(miner_scores)} miner scores to website")
        headers = {
            'Accept': '*/*',
            'Content-Type': 'application/json'
        }

        try:
            # ToDo Update endpoint
            response = requests.post(
                "https://dev-nextplace-api.azurewebsites.net/Predictions",
                json=data_to_send,
                headers=headers
            )
            response.raise_for_status()
            bt.logging.info(f"| {current_thread} | ✅ Data sent to Nextplace site successfully.")

        except requests.exceptions.HTTPError as e:
            bt.logging.warning(f"| {current_thread} | ❗ HTTP error occurred: {e}. No data was sent to the Nextplace site.")
            if e.response is not None:
                bt.logging.warning(
                    f"| {current_thread} | ❗ Error sending data to site. Response content: {e.response.text}")
        except requests.exceptions.RequestException as e:
            bt.logging.warning(
                f"| {current_thread} | ❗ Error sending data to site. An error occurred while sending data: {e}. No data was sent to the Nextplace site.")


    def print_total_number_of_predictions(self) -> None:
        """
        RUN IN THREAD
        Prints total number of predictions across all miners
        Returns:
            None
        """
        current_thread = threading.current_thread().name
        all_table_query = "SELECT name FROM sqlite_master WHERE type='table'"
        all_tables = [x[0] for x in self.database_manager.query(all_table_query)]  # Get all tables in database
        predictions_tables = [s for s in all_tables if s.startswith("predictions_")]
        count = 0
        for predictions_table in predictions_tables:
            with self.database_manager.lock:
                count += self.database_manager.get_size_of_table(predictions_table)
        bt.logging.info(f"| {current_thread} | ✨ Database currently has {count} predictions")

    def check_timer_set_weights(self) -> None:
        """
        Check weight setting timer. If time to set weights, try to acquire lock and set weights
        Returns:
            None
        """
        if self.weight_setter.is_time_to_set_weights():
            if not self.database_manager.lock.acquire(blocking=True, timeout=10):
                # If the lock is held by another thread, wait for 10 seconds, if still not available, return
                bt.logging.trace(f"| {self.current_thread} | 🍃 Another thread is holding the database_manager lock. Will check timer and set weights later. This is expected behavior 😊.")
                return
            try:
                self.weight_setter.check_timer_set_weights()
            finally:
                self.database_manager.lock.release()

    def is_thread_running(self, thread_name: str):
        for thread in threading.enumerate():  # Get a list of all active threads
            if thread.name == thread_name:
                return True
        return False

    # OVERRIDE | Required
    def forward(self, step: int) -> None:
        """
        Forward pass
        Returns:
            None
        """
        bt.logging.info(f"| {self.current_thread} | ⏩ Running forward pass")

        # Need database lock to handle synapse creation and prediction management
        if not self.database_manager.lock.acquire(blocking=True, timeout=10):
            # If the lock is held by another thread, wait for 10 seconds, if still not available, return
            bt.logging.trace(f"| {self.current_thread} | 🍃 Another thread is holding the database_manager lock, waiting for that thread to complete. This is expected behavior 😊.")
            self.should_step = False
            time.sleep(10)
            return

        try:

            # Need market lock to maintain market manager state safely
            if not self.market_manager.lock.acquire(blocking=True, timeout=10):
                # If the lock is held by another thread, wait for 10 seconds, if still not available, return
                bt.logging.trace(f"| {self.current_thread} | 🍃 Another thread is holding the market_manager lock, waiting for that thread to complete. This is expected behavior 😊.")
                self.should_step = False
                time.sleep(10)
                return

            try:
                # If we don't have any properties AND we aren't getting them yet, start thread to get properties
                number_of_properties = self.database_manager.get_size_of_table('properties')
                properties_thread_name = "🏠 PropertiesThread 🏠"
                properties_thread_is_running = self.is_thread_running(properties_thread_name)
                if number_of_properties == 0 and not properties_thread_is_running:
                    thread = threading.Thread(target=self.market_manager.get_properties_for_market, name=properties_thread_name)  # Create thread
                    thread.start()  # Start thread
                    return

                elif number_of_properties == 0:
                    bt.logging.info(f"| {self.current_thread} | 🏘️ No properties in the properties table. PropertiesThread should be updating this table.")
                    self.should_step = False
                    return

            finally:
                self.market_manager.lock.release()  # Always release the lock

            synapse: RealEstateSynapse = self.synapse_manager.get_synapse()  # Prepare data for miners
            if synapse is None or len(synapse.real_estate_predictions.predictions) == 0:
                bt.logging.trace(f"| {self.current_thread} | ↻ No data for Synapse, returning.")
                return

            responses = self.dendrite.query(
                axons=self.metagraph.axons,
                synapse=synapse,
                deserialize=True,
                timeout=30
            )

            self.prediction_manager.process_predictions(responses)  # Process Miner predictions

        finally:
            self.database_manager.lock.release()  # Always release the lock
