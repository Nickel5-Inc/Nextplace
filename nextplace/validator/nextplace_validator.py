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
from template.base.validator import BaseValidatorNeuron
import threading


class RealEstateValidator(BaseValidatorNeuron):
    def __init__(self, config=None):
        super(RealEstateValidator, self).__init__(config=config)
        self.subtensor = bt.subtensor(config=self.config)
        self.markets = real_estate_markets
        self.database_manager = DatabaseManager()
        self.table_initializer = TableInitializer(self.database_manager)
        self.table_initializer.create_tables()  # Create database tables
        self.market_manager = MarketManager(self.database_manager, self.markets)
        self.scorer = Scorer(self.database_manager, self.markets)
        self.synapse_manager = SynapseManager(self.database_manager)
        self.prediction_manager = PredictionManager(self.database_manager, self.metagraph)
        self.netuid = self.config.netuid
        
        self.weight_setter = WeightSetter(
            metagraph=self.metagraph,
            wallet=self.wallet,
            subtensor=self.subtensor,
            config=config,
            database_manager=self.database_manager
        )

    def sync_metagraph(self):
        """Sync the metagraph with the latest state from the network"""
        bt.logging.info("🔗 Syncing metagraph")
        self.metagraph.sync(subtensor=self.subtensor)
        bt.logging.trace(f"📈 Metagraph has {len(self.metagraph.hotkeys)} hotkeys")

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
            stored_hotkeys = set(self.database_manager.query("SELECT * FROM active_miners"))  # Get stored hotkeys

        # Set operations
        deregistered_hotkeys = list(stored_hotkeys.difference(metagraph_hotkeys))  # Deregistered hotkeys are stored, but not in the metagraph
        new_hotkeys = list(metagraph_hotkeys.difference(stored_hotkeys))  # New hotkeys are in the metagraph, but not stored

        # If we have recently deregistered miners
        if len(deregistered_hotkeys) > 0:
            bt.logging.trace(f"| {current_thread} | 🚨 Found {len(deregistered_hotkeys)} deregistered hotkeys. Cleaning out their data.")
            # For all deregistered miners, clear out their predictions & scores. Remove from active_miners table
            tuples = [(x,) for x in deregistered_hotkeys]
            with self.database_manager.lock:
                self.database_manager.query_and_commit_many("DELETE FROM predictions WHERE miner_hotkey = ?", tuples)
                self.database_manager.query_and_commit_many("DELETE FROM miner_scores WHERE miner_hotkey = ?", tuples)
                self.database_manager.query_and_commit_many("DELETE FROM active_miners WHERE miner_hotkey = ?", tuples)

        # If we have recently registered miners
        if len(new_hotkeys) > 0:
            bt.logging.trace(f"| {current_thread} | ♻️ Found {len(new_hotkeys)} newly registered hotkeys. Tracking.")
            # Add newly registered miners to active_miners table
            tuples = [(x,) for x in new_hotkeys]
            with self.database_manager.lock:
                self.database_manager.query_and_commit_many("INSERT OR IGNORE INTO active_miners (miner_hotkey) VALUES (?)", tuples)

    def check_timer_set_weights(self) -> None:
        """
        Check weight setting timer. If time to set weights, try to acquire lock and set weights
        Returns:
            None
        """
        if self.weight_setter.is_time_to_set_weights():
            if not self.database_manager.lock.acquire(blocking=True, timeout=10):
                # If the lock is held by another thread, wait for 10 seconds, if still not available, return
                bt.logging.trace("🚧 Another thread is holding the database_manager lock. Will check timer and set weights later.")
                return
            try:
                self.weight_setter.check_timer_set_weights()
            finally:
                self.database_manager.lock.release()

    # OVERRIDE | Required
    def forward(self, step: int) -> None:
        """
        Forward pass
        Returns:
            None
        """
        bt.logging.info("⏩ Running forward pass")

        # Need database lock to handle synapse creation and prediction management
        if not self.database_manager.lock.acquire(blocking=True, timeout=10):
            # If the lock is held by another thread, wait for 10 seconds, if still not available, return
            bt.logging.trace("🚧 Another thread is holding the database_manager lock.")
            return

        try:

            # Need market lock to maintain market manager state safely
            if not self.market_manager.lock.acquire(blocking=True, timeout=10):
                # If the lock is held by another thread, wait for 10 seconds, if still not available, return
                bt.logging.trace("🚧 Another thread is holding the market_manager lock.")
                return

            try:
                # If we don't have any properties AND we aren't getting them yet, start thread to get properties
                number_of_properties = self.database_manager.get_size_of_table('properties')
                if number_of_properties == 0 and not self.market_manager.updating_properties:
                    self.market_manager.updating_properties = True  # Set flag
                    thread = threading.Thread(target=self.market_manager.get_properties_for_market, name="🏠 PropertiesThread 🏠")  # Create thread
                    thread.start()  # Start thread
                    return

                elif number_of_properties == 0:
                    bt.logging.info("🚧 Waiting for properties thread to populate properties table")
                    return

            finally:
                self.market_manager.lock.release()  # Always release the lock

            synapse: RealEstateSynapse = self.synapse_manager.get_synapse()  # Prepare data for miners
            if synapse is None or len(synapse.real_estate_predictions.predictions) == 0:
                bt.logging.trace("↻ No data for Synapse, returning.")
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
