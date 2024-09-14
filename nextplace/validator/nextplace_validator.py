import bittensor as bt

from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.database.table_initializer import TableInitializer
from nextplace.validator.utils.market_manager import MarketManager
from nextplace.validator.utils.prediction_manager import PredictionManager
from nextplace.validator.scoring.scoring import Scorer
from nextplace.validator.outgoing_data.synapse_manager import SynapseManager
from nextplace.validator.utils.contants import real_estate_markets
from nextplace.validator.utils.weights import WeightSetter

from template.base.validator import BaseValidatorNeuron
import threading
from time import sleep


class RealEstateValidator(BaseValidatorNeuron):
    def __init__(self, config=None):
        super(RealEstateValidator, self).__init__(config=config)
        self.markets = real_estate_markets
        self.database_manager = DatabaseManager()
        self.table_initializer = TableInitializer(self.database_manager)
        self.table_initializer.create_tables()  # Create database tables
        self.market_manager = MarketManager(self.database_manager, self.markets)
        self.scorer = Scorer(self.database_manager, self.markets)
        self.synapse_manager = SynapseManager(self.database_manager, self.market_manager)
        self.prediction_manager = PredictionManager(self.database_manager, self.metagraph)
        self.scorer = Scorer(self.database_manager, self.markets)
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
        bt.logging.info("Syncing metagraph")
        self.metagraph.sync(subtensor=self.subtensor) # TODO: verify that deregistered keys are handled
        bt.logging.trace(f"metagraph: {self.metagraph.hotkeys}")

    def initialize_subtensor(self):
        bt.logging.info("Syncing Subtensor")
        try:
            self.subtensor = bt.subtensor(config=self.config, netuid=self.netuid)
            bt.logging.info(f"Connected to {self.config.subtensor.network} network")
        except Exception as e:
            bt.logging.error(f"Failed to initialize subtensor: {str(e)}")
            self.subtensor = None
        return self.subtensor

    def score(self) -> None:
        """
        RUN IN THREAD
        - Start another thread for calculating Miner scores
        - Wait for it to finish
        - Set weights
        Returns:
            None
        """
        current_thread = threading.current_thread()
        bt.logging.trace(f"| {current_thread.name} | Starting scoring thread")
        thread = threading.Thread(target=self.scorer.run_score_predictions, name="Score Calculator Thread")  # Create thread
        thread.start()  # Start thread
        thread.join()  # Wait for it to finish
        bt.logging.trace(f"| {current_thread.name} | Setting weights")
        self.weight_setter.set_weights()  # Set weights once scoring is done
        bt.logging.trace(f"| {current_thread.name} | Weights set")

    # OVERRIDE | Required
    def forward(self, step: int) -> None:
        """
        Forward pass
        Returns:
            None
        """
        bt.logging.info("Running forward pass")

        # Need market lock to maintain market manager state safely
        if not self.market_manager.lock.acquire(blocking=True, timeout=10):
            # If the lock is held by another thread, wait for 10 seconds, if still not available, return
            bt.logging.trace("Another thread is holding the market_manager lock.")
            return

        # try/finally to avoid deadlock
        try:
            # If we don't have any properties AND we aren't getting them yet, start thread to get properties
            if self.market_manager.number_of_properties_in_market == 0 and not self.market_manager.updating_properties:
                self.market_manager.updating_properties = True  # Set flag
                thread = threading.Thread(target=self.market_manager.get_properties_for_market, name="Properties Thread")  # Create thread
                thread.start()  # Start thread

            # No properties for Miners yet, another thread is updating the properties table
            if self.market_manager.number_of_properties_in_market == 0:
                bt.logging.trace(f"Waiting for other thread to finish updating properties table")
                return

            else:  # We have properties to send to Miners

                # Need database lock to handle synapse creation and prediction management
                if not self.database_manager.lock.acquire(blocking=True, timeout=10):
                    # If the lock is held by another thread, wait for 10 seconds, if still not available, return
                    bt.logging.trace("Another thread is holding the database_manager lock.")
                    return

                # try/finally to avoid deadlock
                try:  # Build synapse, send to miners, parse and store predictions

                    synapse = self.synapse_manager.get_synapse()  # Prepare data for miners
                    bt.logging.info(f"Sending a synapse for properties in {self.market_manager.get_current_market()}")

                    if synapse:  # Ensure synapse object was created, query the network
                        responses = self.dendrite.query(
                            axons=self.metagraph.axons,
                            synapse=synapse,
                            deserialize=True,
                            timeout=30
                        )

                        self.prediction_manager.process_predictions(responses)  # Process Miner predictions

                    else:
                        # Synapse was empty, this suggests an issue with the database state
                        bt.logging.error("No data available to send to miners")

                    self.market_manager.manage_forward()  # Maintain market manager state

                finally:
                    self.database_manager.lock.release()  # Always release the lock

        finally:
            self.market_manager.lock.release()  # Always release the lock
