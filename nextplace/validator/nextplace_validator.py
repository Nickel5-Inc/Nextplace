import time
import bittensor as bt
from nextplace.protocol import RealEstateSynapse
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.database.table_initializer import TableInitializer
from nextplace.validator.market.market_manager import MarketManager
from nextplace.validator.market.markets import real_estate_markets
from nextplace.validator.miner_manager.miner_manager import MinerManager
from nextplace.validator.predictions.prediction_manager import PredictionManager
from nextplace.validator.scoring.scoring import Scorer
from nextplace.validator.synapse.synapse_manager import SynapseManager
from nextplace.validator.setting_weights.weights import WeightSetter
from nextplace.validator.utils.contants import SYNAPSE_TIMEOUT
from nextplace.validator.website_data.active_prediction_sender import ActivePredictionSender
from nextplace.validator.website_data.miner_score_sender import MinerScoreSender
from template.base.validator import BaseValidatorNeuron
import threading
import queue

PROPERTIES_THREAD_NAME = "🏠 PropertiesThread 🏠"


class RealEstateValidator(BaseValidatorNeuron):
    def __init__(self, config=None):
        super(RealEstateValidator, self).__init__(config=config)
        self.predictions_queue = queue.LifoQueue()

        self.subtensor = bt.subtensor(config=self.config)
        self.markets = real_estate_markets
        self.database_manager = DatabaseManager()
        self.table_initializer = TableInitializer(self.database_manager)
        self.table_initializer.create_tables()  # Create database tables
        self.market_manager = MarketManager(self.database_manager, self.markets)
        self.scorer = Scorer(self.database_manager, self.markets, self.metagraph)
        self.synapse_manager = SynapseManager(self.database_manager)
        self.prediction_manager = PredictionManager(self.database_manager, self.metagraph, self.predictions_queue)
        self.netuid = self.config.netuid
        self.should_step = True
        self.current_thread = threading.current_thread().name
        self.miner_manager = MinerManager(self.database_manager, self.metagraph)
        self.miner_score_sender = MinerScoreSender(self.database_manager)
        self.prediction_sender = ActivePredictionSender(self.predictions_queue)

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
        bt.logging.info(f"| {self.current_thread} | 📈 Metagraph has {len(self.metagraph.hotkeys)} hotkeys")

    def check_timer_set_weights(self) -> None:
        """
        Check weight setting timer. If time to set weights, try to acquire lock and set weights
        Returns:
            None
        """
        if self.weight_setter.is_time_to_set_weights():
            if not self.database_manager.lock.acquire(blocking=True, timeout=10):
                # If the lock is held by another thread, wait for 10 seconds, if still not available, return
                bt.logging.info(f"| {self.current_thread} | 🍃 Another thread is holding the database_manager lock. Will check timer and set weights later. This is expected behavior 😊.")
                return
            try:
                self.sync_metagraph()
                self.weight_setter.check_timer_set_weights()
            finally:
                self.database_manager.lock.release()

    def is_thread_running(self, thread_name: str):
        for thread in threading.enumerate():  # Get a list of all active threads
            if thread.name == thread_name:
                return True
        return False

    def forward(self, step: int) -> None:
        """
        Forward pass
        Returns:
            None
        """
        bt.logging.info(f"| {self.current_thread} | ⏩ Running forward pass")

        with self.database_manager.lock:
            synapse: RealEstateSynapse or None = self.synapse_manager.get_synapse()  # Prepare data for miners

        if synapse is None or len(synapse.real_estate_predictions.predictions) == 0:  # No data in Properties table yet
            bt.logging.info(f"| {self.current_thread} | ↻ No data in Synapse. Waiting for PropertiesThread to update the Properties table.")
            self.should_step = False
            return

        # Get list of all nextplace IDs in this synapse
        synapse_ids = set([x.nextplace_id for x in synapse.real_estate_predictions.predictions])

        # Query the metagraph
        all_responses = self.dendrite.query(
            axons=self.metagraph.axons,
            synapse=synapse,
            deserialize=True,
            timeout=SYNAPSE_TIMEOUT
        )

        # Handle responses
        self.prediction_manager.process_predictions(all_responses, synapse_ids)
