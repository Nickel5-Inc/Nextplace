import bittensor as bt
from nextplace.validator.api.properties_api import PropertiesAPI
from nextplace.validator.database.database_manager import DatabaseManager
import threading

"""
Helper class manages the real estate market
"""


class MarketManager:
    def __init__(self, database_manager: DatabaseManager, markets: list[dict[str, str]]):
        self.database_manager = database_manager
        self.markets = markets
        self.properties_api = PropertiesAPI(database_manager, markets)
        self.lock = threading.RLock()  # Reentrant lock for thread safety
        self.updating_properties = False
        initial_market_index = self._find_initial_market_index()
        bt.logging.info(f"Initial market index: {initial_market_index}")
        self.market_index = initial_market_index  # Index into self.markets. The current market

    def _find_initial_market_index(self):
        market = self._find_initial_market_name()
        if market is None:
            return 0
        idx = next((i for i, obj in enumerate(self.markets) if obj["name"] == market), None)
        if idx is None:
            return 0
        return 0 if idx == len(self.markets) - 1 else idx + 1

    def _find_initial_market_name(self) -> str or None:
        number_of_properties = self.database_manager.get_size_of_table('properties')
        if number_of_properties > 0:
            some_property = self.database_manager.query("""
                        SELECT market
                        FROM properties
                        LIMIT 1
                    """)
            if some_property:
                return some_property[0][0]
        most_recent_prediction = self.database_manager.query(
            """
                SELECT market 
                FROM predictions
                ORDER BY prediction_timestamp DESC
                LIMIT 1
            """
        )
        if not most_recent_prediction:
            return None
        return most_recent_prediction[0][0]

    def get_properties_for_market(self) -> None:
        """
        RUN IN THREAD
        Hit the API, update the database
        Returns:
            None
        """
        current_thread = threading.current_thread()
        bt.logging.info(f"| {current_thread.name} | No properties were found, getting the next market and updating properties")
        current_market = self.markets[self.market_index]  # Extract market object
        self.properties_api.process_region_market(current_market)  # Populate database with this market
        with self.lock:  # Acquire lock
            bt.logging.info(f"| {current_thread.name} | Finished ingesting properties in {current_market['name']}")
            self.market_index = self.market_index + 1 if self.market_index < len(self.markets) - 1 else 0 # Wrap index around
            self.updating_properties = False  # Update flag

