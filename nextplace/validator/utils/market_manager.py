import bittensor as bt
from nextplace.validator.api.properties_api import PropertiesAPI
from nextplace.validator.database.database_manager import DatabaseManager
import threading

from nextplace.validator.utils.contants import NUMBER_OF_PROPERTIES_PER_SYNAPSE

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
        self.property_index = 0  # The current property being sent to Miners
        number_of_properties_in_database = self.database_manager.get_size_of_table('properties')
        self.number_of_properties_in_market = number_of_properties_in_database  # The number of properties associated with the current market

    def _find_initial_market_index(self):
        market = self._find_initial_market_name()
        bt.logging.trace(f"Initial market: {market}")
        if market is None:
            return 0
        idx = next((i for i, obj in enumerate(self.markets) if obj["name"] == market), None)
        if idx is None:
            return 0
        return idx

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

    def get_current_market(self):
        market = self.markets[self.market_index]
        return market['name']

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
        with self.database_manager.lock:
            rows_in_properties = self.database_manager.get_size_of_table('properties')  # Properties in table
        with self.lock:  # Acquire lock
            self.number_of_properties_in_market = rows_in_properties
            bt.logging.info(f"| {current_thread.name} | {current_market['name']} real estate market has {self.number_of_properties_in_market} properties ")
            self.updating_properties = False  # Update flag

    def manage_forward(self) -> None:
        """
        Manage the market after a forward pass. self.lock and database_manager.lock are already acquired here
        Returns:
            None
        """
        self.property_index += NUMBER_OF_PROPERTIES_PER_SYNAPSE  # Maintain properties index

        # We still have properties we can send
        if self.property_index < self.number_of_properties_in_market - 1:
            return

        # We're updating the properties, so do nothing
        if self.updating_properties:  # Called from forward(), so main thread already has this object lock
            bt.logging.trace(f"Updating properties in another thread")
            return

        # We've gotten to the end of the properties table, and we're not updating properties
        self._handle_market_increment()

    def _handle_market_increment(self) -> None:
        """
        Reset properties index, increment (or wrap around) market index, clear out properties table
        Returns:
            None
        """
        bt.logging.info("Resetting market and property indices")
        bt.logging.trace(f"Clearing properties table...")
        self.database_manager.delete_all_properties()  # Clear out the properties table
        bt.logging.trace(f"Cleared out properties table")

        # Reset indices
        self.property_index = 0
        self.number_of_properties_in_market = 0
        self.market_index = self.market_index + 1 if self.market_index < len(self.markets) - 1 else 0 # Wrap index around
        bt.logging.trace(f"Indices reset. market_index is now {self.market_index}")
