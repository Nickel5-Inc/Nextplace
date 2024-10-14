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
        self.current_thread = threading.current_thread().name
        initial_market_index = self._find_initial_market_index()
        bt.logging.info(f"| {self.current_thread} | 🏁 Initial market index: {initial_market_index}")
        self.market_index = initial_market_index  # Index into self.markets. The current market

    def _find_initial_market_index(self) -> int:
        """
        Get the initial market index
        Returns:
            The initial market index
        """
        # Get from the properties table
        number_of_properties = self.database_manager.get_size_of_table('properties')
        if number_of_properties > 0:
            return self._find_initial_market_from_properties()

        # Get from the predictions table
        number_of_predictions = self.database_manager.get_size_of_table('predictions')
        if number_of_predictions > 0:
            return self._find_initial_market_from_predictions()

        # Just start at beginning
        return 0

    def _find_initial_market_from_properties(self) -> int:
        """
        Query the properties table and get the market represented there. Then return the next market
        Returns:
            The next market
        """
        # Get any property
        some_property = self.database_manager.query("""
            SELECT market
            FROM properties
            LIMIT 1
        """)
        if some_property:
            market = some_property[0][0]  # Extract market
            idx = next((i for i, obj in enumerate(self.markets) if obj["name"] == market), None)  # Get market index
            return idx + 1 if idx < len(self.markets) - 1 else 0  # Get next market, wrap around if need be
        else:
            number_of_predictions = self.database_manager.get_size_of_table('predictions')  # Check size of predictions
            if number_of_predictions > 0:
                return self._find_initial_market_from_predictions()
        return 0

    def _find_initial_market_from_predictions(self) -> int:
        """
        Get the newest prediction in the predictions table, find that market, then return the next one
        Returns:
            The next market
        """
        most_recent_prediction = self.database_manager.query(
            """
                SELECT market 
                FROM predictions
                ORDER BY prediction_timestamp DESC
                LIMIT 1
            """
        )
        if not most_recent_prediction:
            return 0
        market = most_recent_prediction[0][0]  # Extract market name
        idx = next((i for i, obj in enumerate(self.markets) if obj["name"] == market), None)  # Get index of market
        return idx + 1 if idx < len(self.markets) - 1 else 0  # Return next one, wrap around if need be

    def get_properties_for_market(self) -> None:
        """
        RUN IN THREAD
        Hit the API, update the database
        Returns:
            None
        """
        bt.logging.info(f"| {self.current_thread} | 🔑 No properties were found, getting the next market and updating properties")
        current_market = self.markets[self.market_index]  # Extract market object
        self.properties_api.process_region_market(current_market)  # Populate database with this market
        with self.lock:  # Acquire lock
            bt.logging.info(f"| {self.current_thread} | ✅ Finished ingesting properties in {current_market['name']}")
            self.market_index = self.market_index + 1 if self.market_index < len(self.markets) - 1 else 0 # Wrap index around
            self.updating_properties = False  # Update flag

