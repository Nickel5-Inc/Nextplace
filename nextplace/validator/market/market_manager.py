import bittensor as bt
from nextplace.validator.api.properties_api import PropertiesAPI
from nextplace.validator.database.database_manager import DatabaseManager
import threading
from time import sleep

from nextplace.validator.utils.contants import SYNAPSE_TIMEOUT, NUMBER_OF_PROPERTIES_PER_SYNAPSE

"""
Helper class manages the real estate market
"""


class MarketManager:

    def __init__(self, database_manager: DatabaseManager, markets: list[dict[str, str]]):
        self.database_manager = database_manager
        self.markets = markets
        self.properties_api = PropertiesAPI(database_manager, markets)

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
            ORDER BY query_date
            DESC
            LIMIT 1
        """)
        if some_property:
            market = some_property[0][0]  # Extract market
            idx = next((i for i, obj in enumerate(self.markets) if obj["name"] == market), None)  # Get market index
            return idx + 1 if idx < len(self.markets) - 1 else 0  # Get next market, wrap around if need be
        return 0


    def ingest_properties(self) -> None:
        """
        RUN IN THREAD
        Populate the properties table with properties
        Returns:
            None
        """
        current_thread = threading.current_thread().name  # Get thread name
        market_index = self._find_initial_market_index()
        
        # Keep at least (x) synapses worth of properties in the table at all times
        number_of_synapses = 5
        min_properties_table_size = NUMBER_OF_PROPERTIES_PER_SYNAPSE * number_of_synapses
        
        while True:
            
            # Get size of properties table
            with self.database_manager.lock:
                size_of_properties_table = self.database_manager.get_size_of_table('properties')

            bt.logging.info(f"| {current_thread} | {size_of_properties_table} items in property table")
                
            # If size is less than our min, get more properties
            if size_of_properties_table < min_properties_table_size:
                current_market = self.markets[market_index]  # Extract market object
                self.properties_api.process_region_market(current_market)  # Populate database with this market
                bt.logging.info(f"| {current_thread} | ✅ Finished ingesting properties in {current_market['name']}")
                market_index = market_index + 1 if market_index < len(self.markets) - 1 else 0  # Wrap index around
                
            # If we're still good on size, just sleep until another synapse goes out
            else:
                sleep(SYNAPSE_TIMEOUT)
