import threading
import sqlite3
import bittensor as bt
import requests
from nextplace.validator.api.api_base import ApiBase
from nextplace.validator.database.database_manager import DatabaseManager

"""
Helper class to get recently sold homes
"""


class SoldHomesAPI(ApiBase):

    def __init__(self, database_manager: DatabaseManager, markets: list[dict[str, str]]):
        super(SoldHomesAPI, self).__init__(database_manager, markets)

    def get_sold_properties(self) -> None:
        """
        Query the redfin API for recently sold homes, store the results in the database
        Returns:
            None
        """
        current_thread = threading.current_thread()
        num_markets = len(self.markets)
        with self.database_manager.lock:
            oldest_prediction = self._get_oldest_prediction()
        bt.logging.trace(f"| {current_thread.name} | Looking for homes sold since oldest unscored prediction: '{oldest_prediction}'")
        for idx, market in enumerate(self.markets):
            bt.logging.trace(f"| {current_thread.name} | Getting sold homes in {market['name']}")
            self._process_region_sold_homes(market, oldest_prediction)
            percent_done = round(((idx + 1) / num_markets) * 100, 2)
            bt.logging.trace(f"| {current_thread.name} | {percent_done}% of markets processed")

    def _process_region_sold_homes(self, market: dict, oldest_prediction: str) -> None:
        """
        Iteratively hit API for sold homes in market, store valid homes in memory, ingest
        Args:
            market: current market
            oldest_prediction: timestamp of our oldest unscored prediction

        Returns:
            None
        """
        region_id = market['id']
        url_sold = "https://redfin-com-data.p.rapidapi.com/properties/search-sold"  # URL for sold houses
        page = 1  # Page number for api results

        valid_results = []
        # Iteratively call the API until we have no more results to read
        while True:

            # Build the query string for this page
            querystring = {
                "regionId": region_id,
                "soldWithin": 31,
                "limit": self.max_results_per_page,
                "page": page
            }

            response = requests.get(url_sold, headers=self.headers, params=querystring)  # Get API response

            # Only proceed with status code is 200
            if response.status_code != 200:
                bt.logging.error(f"Error querying sold properties: {response.status_code}")
                bt.logging.error(response.text)
                break

            data = response.json()  # Get response body
            homes = data.get('data', [])  # Extract data

            if not homes:  # No more results
                break

            # Iterate all homes
            for home in homes:
                # Extract home sale date as datetime object
                home_data = home['homeData']
                sale_date = self._get_nested(home_data, 'lastSaleData', 'lastSoldDate')
                if sale_date and sale_date > oldest_prediction:
                    valid_results.append(home)

            if len(homes) < self.max_results_per_page:  # Last page
                break

            page += 1  # Increment page

        self._ingest_valid_homes(valid_results)


    def _ingest_valid_homes(self, valid_results) -> None:
        """
        Ingest valid results into the database
        Args:
            valid_results: list of valid sold homes

        Returns:
            None
        """
        with self.database_manager.lock:  # Acquire lock
            cursor, db_connection = self.database_manager.get_cursor()  # Get cursor & connection ref
            for home in valid_results:  # Iterate valid homes
                self._process_sold_home(home, cursor)  # Ingest each home
            db_connection.commit()  # Commit db query
            cursor.close()
            db_connection.close()

    def _process_sold_home(self, home: any, cursor: sqlite3.Cursor) -> None:
        """
        Process a single home, store in database
        Args:
            home: a single home object from the redfin api
            cursor: a sqlite3 cursor object

        Returns:
            None

        """
        home_data = home['homeData']  # Extract home data
        property_id = home_data.get('propertyId')  # Extract property id
        sale_price = self._get_nested(home_data, 'priceInfo', 'amount')  # Extract sale price
        sale_date = self._get_nested(home_data, 'lastSaleData', 'lastSoldDate')  # Extract sale date
        address = self._get_nested(home_data, 'addressInfo', 'formattedStreetLine')
        zip_code = self._get_nested(home_data, 'addressInfo', 'zip')
        nextplace_id = self.get_hash(address, zip_code)

        # Store results in database
        if property_id and sale_price and sale_date:
            query = '''
                        INSERT OR IGNORE INTO sales (nextplace_id, property_id, sale_price, sale_date)
                        VALUES (?, ?, ?, ?)
                    '''
            values = (nextplace_id, property_id, sale_price, sale_date)
            cursor.execute(query, values)

    def _get_oldest_prediction(self) -> str:
        """
        Query predictions table for oldest prediction
        Returns:
            the oldest prediction_timestamp
        """
        query = """
            SELECT MIN(prediction_timestamp) 
            FROM predictions 
            WHERE prediction_timestamp IS NOT NULL 
            AND (scored IS FALSE OR scored IS NULL)
        """
        query_result = self.database_manager.query(query)
        return query_result[0][0]
