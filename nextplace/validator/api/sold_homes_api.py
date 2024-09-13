from datetime import datetime
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
        num_predictions = self.database_manager.get_size_of_table('predictions')
        if num_predictions == 0:
            bt.logging.trace("Thread to update scores found no predictions, returning...")
            return
        cursor, db_connection = self.database_manager.get_cursor()  # Get a cursor and connection object
        # Iterate regions
        for market in self.markets:
            self._process_region_sold_homes(market, cursor, db_connection)

        cursor.close()  # Close the cursor
        db_connection.close()  # Close the connection to the database

    def _process_region_sold_homes(self, market: dict, cursor: sqlite3.Cursor, db_connection: sqlite3.Connection) -> None:
        bt.logging.trace(f"Getting sold homes in {market['name']}")
        region_id = market['id']
        url_sold = "https://redfin-com-data.p.rapidapi.com/properties/search-sold"  # URL for sold houses
        page = 1  # Page number for api results

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

            bt.logging.trace(f"Found {len(homes)} sold homes on API page #{page} in {market['name']}")

            if not homes:  # No more results
                break

            # Iterate all homes
            for home in homes:
                # Extract home sale date as datetime object
                home_data = home['homeData']
                sale_date = self._get_nested(home_data, 'lastSaleData', 'lastSoldDate')
                if sale_date:
                    self._process_sold_home(home, cursor)

            db_connection.commit()  # Commit to the database

            if len(homes) < self.max_results_per_page:  # Last page
                break

            page += 1  # Increment page

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
