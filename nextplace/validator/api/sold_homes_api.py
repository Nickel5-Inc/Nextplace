import threading
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
        self.current_thread = threading.currentThread().name

    def get_sold_properties(self) -> None:
        """
        Query the redfin API for recently sold homes, store the results in the database
        Returns:
            None
        """
        num_markets = len(self.markets)
        with self.database_manager.lock:
            oldest_prediction = self._get_oldest_prediction()
        bt.logging.trace(f"| {self.current_thread} | 🕵🏻 Looking for homes sold since oldest unscored prediction: '{oldest_prediction}'")
        for idx, market in enumerate(self.markets):
            bt.logging.trace(f"| {self.current_thread} | 🔍 Getting sold homes in {market['name']}")
            self._process_region_sold_homes(market, oldest_prediction)
            percent_done = round(((idx + 1) / num_markets) * 100, 2)
            bt.logging.trace(f"| {self.current_thread} | {percent_done}% of markets processed")

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
                bt.logging.error(f"| {self.current_thread} | ❗Error querying sold properties: {response.status_code}")
                bt.logging.error(response.text)
                break

            data = response.json()  # Get response body
            homes = data.get('data', [])  # Extract data

            if not homes:  # No more results
                break

            # Iterate all homes
            for home in homes:
                self._process_home(home, oldest_prediction, valid_results)

            if len(homes) < self.max_results_per_page:  # Last page
                break

            page += 1  # Increment page

        self._ingest_valid_homes(valid_results)

    def _process_home(self, home: any, oldest_prediction: str, result_tuples: list[tuple]) -> None:
        home_data = home['homeData']
        property_id = home_data.get('propertyId')  # Extract property id
        sale_price = self._get_nested(home_data, 'priceInfo', 'amount')  # Extract sale price
        sale_date = self._get_nested(home_data, 'lastSaleData', 'lastSoldDate')  # Extract the sale date
        address = self._get_nested(home_data, 'addressInfo', 'formattedStreetLine')
        zip_code = self._get_nested(home_data, 'addressInfo', 'zip')
        nextplace_id = self.get_hash(address, zip_code)
        if address and zip_code and property_id and sale_price and sale_date and sale_date > oldest_prediction:
            result_tuples.append((nextplace_id, property_id, sale_price, sale_date))

    def _ingest_valid_homes(self, result_tuples: list[tuple]) -> None:
        """
        Ingest valid results into the database
        Args:
            valid_results: list of valid sold homes

        Returns:
            None
        """
        with self.database_manager.lock:  # Acquire lock
            query_str = """
                INSERT OR IGNORE INTO sales (nextplace_id, property_id, sale_price, sale_date)
                VALUES (?, ?, ?, ?)
            """
            self.database_manager.query_and_commit_many(query_str, result_tuples)

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
