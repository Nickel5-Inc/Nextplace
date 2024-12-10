import threading
from datetime import datetime, timezone
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
        current_thread = threading.current_thread().name
        num_markets = len(self.markets)
        bt.logging.trace(f"| {current_thread} | 🕵🏻 Looking for recently sold homes'")
        for idx, market in enumerate(self.markets):
            bt.logging.trace(f"| {current_thread} | 🔍 Getting sold homes in {market['name']}")
            self._process_region_sold_homes(market)
            percent_done = round(((idx + 1) / num_markets) * 100, 2)
            bt.logging.trace(f"| {current_thread} | {percent_done}% of markets processed")

    def _process_region_sold_homes(self, market: dict) -> None:
        """
        Iteratively hit API for sold homes in market, store valid homes in memory, ingest
        Args:
            market: current market
            oldest_prediction: timestamp of our oldest unscored prediction

        Returns:
            None
        """
        current_thread = threading.current_thread().name
        region_id = market['id']
        url_sold = "https://redfin-com-data.p.rapidapi.com/properties/search-sold"  # URL for sold houses
        page = 1  # Page number for api results

        invalid_results = {'date': 0, 'price': 0}
        valid_results = []
        # Iteratively call the API until we have no more results to read
        while True:

            # Build the query string for this page
            querystring = {
                "regionId": region_id,
                "soldWithin": 21,
                "limit": self.max_results_per_page,
                "page": page
            }

            response = requests.get(url_sold, headers=self.headers, params=querystring)  # Get API response

            # Only proceed with status code is 200
            if response.status_code != 200:
                current_thread = threading.current_thread().name
                bt.logging.error(f"| {current_thread} | ❗Error querying sold properties: {response.status_code}")
                bt.logging.error(response.text)
                break

            data = response.json()  # Get response body
            homes = data.get('data', [])  # Extract data

            if not homes:  # No more results
                break

            # Iterate all homes
            for home in homes:
                self._process_home(home, valid_results, invalid_results)

            if len(homes) < self.max_results_per_page:  # Last page
                break

            page += 1  # Increment page

        bt.logging.trace(f"| {current_thread} | 📣 Found {invalid_results['date']} homes with invalid dates and {invalid_results['price']} homes with invalid prices")
        self._ingest_valid_homes(valid_results)

    def _process_home(self, home: any, result_tuples: list[tuple], invalid_results: dict[str, int]) -> None:
        home_data = home['homeData']
        property_id = home_data.get('propertyId')  # Extract property id
        sale_price = self._get_nested(home_data, 'priceInfo', 'amount')  # Extract sale price
        sale_date = self._get_nested(home_data, 'lastSaleData', 'lastSoldDate')  # Extract the sale date
        address = self._get_nested(home_data, 'addressInfo', 'formattedStreetLine')
        zip_code = self._get_nested(home_data, 'addressInfo', 'zip')
        nextplace_id = self.get_hash(address, zip_code)
        if address and zip_code and property_id and sale_price and sale_date:
            sale_date_obj = datetime.strptime(sale_date, "%Y-%m-%dT%H:%M:%SZ")
            sale_date_obj = sale_date_obj.replace(tzinfo=timezone.utc)
            sale_date_utc = sale_date_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
            utc_date_only = sale_date_obj.date()
            today = datetime.utcnow().date()
            if sale_price == 0:  # If sale price is 0, ignore
                invalid_results['price'] += 1
                return
            if utc_date_only > today:  # If sale date is in the future, ignore
                invalid_results['date'] += 1
                return
            result_tuples.append((nextplace_id, property_id, sale_price, sale_date_utc))

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
