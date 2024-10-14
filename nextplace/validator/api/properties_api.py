import json
import threading

import requests
import bittensor as bt
from datetime import datetime, timezone
from nextplace.validator.api.api_base import ApiBase
from nextplace.validator.data_containers.home import Home
from nextplace.validator.database.database_manager import DatabaseManager
from nextplace.validator.utils.contants import ISO8601

"""
Helper class to get currently listed homes (properties) on the market
"""


class PropertiesAPI(ApiBase):

    def __init__(self, database_manager: DatabaseManager, markets: list[dict[str, str]]):
        super(PropertiesAPI, self).__init__(database_manager, markets)

    def process_region_market(self, market: dict[str, str]) -> None:
        """
        Process a specific region's housing market data. Ingest 1 page at a time.
        Args:
            market: the current market

        Returns:
            None
        """
        url_for_sale = "https://redfin-com-data.p.rapidapi.com/properties/search-sale"  # Redfin URL
        page = 1  # Page number for api results

        while True:

            # Build query string
            querystring = {
                "regionId": market['id'],
                "limit": self.max_results_per_page,
                "page": page
            }
            response = requests.get(url_for_sale, headers=self.headers, params=querystring)  # Hit the API

            # Only proceed with status code is 200
            if response.status_code != 200:
                current_thread = threading.currentThread().name
                bt.logging.error(f"| {current_thread} | ❗Error querying properties on the market: {response.status_code}")
                bt.logging.error(response.text)
                break

            data = json.loads(response.text)  # Load the result
            homes = data.get('data', [])  # Extract data

            if not homes:
                break

            self._ingest_properties(homes, market['name'])

            if len(homes) < self.max_results_per_page:  # Last page
                break

            page += 1

    def _ingest_properties(self, homes: list, market: str) -> None:
        """
        Ingest all valid results into the `properties` table
        Args:
            homes: list of valid properties on market
            market: the current market

        Returns:
            None
        """
        query_str = """
            INSERT OR IGNORE INTO properties (
                nextplace_id, property_id, listing_id, address, city, state, zip_code, price, beds, baths,
                sqft, lot_size, year_built, days_on_market, latitude, longitude,
                property_type, last_sale_date, hoa_dues, query_date, market
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        values = []
        for home in homes:
            self._process_home_for_ingestion(home, market, values)  # Conditionally add home to the `values` list
        with self.database_manager.lock:  # Acquire database lock
            self.database_manager.query_and_commit_many(query_str, values)  # Update properties table

    def _process_home_for_ingestion(self, home: any, market_name: str, values: list) -> None:
        """
        Check if `price` is present in data. If so, build tuple and add to list of values for ingestion
        Args:
            home: home object
            market_name: current market
            values: list of tuples for ingestion

        Returns:
            None. `values` are updated by reference
        """
        home_data = home['homeData']  # Extract the homeData field
        home_object = self._build_property_object(home_data)  # Build the Home object

        if home_object['price'] is not None:
            query_date = datetime.now(timezone.utc).strftime(ISO8601)  # Get current datetime
            data_tuple = (
                home_object['nextplace_id'], home_object['property_id'], home_object['listing_id'],
                home_object['address'], home_object['city'], home_object['state'], home_object['zip_code'],
                home_object['price'], home_object['beds'], home_object['baths'], home_object['sqft'],
                home_object['lot_size'], home_object['year_built'], home_object['days_on_market'],
                home_object['latitude'], home_object['longitude'], home_object['property_type'],
                home_object['last_sale_date'], home_object['hoa_dues'], query_date, market_name
            )
            values.append(data_tuple)

    def _build_property_object(self, home_data: any) -> Home:
        """
        Build a property object from an API response
        Args:
            home_data: data returned by the redfin api

        Returns:
            A Home object
        """
        address = self._get_nested(home_data, 'addressInfo', 'formattedStreetLine')
        zip_code = self._get_nested(home_data, 'addressInfo', 'zip')
        nextplace_id = self.get_hash(address, zip_code)
        return {
            'nextplace_id': nextplace_id,
            'property_id': home_data.get('propertyId'),
            'listing_id': home_data.get('listingId'),
            'address': address,
            'city': self._get_nested(home_data, 'addressInfo', 'city'),
            'state': self._get_nested(home_data, 'addressInfo', 'state'),
            'zip_code': zip_code,
            'price': self._get_nested(home_data, 'priceInfo', 'amount'),
            'beds': home_data.get('beds'),
            'baths': home_data.get('baths'),
            'sqft': self._get_nested(home_data, 'sqftInfo', 'amount'),
            'lot_size': self._get_nested(home_data, 'lotSize', 'amount'),
            'year_built': self._get_nested(home_data, 'yearBuilt', 'yearBuilt'),
            'days_on_market': self._get_nested(home_data, 'daysOnMarket', 'daysOnMarket'),
            'latitude': self._get_nested(home_data, 'addressInfo', 'centroid', 'centroid', 'latitude'),
            'longitude': self._get_nested(home_data, 'addressInfo', 'centroid', 'centroid', 'longitude'),
            'property_type': home_data.get('propertyType'),
            'last_sale_date': self._get_nested(home_data, 'lastSaleData', 'lastSoldDate'),
            'hoa_dues': self._get_nested(home_data, 'hoaDues', 'amount'),
        }
