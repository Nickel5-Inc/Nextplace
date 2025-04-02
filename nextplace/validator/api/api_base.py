from abc import ABC
import os
from dotenv import load_dotenv
import hmac
import hashlib
from nextplace.validator.database.database_manager import DatabaseManager

"""
Abstract base class contains data global to all API calls
"""
class ApiBase(ABC):
    def __init__(self, database_manager: DatabaseManager, markets: list[dict[str, str]]):
        self.nextplace_hash_key = b'next_place_hash_key_3b1f2aebc9d8e456'  # For creating the nextplace_id
        self.database_manager = database_manager
        self.markets = markets
        
        # Load API keys from environment
        load_dotenv()
        self.us_api_key = os.getenv("NEXT_PLACE_REDFIN_API_KEY")
        self.canada_api_key = os.getenv("NEXTPLACE_CANADA_API_KEY")
        
        # Default US headers
        self.headers = {
            "X-RapidAPI-Key": self.us_api_key,
            "X-RapidAPI-Host": "redfin-com-data.p.rapidapi.com"
        }
        
        # Canadian headers 
        self.canada_headers = {
            "X-RapidAPI-Key": self.canada_api_key,
            "X-RapidAPI-Host": "redfin-canada.p.rapidapi.com"
        }
        
        self.max_results_per_page = 350  # This is typically the maximum allowed by Redfin's API
        
    def get_hash(self, address: str, zip_code: str) -> str:
        """
        Build the nextplace_id using a 1-way cryptographic hash function
        Args:
            address: the home's street address
            zip_code: the home's zip code
            
        Returns:
            the cryptographic hash of the address-zip
        """
        message = f"{address}-{zip_code}"
        hashed = hmac.new(self.nextplace_hash_key, message.encode(), hashlib.sha256)
        return hashed.hexdigest()
    
    def get_headers(self, market_id: str) -> dict:
        """
        Get the appropriate headers based on market ID
        Args:
            market_id: The market identifier
            
        Returns:
            The appropriate headers for the API request
        """
        if market_id.startswith('33'):
            return self.canada_headers
        return self.headers
    
    def get_api_url(self, endpoint: str, market_id: str) -> str:
        """
        Get the appropriate API URL based on market ID
        Args:
            endpoint: The API endpoint (e.g., 'search-sale', 'search-sold')
            market_id: The market identifier
            
        Returns:
            The complete API URL
        """
        base_url = "https://redfin-canada.p.rapidapi.com/properties/" if market_id.startswith('33') else "https://redfin-com-data.p.rapidapi.com/properties/"
        return f"{base_url}{endpoint}"
    
    def _get_nested(self, data: dict, *args: str) -> dict or None:
        """
        Extract nested values from a dictionary
        Args:
            data: the dictionary
            *args:
            
        Returns:
            A dictionary or None
        """
        for arg in args:
            if isinstance(data, dict) and arg in data:
                data = data[arg]
            else:
                return None
        return data
        