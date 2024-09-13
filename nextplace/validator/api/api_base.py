from abc import ABC
import os
from dotenv import load_dotenv

from nextplace.validator.database.database_manager import DatabaseManager

"""
Abstract base class contains data global to all API calls
"""


class ApiBase(ABC):

    def __init__(self, database_manager: DatabaseManager, markets: list[dict[str, str]]):
        self.database_manager = database_manager
        self.markets = markets
        api_key = self._get_api_key_from_env()
        self.headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "redfin-com-data.p.rapidapi.com"
        }
        self.max_results_per_page = 350  # This is typically the maximum allowed by Redfin's API

    def _get_api_key_from_env(self) -> str:
        load_dotenv()
        return os.getenv("NEXT_PLACE_REDFIN_API_KEY")

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
