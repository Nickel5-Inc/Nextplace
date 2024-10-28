import threading
from typing import Any
import requests
import bittensor as bt


class WebsiteCommunicator:

    def __init__(self, endpoint: str):
        api_base = "https://dev-nextplace-api.azurewebsites.net"
        self.endpoint = f"{api_base}/{endpoint}"

    def send_data(self, data: list[dict[str, Any]]) -> None:
        """
        Send data to the nextplace website server
        Args:
            data: list of data objects

        Returns:
            None
        """
        current_thread = threading.current_thread().name
        try:
            response = requests.post(
                self.endpoint,
                json=data,
                headers={
                    'Accept': '*/*',
                    'Content-Type': 'application/json'
                }
            )
            response.raise_for_status()
            bt.logging.info(f"| {current_thread} | ✅ Data sent to Nextplace site successfully.")

        except requests.exceptions.HTTPError as e:
            bt.logging.warning(f"| {current_thread} | ❗ HTTP error occurred: {e}. No data was sent to the Nextplace site.")
            if e.response is not None:
                bt.logging.warning(
                    f"| {current_thread} | ❗ Error sending data to site. Response content: {e.response.text}")
        except requests.exceptions.RequestException as e:
            bt.logging.warning(
                f"| {current_thread} | ❗ Error sending data to site. An error occurred while sending data: {e}. No data was sent to the Nextplace site.")
