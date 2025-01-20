import threading
from typing import Any
import aiohttp
import requests
import bittensor as bt


class WebsiteCommunicator:

    def __init__(self, endpoint: str, suppress_errors: bool = False):
        api_base = "https://dev-nextplace-api.azurewebsites.net"
        self.endpoint = f"{api_base}/{endpoint}"
        self.suppress_errors = suppress_errors
        # self.async_session = aiohttp.ClientSession(headers={'Accept': '*/*', 'Content-Type': 'application/json'})


    def send_data(self, data: list[dict[str, Any]] or dict[str, Any]) -> None:
        """
        Send data to the nextplace website server
        Args:
            data: list of data objects

        Returns:
            None
        """
        current_thread = threading.current_thread().name
        if isinstance(data, list):
            bt.logging.info(f"| {current_thread} | Trying to send {len(data)} datapoints to the web server")

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
            bt.logging.info(f"| {current_thread} | ✅ Data sent to Nextplace web server successfully.")

        except requests.exceptions.HTTPError as e:
            if not self.suppress_errors:
                bt.logging.warning(f"| {current_thread} | ❗ HTTP error occurred: {e}. Data: {data}.")
            if e.response is not None and not self.suppress_errors:
                bt.logging.warning(f"| {current_thread} | ❗ Error sending data to web server. Response content: {e.response.text}")
        except requests.exceptions.RequestException as e:
            if not self.suppress_errors:
                bt.logging.warning(f"| {current_thread} | ❗ Error sending data to web server. An error occurred while sending data: {e}. No data was sent to the Nextplace site.")


    async def send_data_async(self, data: list[dict[str, Any]] or dict[str, Any]) -> None:
        """
        asynchronously sends data to the web server
        Args:
            data:
                dict or list of dicts representing data points
        Returns:
            None
        """
        current_thread = threading.current_thread().name

        if isinstance(data, list):
            bt.logging.info(f"| {current_thread} | Trying to send {len(data)} datapoints to the web server asynchronously.")

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                # async with self.async_session.post(
                        self.endpoint,
                        json=data,
                        headers={
                            'Accept': '*/*',
                            'Content-Type': 'application/json'
                        }
                ) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        bt.logging.info(f"| {current_thread} | ✅ Data sent to Nextplace web server successfully.")
                    else:
                        if not self.suppress_errors:
                            bt.logging.warning(
                                f"| {current_thread} | ❗ Error sending data to web server. Status: {response.status}, Response content: {response_text}")
            except aiohttp.ClientError as e:
                if not self.suppress_errors:
                    bt.logging.warning(
                        f"| {current_thread} | ❗ Error sending data to web server asynchronously. An error occurred: {e}. No data was sent to the Nextplace site.")

    # async def close_async_session(self):
    #     if self.async_session:
    #         await self.async_session.close()
    #
    # async def __aenter__(self):
    #     return self
    #
    # async def __aexit__(self, exc_type, exc, tb):
    #     await self.close_async_session()
