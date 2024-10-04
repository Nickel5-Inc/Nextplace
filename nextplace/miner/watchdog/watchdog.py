import threading
import os
import subprocess
import json
import bittensor as bt
from datetime import datetime, timedelta, timezone
from dateutil import parser
from nextplace.miner.database.database_manager import DatabaseManager


class Watchdog:
    """
    Handles random hanging issues by restarting the PM2 process.
    """
    def __init__(self, database_manager: DatabaseManager):
        self.thread_name = threading.currentThread().name
        self.pm2_process_name = self.get_pm2_process_name()
        self.database_manager = database_manager
        self.timeout = timedelta(minutes=5)

    def get_pm2_process_name(self) -> str or None:
        """
        Extract the name of the PM2 process
        Returns:
            The name of the PM2 process, or None if no PM2 process matching the process ID was found
        """
        try:
            # Fetch the list of PM2 processes in JSON format
            result = subprocess.run(['pm2', 'jlist'], capture_output=True, text=True)
            bt.logging.trace(f"| {self.thread_name} | Result of captured output: {result}")
            bt.logging.trace(f"| {self.thread_name} | stderr: {result.stderr}")
            current_pid = os.getpid()
            bt.logging.trace(f"| {self.thread_name} | PID: {current_pid}")

            if result.returncode != 0:
                bt.logging.error(f"| {self.thread_name} | ❌ PM2 command failed with returncode {result.returncode}")
                return None

            try:
                processes = json.loads(result.stdout)
            except json.JSONDecodeError as json_err:
                bt.logging.error(f"| {self.thread_name} | ❌ Invalid JSON from PM2: {json_err}")
                return None

            # Search for the process that matches the current PID
            for process in processes:
                if process['pid'] == current_pid:
                    bt.logging.trace(f"| {self.thread_name} | 📜 PM2 process name: {process['name']}")
                    return process['name']
            bt.logging.error(f"| {self.thread_name} | ❌ Failed to find the PM2 process name.")
            return None
        except Exception as e:
            bt.logging.error(f"| {self.thread_name} | ❌ Error determining PM2 process name: {str(e)}")
            return None

    def miner_should_restart(self) -> bool:
        """
        Check if the miner needs to restart
        Returns:
            True if it should restart, else False
        """
        bt.logging.info(f"| {self.thread_name} | ⌛ Checking for recent synapse timestamp")
        timestamp = self.database_manager.get_synapse_timestamp()  # Extract stored timestamp for db
        if timestamp is None:  # We haven't stored a synapse yet
            bt.logging.error(f"| {self.thread_name} | ⏰ No timestamp found in synapse table!")
            return True
        timestamp = parser.parse(timestamp)  # Parse the timestamp
        bt.logging.trace(f"| {self.thread_name} | 🕓 Most recent synapse timestamp: {timestamp}")
        now = datetime.now(timezone.utc)  # Get current date time
        time_diff = now - timestamp  # Calculate difference
        return time_diff > self.timeout

    def restart_process(self) -> None:
        """
        Restart the PM2 process
        Returns:
            None
        """
        if not self.pm2_process_name:
            bt.logging.error(f"| {self.thread_name} | ❌ PM2 process name is not set. Cannot restart the process.")
            return

        try:
            # Restart the PM2 process by name
            bt.logging.info(f"| {self.thread_name} | 🔄 Attempting to restart PM2 process: {self.pm2_process_name}")
            subprocess.run(['pm2', 'restart', self.pm2_process_name], check=True)
            bt.logging.info(f"| {self.thread_name} | ✅ PM2 process {self.pm2_process_name} restart initiated")

            # Exit current process to prevent duplication
            os._exit(0)
        except Exception as e:
            bt.logging.error(f"| {self.thread_name} | ❌ Failed to restart PM2 process: {str(e)}")
            os._exit(1)
