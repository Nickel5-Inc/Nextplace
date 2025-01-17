import threading
import bittensor as bt
from nextplace.validator.website_data.website_communicator import WebsiteCommunicator
import queue
from time import sleep

MAX_BATCH_SIZE = 2500


class ActivePredictionSender:
    def __init__(self, data_queue: queue.LifoQueue):
        self.website_communicator = WebsiteCommunicator('Predictions')
        self.data_queue = data_queue
        self.running = True  # Flag for graceful shutdown

    def run(self):
        current_thread = threading.current_thread().name

        while self.running:  # Infinite loop

            batch = []  # Current batch

            try:
                for _ in range(MAX_BATCH_SIZE):  # Iterate
                    item = self.data_queue.get(timeout=5)  # Timeout prevents indefinite blocking
                    batch.append(item)  # Put item into the batch
                    self.data_queue.task_done()  # Mark item as processed

                # Send the batch after collecting enough items
                self.website_communicator.send_data(batch)

            except queue.Empty:
                bt.logging.trace(f"| {current_thread} | Queue was found empty, waiting...")
                sleep(5)  # Wait for items to populate the queue
                continue

    def stop(self):
        """Gracefully stop the sender."""
        self.running = False