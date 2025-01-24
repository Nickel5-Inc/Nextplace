import threading
from nextplace.validator.website_data.website_communicator import WebsiteCommunicator
import queue
from time import sleep
import asyncio

MAX_BATCH_SIZE = 500


class ActivePredictionSender:

    def __init__(self, data_queue: queue.LifoQueue):
        self.website_communicator = WebsiteCommunicator('Predictions')
        self.data_queue = data_queue
        self.running = True  # Flag for graceful shutdown
        self.loop = asyncio.new_event_loop()  # Create a new event loop for async tasks
        self.thread = threading.Thread(target=self.start_event_loop, daemon=True, name="🧶 PredictionSenderEventLoop 🧶")
        self.thread.start()

    def start_event_loop(self):
        """Run the asyncio event loop in a separate thread."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run(self):
        while self.running:  # Infinite loop
            batch = []  # Current batch

            try:
                for _ in range(MAX_BATCH_SIZE):  # Iterate
                    item = self.data_queue.get(timeout=5)  # Timeout prevents indefinite blocking
                    batch.append(item)  # Put item into the batch
                    self.data_queue.task_done()  # Mark item as processed

                # Schedule the async call without blocking
                asyncio.run_coroutine_threadsafe(self.website_communicator.send_data_async(batch), self.loop)

            except queue.Empty:
                sleep(5)  # Wait for items to populate the queue
                continue

    def stop(self):
        """Gracefully stop the sender."""
        self.running = False
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()
