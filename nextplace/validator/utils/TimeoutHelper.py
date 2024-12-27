import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import bittensor as bt
import traceback


def run_with_timeout(func, timeout=180, *args, **kwargs):
    """
    Runs a function with a timeout. If the function doesn't complete within the specified timeout, it returns None.

    Args:
        func (callable): The function to run.
        timeout (int): Timeout in seconds (default is 180).
        args: Positional arguments for the function.
        kwargs: Keyword arguments for the function.

    Returns:
        Any: The result of the function, or None if the function timed out.
    """
    if func is None:
        return None
    current_thread = threading.current_thread().name
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(func, *args, **kwargs)  # Run the function in a separate thread
            try:
                return future.result(timeout=timeout)  # Wait for the result up to the timeout
            except TimeoutError:
                func_name = getattr(func, "__name__", "<unknown>")
                bt.logging.trace(f"| {current_thread} | ❗ Function '{func_name}' timed out")
                return None  # Return None if the function times out
    except Exception as e:
        bt.logging.error(f"| {current_thread} | ❗ Caught exception for function {func} during timeout {e}, {traceback.format_exc()}")