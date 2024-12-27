import signal
import threading

import bittensor as bt


# Exception for timeouts
class TimeoutException(Exception):
    pass


# Timeout handler
def timeout_handler(signum, frame):
    raise TimeoutException("Function execution exceeded the timeout limit.")


def run_with_timeout(func, *args, timeout=180, **kwargs):
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
    current_thread = threading.current_thread().name
    if func is None:
        raise ValueError(f"| {current_thread} | ❗Function to run cannot be None.")
    bt.logging.trace(f"| {current_thread} | 🏃 Running function with timeout: {func.__name__}")
    # Set the timeout handler
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)  # Set the alarm

    try:
        result = func(*args, **kwargs)  # Run the function
        signal.alarm(0)  # Disable the alarm
        return result
    except TimeoutException:
        return None  # Return None if the function timed out
