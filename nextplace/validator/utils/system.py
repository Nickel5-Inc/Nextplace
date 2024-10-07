import functools
import multiprocessing
from bittensor import logging

def timeout_with_multiprocess(seconds):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            def target_func(result_dict, *args, **kwargs):
                try:
                    result_dict["result"] = func(*args, **kwargs)
                except Exception as e:
                    result_dict["exception"] = e

            manager = multiprocessing.Manager()
            result_dict = manager.dict()
            process = multiprocessing.Process(
                target=target_func, args=(result_dict, *args), kwargs=kwargs
            )
            process.start()
            process.join(seconds)

            if process.is_alive():
                process.terminate()
                process.join()
                logging.warning(
                    f"Function '{func.__name__}' timed out after {seconds} seconds"
                )
                return None

            if "exception" in result_dict:
                raise result_dict["exception"]

            return result_dict.get("result", None)

        return wrapper

    return decorator