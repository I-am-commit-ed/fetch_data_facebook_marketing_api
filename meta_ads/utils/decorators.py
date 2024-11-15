import time
from functools import wraps
from typing import Any, Callable
import logging

def retry_with_backoff(max_retries: int = 3, initial_delay: float = 5.0):
    """
    Retry decorator with exponential backoff
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay
            last_exception = None
            
            for retry in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if "rate limit" in str(e).lower():
                        sleep_time = delay * (2 ** retry)
                        logging.warning(f"Rate limit hit. Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                    else:
                        raise e
            
            raise last_exception
        return wrapper
    return decorator