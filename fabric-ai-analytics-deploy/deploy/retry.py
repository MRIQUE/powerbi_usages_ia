from __future__ import annotations
import functools
import time
import requests

_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
_DEFAULT_DELAY = 30


def retryable(func=None, *, max_attempts: int = 5):
    """Wrap an HTTP-calling function with exponential backoff retry.

    Retries on 429 and 5xx responses. Respects Retry-After header.
    Raises on 4xx (except 429) immediately without retry.
    """
    if func is None:
        return functools.partial(retryable, max_attempts=max_attempts)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        delay = _DEFAULT_DELAY
        last_response = None
        for attempt in range(1, max_attempts + 1):
            response = func(*args, **kwargs)
            if response.status_code not in _RETRYABLE_STATUSES:
                response.raise_for_status()
                return response
            last_response = response
            if attempt == max_attempts:
                break
            wait = int(response.headers.get("Retry-After", delay))
            time.sleep(wait)
            delay = min(delay * 2, 300)
        last_response.raise_for_status()

    return wrapper
