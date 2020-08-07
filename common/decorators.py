import functools
from typing import Callable


def try_until_success(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper_try_until_succes(*args, **kwargs):
        done = False
        data = None
        while not done:
            try:
                data = func(*args, **kwargs)
                done = True
            except:
                pass
        return data

    return wrapper_try_until_succes
