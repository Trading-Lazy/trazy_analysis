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


def Singleton(cls):
    instances = {}

    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper
