import itertools
import random
from typing import Iterable, TypeVar


T = TypeVar('T')

def chunks(iterable: Iterable[T], size: int):
    """ Break an array into chunks of size `size` or smaller."""

    iterator = iter(iterable)
    while chunk := list(itertools.islice(iterator, size)):
        yield chunk

class ConfigurationException (Exception):
    """ A class for an exception with the application configuration. """


def backoff_delay(attempt: int, base: float = 0.5, cap: float = 30.0) -> float:
    """
    Exponential backoff with jitter for the `attempt`th consecutive failure (1-based): the
    ceiling doubles from `base` up to `cap`, and the delay is drawn from the top half of it so
    that reconnecting clients do not stampede a recovering host in lockstep.
    """

    ceiling = min(cap, base * (2 ** max(0, attempt - 1)))
    return random.uniform(ceiling / 2, ceiling)
