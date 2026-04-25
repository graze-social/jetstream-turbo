import itertools
from typing import Iterable, TypeVar


T = TypeVar('T')

def chunks(iterable: Iterable[T], size: int):
    """ Break an array into chunks of size `size` or smaller."""

    iterator = iter(iterable)
    while chunk := list(itertools.islice(iterator, size)):
        yield chunk

class ConfigurationException (Exception):
    """ A class for an exception with the application configuration. """