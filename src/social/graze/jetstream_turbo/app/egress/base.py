from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class EgressError (Exception):
    """
    Records could not be egressed, even after retrying. Whoever sees this must not treat the
    records as delivered -- in particular, the persisted cursor must not move past them.
    `count` is how many records failed, when the method knows it was fewer than all of them.
    """

    def __init__(self, message: str, count: Optional[int] = None):
        super().__init__(message)
        self.count = count


class EgressBase (ABC):
    """
    A base class for methods of egress from this application.
    """

    # The `egress` label on this method's metrics.
    name: str = "egress"

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    async def store_records(self, enriched_records: List[Dict[str, Any]]):
        """
        A method to store records in whatever means of egress.
        """

        raise NotImplementedError()