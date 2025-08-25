from abc import ABC, abstractmethod
from typing import Any, Dict, List


class EgressBase (ABC):
    """
    A base class for methods of egress from this application.
    """

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