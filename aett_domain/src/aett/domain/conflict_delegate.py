from abc import ABC, abstractmethod
from typing import Generic

from aett.domain.constants import TUncommitted, TCommitted


class ConflictDelegate(ABC, Generic[TUncommitted, TCommitted]):
    """
    A conflict delegate is a class that can detect conflicts between two events.
    """

    @abstractmethod
    def detect(self, uncommitted: TUncommitted, committed: TCommitted) -> bool:
        """
        Detects if the uncommitted event conflicts with the committed event. The delegate should return True if an event
        is incompatible with a previously persisted event.

        If the delegate returns False then it is assumed that the later event is compatible with the previously
        persisted.
        """
        pass
