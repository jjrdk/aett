from abc import ABC, abstractmethod
from typing import Generic, List

from aett.domain.constants import T
from aett.eventstore import DomainEvent, EventMessage, Memento


class Aggregate(ABC, Generic[T]):
    """
    An aggregate is a cluster of domain objects that can be treated as a single unit. The aggregate base class requires
    implementors to provide a method to apply a snapshot and to get a memento.

    In addition to this, the aggregate base class provides a method to raise events, but the concrete application
    of the event relies on multiple dispatch to call the correct apply method in the subclass.
    """

    def __init__(self, stream_id: str, commit_sequence: int, memento: Memento[T] = None):
        """
        Initialize the aggregate

        :param stream_id: The id of the stream
        :param commit_sequence: The commit sequence number which the aggregate was built from
        """
        self.uncommitted: List[EventMessage] = []
        self._id = stream_id
        self._version = 0
        self._commit_sequence = commit_sequence
        if memento is not None:
            self._version = memento.version
            self.apply_memento(memento)

    @property
    def id(self) -> str:
        """
        Gets the id of the aggregate
        """
        return self._id

    @property
    def version(self) -> int:
        """
        Gets the version of the aggregate
        """
        return self._version

    @property
    def commit_sequence(self):
        """
        Gets the commit sequence number of the aggregate
        """
        return self._commit_sequence

    @abstractmethod
    def apply_memento(self, memento: T) -> None:
        """
        Apply a memento to the aggregate
        :param memento: The memento to apply
        :return: None
        """
        pass

    @abstractmethod
    def get_memento(self) -> T:
        """
        Get a memento of the current state of the aggregate
        :return: A memento instance
        """
        pass

    def __getstate__(self):
        return self.get_memento()

    def __setstate__(self, state):
        self.apply_memento(state)

    def raise_event(self, event: DomainEvent) -> None:
        """
        Raise an event on the aggregate. This is the method that internal logic should use to raise events in order to
        ensure that the event gets applied and the version gets incremented and the event is made available for
        persistence in the event store.
        :param event:
        :return:
        """
        # Use multiple dispatch to call the correct apply method
        self._apply(event)
        self._version += 1
        self.uncommitted.append(EventMessage(body=event, headers=None))
