import datetime
from abc import ABC, abstractmethod
from typing import TypeVar, Type, Dict

from aett.domain import Aggregate
from aett.domain.constants import T


class AsyncAggregateRepository(ABC):
    """
    Defines the abstract interface for an aggregate repository.
    The repository is responsible for loading and saving aggregates to the event store,
    typically using the ICommitEvents interface.
    """
    TAggregate = TypeVar('TAggregate', bound=Aggregate)

    @abstractmethod
    async def get(self, cls: Type[TAggregate], stream_id: str, max_version: int = 2 ** 32) -> TAggregate:
        """
        Gets the aggregate with the specified stream id and type

        :param cls: The type of the aggregate
        :param stream_id: The id of the stream to load
        :param max_version: The max aggregate version to load.
        """
        pass

    async def get_to(self, cls: Type[TAggregate], stream_id: str,
                     max_time: datetime = datetime.datetime.max) -> TAggregate:
        """
        Gets the aggregate with the specified stream id and type

        :param cls: The type of the aggregate
        :param stream_id: The id of the stream to load
        :param max_time: The max aggregate timestamp to load.
        """
        pass

    @abstractmethod
    async def save(self, aggregate: T, headers: Dict[str, str] = None) -> None:
        """
        Save the aggregate to the repository.

        The call to save should be wrapped in a try-except block as concurrent modifications can cause a conflict with
        events committed from a different source. A ConflictingCommitException will be thrown by the storage layer if
        an attempt is made to save an aggregate with a version that is lower than the current version in the store and
        the uncommitted events conflict with the committed events.A NonConflictingCommitException will be thrown if the
        uncommitted events do not conflict with the committed events. In this case it should be safe to retry the
        operation.

        :param aggregate: The aggregate to save.
        :param headers: The headers to assign to the commit.
        """
        pass

    @abstractmethod
    async def snapshot(self, cls: Type[TAggregate], stream_id: str, version: int,
                       headers: Dict[str, str]) -> None:
        """
        Generates a snapshot of the aggregate at the specified version.

        :param cls: The type of the aggregate
        :param stream_id: The id of the aggregate to snapshot
        :param version: The version of the aggregate to snapshot
        :param headers: The headers to assign to the snapshot
        """
        pass

    @abstractmethod
    async def snapshot_at(self, cls: Type[TAggregate], stream_id: str, cut_off: datetime.datetime,
                          headers: Dict[str, str]) -> None:
        """
        Generates a snapshot of the aggregate at the specified time point.

        :param cls: The type of the aggregate
        :param stream_id: The id of the aggregate to snapshot
        :param cut_off: The time point of the aggregate to snapshot
        :param headers: The headers to assign to the snapshot
        """
        pass
