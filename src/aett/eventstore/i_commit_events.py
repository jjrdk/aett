import datetime
import typing
from abc import ABC, abstractmethod
from typing import Iterable

from aett.eventstore.constants import MAX_INT
from aett.eventstore.commit import Commit


class ICommitEvents(ABC):
    """
    Indicates the ability to commit events and access events to and from a given stream.

    Instances of this class must be designed to be multi-thread safe such that they can be shared between threads.
    """

    @abstractmethod
    def get(self, tenant_id: str, stream_id: str, min_revision: int = 0, max_revision: int = MAX_INT) -> \
            Iterable[Commit]:
        """
        Gets the corresponding commits from the stream indicated starting at the revision specified until the
        end of the stream sorted in ascending order--from oldest to newest.

        :param tenant_id: The value which uniquely identifies bucket the stream belongs to.
        :param stream_id: The stream from which the events will be read.
        :param min_revision: The minimum revision of the stream to be read.
        :param max_revision: The maximum revision of the stream to be read.
        :return: A series of committed events from the stream specified sorted in ascending order.
        :raises StorageException:
        :raises StorageUnavailableException:
        """
        pass

    @abstractmethod
    def get_to(self, tenant_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            typing.AsyncIterable[Commit]:
        """
        Gets the corresponding commits from the stream indicated starting at the revision specified until the
        end of the stream sorted in ascending order--from oldest to newest.

        :param tenant_id: The value which uniquely identifies bucket the stream belongs to.
        :param stream_id: The stream from which the events will be read.
        :param max_time: The max timestamp to return.
        :return: A series of committed events from the stream specified sorted in ascending order.
        :raises StorageException:
        :raises StorageUnavailableException:
        """
        pass

    @abstractmethod
    def get_all_to(self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        """
        Gets the corresponding commits from the stream indicated starting at the revision specified until the
        end of the stream sorted in ascending order--from oldest to newest.

        :param tenant_id: The value which uniquely identifies bucket the stream belongs to.
        :param max_time: The max timestamp to return.
        :return: A series of committed events from the stream specified sorted in ascending order.
        :raises StorageException:
        :raises StorageUnavailableException:
        """
        pass

    @abstractmethod
    def commit(self, commit: Commit):
        """
        Writes the to-be-committed events stream provided to the underlying persistence mechanism.

        :param commit: The series of events and associated metadata to be committed.
        :raises ConcurrencyException:
        :raises StorageException:
        :raises StorageUnavailableException:
        """
        pass
