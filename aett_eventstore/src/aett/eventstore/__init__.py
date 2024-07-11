import datetime
import inspect
import typing
from abc import ABC, abstractmethod
from typing import Iterable, Dict, List, Optional, Any
from uuid import UUID
from pydantic import BaseModel
from pydantic_core import to_json, from_json

T = typing.TypeVar('T')
MAX_INT = 2 ** 32 - 1
COMMITS = 'commits'
SNAPSHOTS = 'snapshots'


class StreamHead(BaseModel):
    tenant_id: str
    stream_id: str
    head_revision: int
    snapshot_revision: int


class Topic(object):
    """
    Represents the topic of an event message.
    Should be used as a decorator on a class to indicate the topic of the event which will help with type deserialization.
    """

    def __init__(self, topic: str):
        self.topic = topic

    def __call__(self, cls):
        cls.__topic__ = self.topic
        return cls

    @staticmethod
    def get(cls: type) -> str:
        return cls.__topic__ if hasattr(cls, '__topic__') else cls.__name__


class TopicMap:
    """
    Represents a map of topics to event classes.
    """

    def __init__(self):
        self.__topics = {}

    def add(self, topic: str, cls: type):
        """
        Adds the topic and class to the map.
        :param topic: The topic of the event.
        :param cls: The class of the event.
        """
        self.__topics[topic] = cls

    def register(self, instance: Any):
        t = instance if isinstance(instance, type) else type(instance)
        topic = Topic.get(t)
        if topic not in self.__topics:
            self.add(topic, t)

    def register_module(self, module: object):
        """
        Registers all the classes in the module.
        """
        for c in inspect.getmembers(module, inspect.isclass):
            self.register(c[1])

    def get(self, topic: str) -> type | None:
        """
        Gets the class of the event given the topic.
        :param topic: The topic of the event.
        :return: The class of the event.
        """
        return self.__topics.get(topic, None)

    def get_all_types(self) -> List[type]:
        """
        Gets all the types in the map.
        :return: A list of all the types in the map.
        """
        return list(self.__topics.values())


class BaseEvent(ABC, BaseModel):
    """
    Represents a single event which has occurred.
    """

    source: str
    """
    Gets the value which uniquely identifies the source of the event.
    """

    timestamp: datetime.datetime
    """
    Gets the point in time at which the event was generated.
    """


class DomainEvent(BaseEvent):
    """
    Represents a single event which has occurred within the domain.
    """

    version: int
    """
    Gets the version of the aggregate which generated the event.
    """


class Memento(ABC, BaseModel, typing.Generic[T]):
    id: str
    """
    Gets the id of the aggregate which generated the memento.
    """

    version: int
    """
    Gets the version of the aggregate which generated the memento.
    """

    payload: T
    """
    Gets the state of the aggregate at the time the memento was taken.
    """


class EventMessage(BaseModel):
    """
    Represents a single event message within a commit.
    """

    body: object
    """
    Gets the body of the event message.
    """

    headers: Dict[str, Any] | None = None
    """
    Gets the metadata which provides additional, unstructured information about this event message.
    """

    def to_json(self) -> bytes:
        """
        Converts the event message to a dictionary which can be serialized to JSON.
        """
        if self.headers is None:
            self.headers = {}
        if 'topic' not in self.headers:
            self.headers['topic'] = Topic.get(type(self.body))
        return to_json(self)

    @staticmethod
    def from_json(j: bytes | str, topic_map: TopicMap) -> 'EventMessage':
        json_dict = from_json(j)
        headers = json_dict['headers'] if 'headers' in json_dict and json_dict['headers'] is not None else None
        decoded_body = json_dict['body']
        topic = decoded_body.pop('$type', None)
        if topic is None and headers is not None and 'topic' in headers:
            topic = headers['topic']
        if headers is not None and topic is None and 'topic' in headers:
            topic = headers['topic']
        if topic is None:
            return EventMessage(body=BaseEvent(**decoded_body), headers=headers)
        else:
            t = topic_map.get(topic=topic)
            body = t(**decoded_body)
            return EventMessage(body=body, headers=headers)


class Commit(BaseModel):
    """
    Represents a series of events which have been fully committed as a single unit
    and which apply to the stream indicated.
    """

    tenant_id: str
    """
    Gets or sets the value which identifies tenant to which the stream and the commit belongs.
    """

    stream_id: str
    """
    Gets the value which uniquely identifies the stream to which the commit belongs.
    """

    stream_revision: int
    """
    Gets the value which indicates the revision of the most recent event in the stream to which this commit applies.
    """

    commit_id: UUID
    """
    Gets the value which uniquely identifies the commit within the stream.
    """

    commit_sequence: int
    """
    Gets the value which indicates the sequence (or position) in the stream to which this commit applies.
    """

    commit_stamp: datetime.datetime
    """
    Gets the point in time at which the commit was persisted.
    """

    headers: Dict[str, object]
    """
    Gets the metadata which provides additional, unstructured information about this commit.
    """

    events: List[EventMessage]
    """
    Gets the collection of event messages to be committed as a single unit.
    """

    checkpoint_token: int
    """
    The checkpoint that represents the storage level order.
    """


class Snapshot(BaseModel):
    """
    Represents a materialized view of a stream at specific revision.
    """

    tenant_id: str
    """
    Gets the value which uniquely identifies the tenant to which the stream belongs.
    """

    stream_id: str
    """
    Gets the value which uniquely identifies the stream to which the snapshot applies.
    """

    stream_revision: int
    """
    Gets the position at which the snapshot applies.
    """

    commit_sequence: int
    """
    Gets the commit sequence at which the snapshot applies.
    """

    payload: str
    """
    Gets the snapshot or materialized view of the stream at the revision indicated.
    """

    headers: Dict[str, str]

    @staticmethod
    def from_memento(tenant_id: str, memento: Memento, commit_sequence: int, headers: Dict[str, str]) -> 'Snapshot':
        """
        Converts the memento to a snapshot which can be persisted.
        :param tenant_id: The value which uniquely identifies the bucket to which the stream belongs.
        :param memento:  The memento to be converted.
        :param commit_sequence: The commit sequence at which the snapshot applies.
        :param headers: The headers to assign to the snapshot
        :return:
        """
        return Snapshot(tenant_id=tenant_id, stream_id=memento.id, stream_revision=memento.version,
                        payload=memento.payload.model_dump_json(serialize_as_any=True), headers=headers,
                        commit_sequence=commit_sequence)


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
            Iterable[Commit]:
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


class IAccessSnapshots(ABC):
    """
    Indicates the ability to get and add snapshots.
    """

    @abstractmethod
    def get(self, tenant_id: str, stream_id: str, max_revision: int) -> Optional[Snapshot]:
        """
        Gets the snapshot at the revision indicated or the most recent snapshot below that revision.

        :param tenant_id: The value which uniquely identifies the bucket to which the stream and the snapshot belong.
        :param stream_id: The stream for which the snapshot should be returned.
        :param max_revision: The maximum revision possible for the desired snapshot.
        :return: If found, returns the snapshot for the stream indicated; otherwise null.
        :raises StorageException:
        :raises StorageUnavailableException:
        """
        pass

    @abstractmethod
    def add(self, snapshot: Snapshot, headers: Dict[str, str] = None):
        """
        Adds the snapshot provided to the stream indicated. Using a snapshotId of Guid.Empty will always persist the
        snapshot.

        :param snapshot: The snapshot to save.
        :param headers: The metadata to assign to the snapshot.
        :raises StorageException:
        :raises StorageUnavailableException:
        """
        pass


class IManagePersistence(ABC):
    @abstractmethod
    def initialize(self):
        """
        Initializes the persistence mechanism.
        """
        pass

    @abstractmethod
    def drop(self):
        """
        Drops the persistence mechanism.
        """
        pass

    @abstractmethod
    def purge(self, tenant_id: str):
        """
        Purges the persistence mechanism.

        :param tenant_id: The value which uniquely identifies the tenant to be purged.
        """
        pass

    @abstractmethod
    def get_from(self, checkpoint: int) -> Iterable[Commit]:
        """
        Gets the commits from the checkpoint.
        :param checkpoint: The checkpoint to start from.
        :return: The commits from the checkpoint.
        """
        pass
