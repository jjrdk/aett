import datetime
import inspect
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Dict, List, Optional, Any
from uuid import UUID

import jsonpickle

T = typing.TypeVar('T')
MAX_INT = 2 ** 32 - 1


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

    #
    # def __new__(cls):
    #     if not hasattr(cls, '__singleton_instance'):
    #         cls.__singleton_instance = super(TopicMap, cls).__new__(cls)
    #     return cls.__singleton_instance

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

    def get(self, topic: str) -> type:
        """
        Gets the class of the event given the topic.
        :param topic: The topic of the event.
        :return: The class of the event.
        """
        return self.__topics[topic]

    def get_all_types(self) -> List[type]:
        """
        Gets all the types in the map.
        :return: A list of all the types in the map.
        """
        return list(self.__topics.values())


@dataclass(frozen=True, kw_only=True)
class BaseEvent(ABC):
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


@dataclass(frozen=True, kw_only=True)
class DomainEvent(BaseEvent):
    """
    Represents a single event which has occurred within the domain.
    """

    version: int
    """
    Gets the version of the aggregate which generated the event.
    """


@dataclass(frozen=True, kw_only=True)
class Memento(ABC, typing.Generic[T]):
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


@dataclass(frozen=True, kw_only=True)
class EventMessage:
    """
    Represents a single event message within a commit.
    """

    body: BaseEvent
    """
    Gets the body of the event message.
    """

    headers: Dict[str, Any] | None = None
    """
    Gets the metadata which provides additional, unstructured information about this event message.
    """

    def to_json(self) -> dict:
        """
        Converts the event message to a dictionary which can be serialized to JSON.
        """
        data = {}
        headers = self.headers if self.headers is not None else {}
        headers['topic'] = Topic.get(type(self.body))
        data['headers'] = jsonpickle.encode(headers, unpicklable=False)
        value = {'$type': headers['topic']}
        value.update(self.body.__dict__)
        data['body'] = jsonpickle.encode(value, unpicklable=False)
        return data

    @staticmethod
    def from_json(json_dict: dict, topic_map: TopicMap) -> 'EventMessage':
        headers = jsonpickle.decode(json_dict['headers']) if 'headers' in json_dict else None
        decoded_body = jsonpickle.decode(json_dict['body'])
        decoded_body.pop('$type', None)
        body = None if not headers else topic_map.get(headers['topic'])(**decoded_body)
        return EventMessage(body=body, headers=headers)


@dataclass(frozen=True, kw_only=True)
class Commit:
    """
    Represents a series of events which have been fully committed as a single unit
    and which apply to the stream indicated.
    """

    bucket_id: str
    """
    Gets or sets the value which identifies bucket to which the stream and the commit belongs.
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


@dataclass(frozen=True, kw_only=True)
class Snapshot:
    """
    Represents a materialized view of a stream at specific revision.
    """

    bucket_id: str
    """
    Gets the value which uniquely identifies the bucket to which the stream belongs.
    """

    stream_id: str
    """
    Gets the value which uniquely identifies the stream to which the snapshot applies.
    """

    stream_revision: int
    """
    Gets the position at which the snapshot applies.
    """

    payload: str
    """
    Gets the snapshot or materialized view of the stream at the revision indicated.
    """

    headers: Dict[str, str]

    @staticmethod
    def from_memento(bucket_id: str, memento: Memento, headers: Dict[str, str]) -> 'Snapshot':
        """
        Converts the memento to a snapshot which can be persisted.
        :param bucket_id: The value which uniquely identifies the bucket to which the stream belongs.
        :param memento:  The memento to be converted.
        :param headers: The headers to assign to the snapshot
        :return:
        """
        return Snapshot(bucket_id=bucket_id, stream_id=memento.id, stream_revision=memento.version,
                        payload=jsonpickle.encode(memento.payload), headers=headers)


class ICommitEvents(ABC):
    """
    Indicates the ability to commit events and access events to and from a given stream.

    Instances of this class must be designed to be multi-thread safe such that they can be shared between threads.
    """

    @abstractmethod
    def get(self, bucket_id: str, stream_id: str, min_revision: int = 0, max_revision: int = MAX_INT) -> \
            Iterable[Commit]:
        """
        Gets the corresponding commits from the stream indicated starting at the revision specified until the
        end of the stream sorted in ascending order--from oldest to newest.

        :param bucket_id: The value which uniquely identifies bucket the stream belongs to.
        :param stream_id: The stream from which the events will be read.
        :param min_revision: The minimum revision of the stream to be read.
        :param max_revision: The maximum revision of the stream to be read.
        :return: A series of committed events from the stream specified sorted in ascending order.
        :raises StorageException:
        :raises StorageUnavailableException:
        """
        pass

    @abstractmethod
    def get_to(self, bucket_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        """
        Gets the corresponding commits from the stream indicated starting at the revision specified until the
        end of the stream sorted in ascending order--from oldest to newest.

        :param bucket_id: The value which uniquely identifies bucket the stream belongs to.
        :param stream_id: The stream from which the events will be read.
        :param max_time: The max timestamp to return.
        :return: A series of committed events from the stream specified sorted in ascending order.
        :raises StorageException:
        :raises StorageUnavailableException:
        """
        pass

    @abstractmethod
    def commit(self, event_stream: 'EventStream', commit_id: UUID):
        """
        Writes the to-be-committed events stream provided to the underlying persistence mechanism.

        :param commit_id: The identifier of the commit.
        :param event_stream: The series of events and associated metadata to be committed.
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
    def get(self, bucket_id: str, stream_id: str, max_revision: int) -> Optional[Snapshot]:
        """
        Gets the snapshot at the revision indicated or the most recent snapshot below that revision.

        :param bucket_id: The value which uniquely identifies the bucket to which the stream and the snapshot belong.
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


class StreamNotFoundException(Exception):
    """
    Represents the error that occurs when a stream is not found.
    """

    def __init__(self, message):
        super().__init__(message)


class EventStream:
    """
    Represents a series of events which originate from a single source.
    """

    @staticmethod
    def load(bucket_id: str,
             stream_id: str,
             client: ICommitEvents,
             min_version: int = 0,
             max_version: int = 2 ** 32) -> 'EventStream':
        """
        Loads the event stream from the underlying persistence mechanism.
        :param bucket_id: The value which uniquely identifies the bucket to which the stream belongs.
        :param stream_id: The value which uniquely identifies the stream to be loaded.
        :param client: The client to use to access the underlying persistence mechanism.
        :param min_version: The minimum revision of the stream to be read.
        :param max_version: The maximum revision of the stream to be read.
        :return: An instance of the event stream.
        """
        instance = EventStream(bucket_id, stream_id, 0)
        commits = client.get(bucket_id, stream_id, min_version, max_version)
        instance.__populate_stream__(min_version, max_version, commits)
        if min_version > 0 and len(instance.committed) == 0:
            raise StreamNotFoundException(f"Stream {stream_id} not found in {bucket_id}")
        return instance

    @staticmethod
    def create(bucket_id: str, stream_id: str):
        return EventStream(bucket_id, stream_id, 0)

    def add(self, event: EventMessage):
        self.uncommitted.append(event)
        self.version += 1

    def set_header(self, key: str, value: object):
        self.uncommitted_headers[key] = value

    def set_persisted(self, commit_sequence: int):
        self.committed.extend(self.uncommitted)
        self.committed_headers.update(self.uncommitted_headers)
        self.commit_sequence = commit_sequence
        self.uncommitted.clear()
        self.uncommitted_headers.clear()

    def to_commit(self, commit_id: UUID) -> Commit:
        commit = Commit(bucket_id=self.bucket_id,
                        stream_id=self.stream_id,
                        stream_revision=self.version,
                        commit_id=commit_id,
                        commit_sequence=self.commit_sequence + 1,
                        commit_stamp=datetime.datetime.now(),
                        headers=self.uncommitted_headers,
                        events=self.uncommitted,
                        checkpoint_token=0)
        return commit

    def __populate_stream__(
            self,
            min_version: int,
            max_version: int,
            commits: Iterable[Commit]):
        for commit in commits:
            self.commit_sequence = commit.commit_sequence
            current_version = commit.stream_revision - len(commit.events) + 1
            if current_version > max_version:
                return
            self.__copy_to_committed_headers__(commit)
            self.__copy_to_events__(min_version, max_version, current_version, commit.events)

    def __copy_to_committed_headers__(self, commit: Commit):
        for key in commit.headers.keys():
            self.committed_headers[key] = commit.headers[key]

    def __copy_to_events__(self, min_version: int, max_version: int, current_version: int,
                           events: Iterable[EventMessage]):
        for event in events:
            if not isinstance(event.body, DomainEvent):
                continue
            if current_version > max_version:
                break
            current_version += 1
            if current_version < min_version:
                continue
            self.committed.append(event)
            current_version = current_version - 1
            self.version = current_version

    def __update__(self, event_store: ICommitEvents):
        version = self.version - len(self.uncommitted)
        commits = event_store.get(self.bucket_id, self.stream_id, version + 1, MAX_INT)
        self.__populate_stream__(version + 1, MAX_INT, commits)
        to_be_committed = self.uncommitted.copy()
        self.uncommitted.clear()
        for event in to_be_committed:
            self.add(event)

    def __init__(self, bucket_id: str, stream_id: str, stream_revision: int):
        self.bucket_id = bucket_id
        self.stream_id = stream_id
        self.version = stream_revision
        self.commit_sequence: int = 0
        self.committed_headers: dict[str, object] = {}
        self.uncommitted_headers: dict[str, object] = {}
        self.committed: list[EventMessage] = []
        self.uncommitted: list[EventMessage] = []
