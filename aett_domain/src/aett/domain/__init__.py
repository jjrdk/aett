import typing
import uuid
from abc import ABC, abstractmethod

from aett.eventstore import EventStream, EventMessage, Memento, DomainEvent, BaseEvent, ICommitEvents, IAccessSnapshots

T = typing.TypeVar('T', bound=Memento)


class Aggregate(ABC, typing.Generic[T]):
    """
    An aggregate is a cluster of domain objects that can be treated as a single unit. The aggregate base class requires
    implementors to provide a method to apply a snapshot and to get a memento.

    In addition to this, the aggregate base class provides a method to raise events, but the concrete application
    of the event relies on multiple dispatch to call the correct apply method in the subclass.
    """

    def __init__(self, stream: EventStream, memento: T = None):
        self.uncommitted: typing.List[EventMessage] = []
        self._id = stream.stream_id
        self._version = 0
        if memento is not None:
            self.apply_memento(memento)
            self._version = memento.version
        for event in stream.committed:
            if isinstance(event.body, DomainEvent):
                self.raise_event(event.body)
        self.uncommitted.clear()

    @property
    def id(self) -> str:
        return self._id

    @property
    def version(self) -> int:
        return self._version

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


class Saga(ABC):
    def __init__(self, event_stream: EventStream):
        self._id = event_stream.stream_id
        self._version = 0
        self.uncommitted: typing.List[EventMessage] = []
        self._headers: typing.Dict[str, typing.Any] = {}
        for event in event_stream.committed:
            self.transition(event.body)
        self.uncommitted.clear()

    @property
    def id(self) -> str:
        return self._id

    @property
    def version(self) -> int:
        return self._version

    def transition(self, event: BaseEvent) -> None:
        """
        Transitions the saga to the next state based on the event
        :param event: The trigger event
        :return: None
        """
        # Use multiple dispatch to call the correct apply method
        self._apply(event)
        self.uncommitted.append(EventMessage(body=event, headers=self._headers))
        self._version += 1

    def dispatch(self, command: T) -> None:
        """
        Adds a command to the stream to be dispatched when the saga is committed
        :param command: The command to dispatch
        :return: None
        """
        index = len(self._headers)
        self._headers[f'UndispatchedMessage.{index}'] = command


class AggregateRepository(ABC):
    TAggregate = typing.TypeVar('TAggregate', bound=Aggregate)

    @abstractmethod
    def get(self, cls: typing.Type[TAggregate], stream_id: str, max_version: int = 2**32) -> TAggregate:
        pass

    @abstractmethod
    def save(self, aggregate: T) -> None:
        pass


class SagaRepository(ABC):
    TSaga = typing.TypeVar('TSaga', bound=Saga)

    @abstractmethod
    def get(self, cls: typing.Type[TSaga], stream_id: str) -> TSaga:
        pass

    @abstractmethod
    def save(self, saga: Saga) -> None:
        pass


class ConflictDetector:
    def __init__(self, delegates: typing.List[typing.Callable[[BaseEvent, BaseEvent], bool]]):
        self.delegates: typing.Dict[
            typing.Type, typing.Dict[typing.Type, typing.Callable[[BaseEvent, BaseEvent], bool]]] = typing.Dict[
            typing.Type, typing.Dict[typing.Type, typing.Callable[[BaseEvent, BaseEvent], bool]]]()
        for delegate in delegates:
            if delegate[0] not in self.delegates:
                self.delegates[delegate[0]] = typing.Dict[typing.Type, typing.Callable[[BaseEvent, BaseEvent], bool]]()
            self.delegates[delegate[0]][delegate[1]] = delegate

    def conflicts_with(self,
                       uncommitted_events: typing.Iterable[BaseEvent],
                       committed_events: typing.Iterable[BaseEvent]) -> bool:
        for uncommitted in uncommitted_events:
            for committed in committed_events:
                if type(uncommitted) in self.delegates and type(committed) in self.delegates[type(uncommitted)]:
                    if self.delegates[type(uncommitted)][type(committed)](uncommitted, committed):
                        return True


class DefaultAggregateRepository(AggregateRepository):
    TAggregate = typing.TypeVar('TAggregate', bound=Aggregate)

    def get(self, cls: typing.Type[TAggregate], stream_id: str, version: int = 2 ** 32) -> TAggregate:
        stream = EventStream.load(bucket_id=self._bucket_id, stream_id=stream_id, client=self._store,
                                  max_version=version)
        # self._store.get(bucket_id=self._bucket_id, stream_id=stream_id, min_revision=0, max_revision=version))
        aggregate = cls(stream, None)
        return aggregate

    def save(self, aggregate: TAggregate) -> None:
        if len(aggregate.uncommitted) == 0:
            return
        stream = EventStream.load(bucket_id=self._bucket_id,
                                  stream_id=aggregate.id,
                                  client=self._store,
                                  max_version=2 ** 32)
        for event in aggregate.uncommitted:
            stream.add(event)
        self._store.commit(stream, uuid.uuid4())

    def __init__(self, bucket_id: str, store: ICommitEvents):
        self._bucket_id = bucket_id
        self._store = store


class DefaultSagaRepository(SagaRepository):
    TSaga = typing.TypeVar('TSaga', bound=Saga)

    def __init__(self, bucket_id: str, store: ICommitEvents):
        self._bucket_id = bucket_id
        self._store = store

    def get(self, cls: typing.Type[TSaga], stream_id: str) -> TSaga:
        stream = EventStream.load(bucket_id=self._bucket_id, stream_id=stream_id, client=self._store)
        saga = cls(stream)
        return saga

    def save(self, saga: TSaga) -> None:
        stream = EventStream.load(bucket_id=self._bucket_id, stream_id=saga.id,client=self._store)
        for event in saga.uncommitted:
            stream.add(event)
        self._store.commit(stream, uuid.uuid4())
