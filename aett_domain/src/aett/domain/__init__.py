import uuid

from aett.eventstore import *

T = typing.TypeVar('T', bound=Memento)


class Aggregate(ABC, typing.Generic[T]):
    """
    An aggregate is a cluster of domain objects that can be treated as a single unit. The aggregate base class requires
    implementors to provide a method to apply a snapshot and to get a memento.

    In addition to this, the aggregate base class provides a method to raise events, but the concrete application
    of the event relies on multiple dispatch to call the correct apply method in the subclass.
    """

    def __init__(self, stream_id: str):
        self.uncommitted: typing.List[EventMessage] = []
        self._id = stream_id
        self._version = 0
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

    @property
    def headers(self):
        return self._headers

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
    def get(self, cls: typing.Type[TAggregate], stream_id: str, max_version: int = 2 ** 32) -> TAggregate:
        """
        Gets the aggregate with the specified stream id and type

        :param cls: The type of the aggregate
        :param stream_id: The id of the stream to load
        :param max_version: The max aggregate version to load.
        """
        pass

    def get_to(self, cls: typing.Type[TAggregate], stream_id: str,
               max_time: datetime = datetime.datetime.max) -> TAggregate:
        """
        Gets the aggregate with the specified stream id and type

        :param cls: The type of the aggregate
        :param stream_id: The id of the stream to load
        :param max_time: The max aggregate timestamp to load.
        """
        pass

    @abstractmethod
    def save(self, aggregate: T, headers: Dict[str, str] = None) -> None:
        """
        Save the aggregate to the repository.

        :param aggregate: The aggregate to save.
        :param headers: The headers to assign to the commit.
        """
        pass

    @abstractmethod
    def snapshot(self, cls: typing.Type[TAggregate], stream_id: str, version: int, headers: Dict[str, str]) -> None:
        pass

    @abstractmethod
    def snapshot_at(self, cls: typing.Type[TAggregate], stream_id: str, cut_off: datetime.datetime,
                    headers: Dict[str, str]) -> None:
        pass


class SagaRepository(ABC):
    TSaga = typing.TypeVar('TSaga', bound=Saga)

    @abstractmethod
    def get(self, cls: typing.Type[TSaga], stream_id: str) -> TSaga:
        pass

    @abstractmethod
    def save(self, saga: Saga) -> None:
        pass


class DefaultAggregateRepository(AggregateRepository):
    TAggregate = typing.TypeVar('TAggregate', bound=Aggregate)

    def get(self, cls: typing.Type[TAggregate], stream_id: str, max_version: int = 2 ** 32) -> TAggregate:
        memento_type = inspect.signature(cls.apply_memento).parameters['memento'].annotation
        snapshot = self._snapshot_store.get(bucket_id=self._bucket_id, stream_id=stream_id, max_revision=max_version)
        min_version = 0
        if snapshot is not None:
            min_version = snapshot.stream_revision
        commits = self._store.get(bucket_id=self._bucket_id,
                                  stream_id=stream_id,
                                  min_revision=min_version,
                                  max_revision=max_version)

        aggregate = cls(stream_id)
        if snapshot is not None:
            aggregate.apply_memento(memento_type(**jsonpickle.decode(snapshot.payload)))
        for commit in commits:
            for event in commit.events:
                aggregate.raise_event(event.body)
        aggregate.uncommitted.clear()
        return aggregate

    def get_to(self, cls: typing.Type[TAggregate], stream_id: str,
               max_time: datetime = datetime.datetime.max) -> TAggregate:
        commits = self._store.get_to(bucket_id=self._bucket_id,
                                     stream_id=stream_id,
                                     max_time=max_time)
        aggregate = cls(stream_id)
        for commit in commits:
            for event in commit.events:
                aggregate.raise_event(event.body)
        aggregate.uncommitted.clear()
        return aggregate

    def save(self, aggregate: TAggregate, headers: Dict[str, str] = None) -> None:
        if headers is None:
            headers = {}
        if len(aggregate.uncommitted) == 0:
            return
        stream = EventStream.load(bucket_id=self._bucket_id,
                                  stream_id=aggregate.id,
                                  client=self._store,
                                  max_version=2 ** 32)
        for event in aggregate.uncommitted:
            stream.add(event)
        for key, value in headers.items():
            stream.set_header(key=key, value=value)
        self._store.commit(stream, uuid.uuid4())
        aggregate.uncommitted.clear()

    def snapshot(self, cls: typing.Type[TAggregate], stream_id: str, version: int = MAX_INT,
                 headers: Dict[str, str] = None) -> None:
        agg = self.get(cls, stream_id, version)
        self._snapshot_aggregate(agg, headers)

    def snapshot_at(self, cls: typing.Type[TAggregate], stream_id: str, cut_off: datetime.datetime,
                    headers: Dict[str, str] = None) -> None:
        agg = self.get_to(cls, stream_id, cut_off)
        self._snapshot_aggregate(agg, headers)

    def _snapshot_aggregate(self, aggregate: Aggregate, headers: Dict[str, str] = None) -> None:
        memento = aggregate.get_memento()
        snapshot = Snapshot(bucket_id=self._bucket_id, stream_id=aggregate.id,
                            payload=jsonpickle.encode(memento.payload, unpicklable=False),
                            stream_revision=memento.version, headers={})
        self._snapshot_store.add(snapshot=snapshot, headers=headers)

    def __init__(self, bucket_id: str, store: ICommitEvents, snapshot_store: IAccessSnapshots):
        self._bucket_id = bucket_id
        self._store = store
        self._snapshot_store = snapshot_store


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
        stream = EventStream.load(bucket_id=self._bucket_id, stream_id=saga.id, client=self._store)
        for event in saga.uncommitted:
            stream.add(event)
        self._store.commit(stream, uuid.uuid4())
        saga.uncommitted.clear()


TUncommitted = typing.TypeVar('TUncommitted', bound=BaseEvent)
TCommitted = typing.TypeVar('TCommitted', bound=BaseEvent)


class ConflictDelegate(ABC, typing.Generic[TUncommitted, TCommitted]):
    def detect(self, uncommitted: TUncommitted, committed: TCommitted) -> bool:
        pass


class ConflictDetector:
    def __init__(self, delegates: typing.List[ConflictDelegate] = None):
        self.delegates: typing.Dict[
            typing.Type, typing.Dict[typing.Type, typing.Callable[[BaseEvent, BaseEvent], bool]]] = {}
        if delegates is not None:
            for delegate in delegates:
                args = inspect.getfullargspec(delegate.detect)
                uncommitted_type = args.annotations[args.args[1]]
                committed_type = args.annotations[args.args[2]]
                if uncommitted_type not in self.delegates:
                    self.delegates[uncommitted_type] = {}
                self.delegates[uncommitted_type][committed_type] = delegate.detect

    def conflicts_with(self,
                       uncommitted_events: typing.Iterable[BaseEvent],
                       committed_events: typing.Iterable[BaseEvent]) -> bool:
        for uncommitted in uncommitted_events:
            for committed in committed_events:
                if type(uncommitted) in self.delegates and type(committed) in self.delegates[type(uncommitted)]:
                    if self.delegates[type(uncommitted)][type(committed)](uncommitted, committed):
                        return True
        return False
