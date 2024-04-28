import logging
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

    def __init__(self, stream_id: str, commit_sequence: int, memento: Memento[T] = None):
        """
        Initialize the aggregate

        param stream_id: The id of the stream
        param commit_sequence: The commit sequence number which the aggregate was built from
        """
        self.uncommitted: typing.List[EventMessage] = []
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


class Saga(ABC):
    """
    A saga is a long-running process that coordinates multiple services to achieve a goal.
    The saga base class requires implementors to provide a method to apply events during state transition.

    In addition to this, the aggregate base class provides a method to raise events, but the concrete application
    of the event relies on multiple dispatch to call the correct apply method in the subclass.
    """

    def __init__(self, saga_id: str, commit_sequence: int):
        """
        Initialize the saga

        param saga_id: The id of the saga
        param commit_sequence: The commit sequence number which the saga was built from
        """
        self._id = saga_id
        self._commit_sequence = commit_sequence
        self._version = 0
        self.uncommitted: typing.List[EventMessage] = []
        self._headers: typing.Dict[str, typing.Any] = {}

    @property
    def id(self) -> str:
        """
        Gets the id of the saga
        """
        return self._id

    @property
    def version(self) -> int:
        """
        Gets the version of the saga
        """
        return self._version

    @property
    def commit_sequence(self):
        """
        Gets the commit sequence number of the saga
        """
        return self._commit_sequence

    @property
    def headers(self):
        """
        Gets the metadata headers of the saga
        """
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
    """
    Defines the abstract interface for an aggregate repository.
    The repository is responsible for loading and saving aggregates to the event store,
    typically using the ICommitEvents interface.
    """
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
    def snapshot(self, cls: typing.Type[TAggregate], stream_id: str, version: int, headers: Dict[str, str]) -> None:
        """
        Generates a snapshot of the aggregate at the specified version.

        param cls: The type of the aggregate
        param stream_id: The id of the aggregate to snapshot
        param version: The version of the aggregate to snapshot
        param headers: The headers to assign to the snapshot
        """
        pass

    @abstractmethod
    def snapshot_at(self, cls: typing.Type[TAggregate], stream_id: str, cut_off: datetime.datetime,
                    headers: Dict[str, str]) -> None:
        """
        Generates a snapshot of the aggregate at the specified time point.

        param cls: The type of the aggregate
        param stream_id: The id of the aggregate to snapshot
        param cut_off: The time point of the aggregate to snapshot
        param headers: The headers to assign to the snapshot
        """
        pass


class SagaRepository(ABC):
    """
    Defines the abstract interface for an saga repository.
    The repository is responsible for loading and saving sagas to the event store,
    typically using the ICommitEvents interface.
    """
    TSaga = typing.TypeVar('TSaga', bound=Saga)

    @abstractmethod
    def get(self, cls: typing.Type[TSaga], stream_id: str) -> TSaga:
        """
        Gets the saga with the specified stream id and type at the latest version.
        """
        pass

    @abstractmethod
    def save(self, saga: Saga) -> None:
        """
        Save the saga to the repository.
        """
        pass


class DefaultAggregateRepository(AggregateRepository):
    def __init__(self, tenant_id: str, store: ICommitEvents, snapshot_store: IAccessSnapshots,
                 logger: logging.Logger = None):
        """
        Initialize the default aggregate repository.

        param tenant_id: The tenant id of the repository instance
        param store: The event store to use
        param snapshot_store: The snapshot store to use
        """
        self._tenant_id = tenant_id
        self._store = store
        self._snapshot_store = snapshot_store
        self._logger = logger if logger is not None else logging.getLogger(DefaultAggregateRepository.__name__)

    def get(self, cls: typing.Type[AggregateRepository.TAggregate], stream_id: str, max_version: int = 2 ** 32) -> \
            AggregateRepository.TAggregate:
        self._logger.info(f'Getting aggregate {cls.__name__} with id {stream_id} at version {max_version}')
        snapshot = self._snapshot_store.get(tenant_id=self._tenant_id, stream_id=stream_id, max_revision=max_version)
        min_version = 0
        commit_sequence = 0
        if snapshot is not None:
            min_version = snapshot.stream_revision + 1
            commit_sequence = snapshot.commit_sequence
        commits = list(self._store.get(tenant_id=self._tenant_id,
                                       stream_id=stream_id,
                                       min_revision=min_version,
                                       max_revision=max_version))
        if len(commits) > 0:
            commit_sequence = commits[-1].commit_sequence
        memento_type = inspect.signature(cls.apply_memento).parameters['memento'].annotation
        aggregate = cls(stream_id, commit_sequence,
                        memento_type(**jsonpickle.decode(snapshot.payload)) if snapshot is not None else None)
        for commit in commits:
            for event in commit.events:
                aggregate.raise_event(event.body)
        aggregate.uncommitted.clear()
        return aggregate

    def get_to(self, cls: typing.Type[AggregateRepository.TAggregate], stream_id: str,
               max_time: datetime = datetime.datetime.max) -> AggregateRepository.TAggregate:
        self._logger.info(
            f'Getting aggregate {cls.__name__} with id {stream_id} at time point {max_time:%Y%m%d-%H%M%S%z}')
        commits = list(self._store.get_to(tenant_id=self._tenant_id,
                                          stream_id=stream_id,
                                          max_time=max_time))
        commit_sequence = commits[-1].commit_sequence if len(commits) > 0 else 0
        aggregate = cls(stream_id, commit_sequence, None)
        for commit in commits:
            for event in commit.events:
                aggregate.raise_event(event.body)
        aggregate.uncommitted.clear()
        return aggregate

    def save(self, aggregate: AggregateRepository.TAggregate, headers: Dict[str, str] = None) -> None:
        self._logger.info(f'Saving aggregate {aggregate.id} at version {aggregate.version}')
        if headers is None:
            headers = {}
        if len(aggregate.uncommitted) == 0:
            return
        commit = Commit(tenant_id=self._tenant_id,
                        stream_id=aggregate.id,
                        stream_revision=aggregate.version,
                        commit_id=uuid.uuid4(),
                        commit_sequence=aggregate.commit_sequence + 1,
                        commit_stamp=datetime.datetime.now(datetime.UTC),
                        headers=dict(headers),
                        events=list(aggregate.uncommitted),
                        checkpoint_token=0)
        self._store.commit(commit)
        self._logger.info(f'Saved aggregate {aggregate.id}')
        aggregate.uncommitted.clear()

    def snapshot(self, cls: typing.Type[AggregateRepository.TAggregate], stream_id: str, version: int = MAX_INT,
                 headers: Dict[str, str] = None) -> None:
        self._logger.info(f'Snapshotting aggregate {cls.__name__} with id {stream_id} at version {version}')
        agg = self.get(cls, stream_id, version)
        self._snapshot_aggregate(agg, headers)

    def snapshot_at(self, cls: typing.Type[AggregateRepository.TAggregate], stream_id: str, cut_off: datetime.datetime,
                    headers: Dict[str, str] = None) -> None:
        self._logger.info(
            f'Snapshotting aggregate {cls.__name__} with id {stream_id} at time point {cut_off:%Y%m%d-%H%M%S%z}')
        agg = self.get_to(cls, stream_id, cut_off)
        self._snapshot_aggregate(agg, headers)

    def _snapshot_aggregate(self, aggregate: Aggregate, headers: Dict[str, str] = None) -> None:
        memento = aggregate.get_memento()
        snapshot = Snapshot(tenant_id=self._tenant_id,
                            stream_id=aggregate.id,
                            commit_sequence=aggregate.commit_sequence,
                            payload=jsonpickle.encode(memento, unpicklable=False),
                            stream_revision=memento.version,
                            headers=headers or {})
        self._snapshot_store.add(snapshot=snapshot, headers=headers)


class DefaultSagaRepository(SagaRepository):

    def __init__(self, tenant_id: str, store: ICommitEvents, logger: logging.Logger = None):
        """
        Initialize the default saga repository.

        param tenant_id: The tenant id of the repository instance
        param store: The event store to use
        """
        self._tenant_id = tenant_id
        self._store = store
        self._logger = logger if logger is not None else logging.getLogger(DefaultSagaRepository.__name__)

    def get(self, cls: typing.Type[SagaRepository.TSaga], stream_id: str) -> SagaRepository.TSaga:
        self._logger.info(f'Getting saga {cls.__name__} with id {stream_id}')
        commits = list(self._store.get(self._tenant_id, stream_id))
        commit_sequence = commits[-1].commit_sequence if len(commits) > 0 else 0
        saga = cls(stream_id, commit_sequence)
        for commit in commits:
            for event in commit.events:
                saga.transition(event.body)
        saga.uncommitted.clear()
        return saga

    def save(self, saga: Saga) -> None:
        self._logger.info(f'Saving saga {saga.id} at version {saga.version}')
        commit = Commit(tenant_id=self._tenant_id,
                        stream_id=saga.id,
                        stream_revision=saga.version,
                        commit_id=uuid.uuid4(),
                        commit_sequence=saga.commit_sequence + 1,
                        commit_stamp=datetime.datetime.now(datetime.UTC),
                        headers=dict(saga.headers),
                        events=list(saga.uncommitted),
                        checkpoint_token=0)
        self._store.commit(commit=commit)
        self._logger.info(f'Saved saga {saga.id}')
        saga.uncommitted.clear()
        saga.headers.clear()


TUncommitted = typing.TypeVar('TUncommitted', bound=BaseEvent)
TCommitted = typing.TypeVar('TCommitted', bound=BaseEvent)


class ConflictDelegate(ABC, typing.Generic[TUncommitted, TCommitted]):
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


class ConflictDetector:
    @staticmethod
    def empty() -> 'ConflictDetector':
        return ConflictDetector()

    def __init__(self, delegates: typing.List[ConflictDelegate] = None, logger: logging.Logger = None):
        """
        Initialize the conflict detector with the specified delegates.

        param delegates: The delegates to use for conflict detection
        param logger: The optional logger to use for logging.
        """
        self.delegates: typing.Dict[
            typing.Type, typing.Dict[typing.Type, typing.Callable[[BaseEvent, BaseEvent], bool]]] = {}
        self._logger = logger if logger is not None and delegates is not None else logging.getLogger(
            ConflictDetector.__name__)
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
        """
        Detects if the uncommitted events conflict with the committed events.

        param uncommitted_events: The uncommitted events to analyze
        param committed_events: The committed events to compare against.
        """
        if len(self.delegates) == 0:
            return False
        for uncommitted in uncommitted_events:
            for committed in committed_events:
                if type(uncommitted) in self.delegates and type(committed) in self.delegates[type(uncommitted)]:
                    if self.delegates[type(uncommitted)][type(committed)](uncommitted, committed):
                        if isinstance(uncommitted, DomainEvent):
                            self._logger.warning(
                                f'Detected conflict between uncommitted event {type(uncommitted).__name__} from {uncommitted.source} with version {uncommitted.version}')
                        else:
                            self._logger.warning(
                                f'Detected conflict between uncommitted event {type(uncommitted).__name__} from {uncommitted.source} with timestamp {uncommitted.timestamp:%Y%m%d-%H%M%S%z}')
                        return True
        return False


class DuplicateCommitException(Exception):
    """
    Exception raised when a duplicate commit is detected.
    """

    def __init__(self, message: str):
        super().__init__(message)


class ConflictingCommitException(Exception):
    """
    Exception raised when a conflicting commit is detected.
    """

    def __init__(self, message: str):
        super().__init__(message)


class NonConflictingCommitException(Exception):
    """
    Exception raised when a non-conflicting commit is detected.
    """

    def __init__(self, message: str):
        super().__init__(message)
