import typing
from abc import ABC, abstractmethod

from aett.eventstore.EventStream import EventStream, EventMessage, Memento, DomainEvent, BaseEvent

T = typing.TypeVar('T', bound=Memento)


class Aggregate(ABC, typing.Generic[T]):
    """
    An aggregate is a cluster of domain objects that can be treated as a single unit. The aggregate base class requires
    implementors to provide a method to apply a snapshot and to get a memento.

    In addition to this, the aggregate base class provides a method to raise events, but the concrete application
    of the event relies on multiple dispatch to call the correct apply method in the subclass.
    """

    def __init__(self, identifier: str, stream: EventStream, memento: T = None):
        self.id = identifier
        self.version = 0
        self.stream: EventStream = stream
        if memento is not None:
            self.apply_memento(memento)
        for event in self.stream.committed:
            if isinstance(event.body, DomainEvent):
                self.raise_event(event.body)

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
        self.apply(event)
        self.version += 1
        self.stream.add(EventMessage(body=event, headers=None))


class Saga(ABC):
    def __init__(self, event_stream: EventStream):
        self.id = id
        self.version = 0
        self.stream: EventStream = event_stream
        for event in self.stream.committed:
            self.transition(event.body)

    def transition(self, event: BaseEvent) -> None:
        """
        Transitions the saga to the next state based on the event
        :param event: The trigger event
        :return: None
        """
        # Use multiple dispatch to call the correct apply method
        self.apply(event)
        self.stream.add(event)
        self.version += 1

    def dispatch(self, command: T) -> None:
        """
        Adds a command to the stream to be dispatched when the saga is committed
        :param command: The command to dispatch
        :return: None
        """
        index = len(self.stream.uncommitted_headers)
        self.stream.set_header(f'UndispatchedMessage.{index}', command)


class AggregateRepository(ABC):
    TAggregate = typing.TypeVar('TAggregate', bound=Aggregate)

    @abstractmethod
    def get(self, cls: typing.Type[TAggregate], id: str, version: int) -> TAggregate:
        pass

    @abstractmethod
    def save(self, aggregate: T) -> None:
        pass


class SagaRepository(ABC):
    TSaga = typing.TypeVar('TSaga', bound=Saga)

    @abstractmethod
    def get(self, cls: typing.Type[TSaga], id: str) -> TSaga:
        pass

    @abstractmethod
    def save(self, saga: Saga) -> None:
        pass