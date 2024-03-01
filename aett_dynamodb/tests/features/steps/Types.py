import datetime
from dataclasses import dataclass

from aett.domain.Domain import Aggregate, Saga
from aett.eventstore.EventStream import DomainEvent, Memento, EventStream


@dataclass(frozen=True, kw_only=True)
class TestEvent(DomainEvent):
    value: int


@dataclass(frozen=True)
class TestMemento(Memento):
    value: int = 0


class TestAggregate(Aggregate[TestMemento]):
    def __init__(self, event_stream: EventStream, memento: TestMemento = None):
        self.value = 0
        super().__init__(event_stream, memento)

    def apply_memento(self, memento: TestMemento) -> None:
        if self.id != memento.id:
            raise ValueError("Memento id does not match aggregate id")
        self.value = memento.value

    def get_memento(self) -> TestMemento:
        return TestMemento(id=self.id, version=self.version, value=self.value, payload={'key': self.value})

    def set_value(self, value: int) -> None:
        self.raise_event(
            TestEvent(value=value, id=self.id, version=self.version, timestamp=datetime.datetime.now(datetime.UTC)))

    def apply(self, event: TestEvent) -> None:
        self.value = event.value


class TestSaga(Saga):
    def apply(self, event: TestEvent) -> None:
        pass
