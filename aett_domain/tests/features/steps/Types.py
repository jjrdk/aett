import datetime
from aett.domain import Aggregate, Saga
from aett.eventstore import DomainEvent, Memento, Topic


@Topic("Test")
class TestEvent(DomainEvent):
    value: int


class TestMemento(Memento):
    value: int = 0


class TestAggregate(Aggregate[TestMemento]):
    def __init__(self, stream_id: str, commit_sequence: int):
        self.value = 0
        super().__init__(stream_id=stream_id, commit_sequence=commit_sequence)

    def apply_memento(self, memento: TestMemento) -> None:
        if memento is None:
            return
        if self.id != memento.id:
            raise ValueError("Memento id does not match aggregate id")
        self.value = memento.value

    def get_memento(self) -> TestMemento:
        return TestMemento(id=self.id, version=self.version, value=self.value, payload={'key': self.value})

    def set_value(self, value: int) -> None:
        self.raise_event(
            TestEvent(value=value, source=self.id, version=self.version,
                      timestamp=datetime.datetime.now(datetime.timezone.utc)))

    def _apply(self, event: TestEvent) -> None:
        self.value = event.value


class TestSaga(Saga):
    def _apply(self, event: TestEvent) -> None:
        pass
