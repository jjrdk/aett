import datetime

from pydantic import BaseModel

from aett.domain import Aggregate, Saga, ConflictDelegate
from aett.eventstore import DomainEvent, Memento, Topic
from aett.eventstore.base_command import BaseCommand


class DeepNestedValue(BaseModel):
    value: int


class NestedValue(BaseModel):
    value: DeepNestedValue


@Topic("Nested")
class NestedEvent(DomainEvent):
    value: NestedValue


@Topic("Test")
class TestEvent(DomainEvent):
    value: int


@Topic("Command")
class TestCommand(BaseCommand):
    value: int


class TestMemento(Memento):
    pass


class TestEventConflictDelegate(ConflictDelegate[TestEvent, TestEvent]):
    def detect(self, uncommitted: TestEvent, committed: TestEvent) -> bool:
        return uncommitted.value == committed.value


class TestAggregate(Aggregate[TestMemento]):
    def __init__(
            self, stream_id: str, commit_sequence: int, memento: TestMemento = None
    ):
        self.value = 0
        super().__init__(
            stream_id=stream_id, commit_sequence=commit_sequence, memento=memento
        )

    def apply_memento(self, memento: TestMemento) -> None:
        if self.id != memento.id:
            raise ValueError("Memento id does not match aggregate id")
        self.value = int(memento.payload["key"])

    def get_memento(self) -> TestMemento:
        return TestMemento(
            id=self.id, version=self.version, payload={"key": self.value + 1000}
        )

    def set_value(self, value: int) -> None:
        self.raise_event(
            TestEvent(
                value=value,
                source=self.id,
                version=self.version,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
            )
        )

    def add_value(self, value: int) -> None:
        self.raise_event(
            TestEvent(
                value=self.value + value,
                source=self.id,
                version=self.version,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
            )
        )

    def _apply(self, event: TestEvent) -> None:
        self.value = event.value


class TestSaga(Saga):
    def _apply(self, event: TestEvent) -> None:
        cmd = TestCommand(aggregate_id="test",
                          version=0,
                          timestamp=datetime.datetime.now(datetime.timezone.utc),
                          value=event.value)
        self.dispatch(cmd)
