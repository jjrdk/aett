import datetime
import typing
from typing import Type, Dict

from aett.domain import AggregateRepository, Aggregate
from aett.eventstore import MAX_INT
from features.steps.Types import TestAggregate


class TestAggregateRepository(AggregateRepository):
    TAggregate = typing.TypeVar('TAggregate', bound=Aggregate)

    def get_to(self, cls: Type[TAggregate], stream_id: str, max_time: datetime = datetime.datetime.max) -> TAggregate:
        pass

    def snapshot_at(self, cls: typing.Type[TAggregate], stream_id: str, cut_off: datetime.datetime,
                    headers: Dict[str, str]) -> None:
        pass

    def snapshot(self, cls: typing.Type[TAggregate], stream_id: str, version: int,
                 headers: typing.Dict[str, str]) -> None:
        pass

    def __init__(self, storage: {}):
        self.storage: dict = storage

    def get(self, cls: typing.Type[TAggregate], identifier: str, version: int = MAX_INT) -> TestAggregate:
        m = self.storage.get(identifier)
        agg = cls(identifier, 0)
        agg.apply_memento(m)
        return agg

    def save(self, aggregate: TAggregate, headers: typing.Dict[str, str] = None) -> None:
        self.storage[aggregate.id] = aggregate.get_memento()
