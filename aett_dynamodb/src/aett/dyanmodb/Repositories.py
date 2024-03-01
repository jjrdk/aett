import typing
import uuid

from aett.domain.Domain import AggregateRepository, Aggregate, SagaRepository, Saga
from aett.dyanmodb.EventStore import CommitStore


class DynamoAggregateRepository(AggregateRepository):
    TAggregate = typing.TypeVar('TAggregate', bound=Aggregate)

    def get(self, cls: typing.Type[TAggregate], id: str, version: int) -> TAggregate:
        stream = self._store.get(self._bucket_id, id, version)
        aggregate = cls(id, stream, None)
        return aggregate

    def save(self, aggregate: TAggregate) -> None:
        self._store.commit(aggregate.stream, uuid.uuid4())

    def __init__(self, bucket_id: str, store: CommitStore):
        self._bucket_id = bucket_id
        self._store = store


class DynamoSagaRepository(SagaRepository):
    TSaga = typing.TypeVar('TSaga', bound=Saga)

    def __init__(self, bucket_id: str, store: CommitStore):
        self._bucket_id = bucket_id
        self._store = store

    def get(self, cls: typing.Type[TSaga], id: str) -> TSaga:
        stream = self._store.get(self._bucket_id, id)
        saga = cls(stream)
        return saga

    def save(self, saga: Saga) -> None:
        stream = saga.stream
        self._store.commit(stream, uuid.uuid4())
