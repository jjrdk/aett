import typing
import uuid

from aett.domain.Domain import AggregateRepository, Aggregate, SagaRepository, Saga
from aett.eventstore.EventStream import EventStream, ICommitEvents


class DefaultAggregateRepository(AggregateRepository):
    TAggregate = typing.TypeVar('TAggregate', bound=Aggregate)

    def get(self, cls: typing.Type[TAggregate], id: str, version: int) -> TAggregate:
        stream = self._store.get(self._bucket_id, id, version)
        aggregate = cls(stream, None)
        return aggregate

    def save(self, aggregate: TAggregate) -> None:
        if len(aggregate.uncommitted) == 0:
            return
        stream = self._store.get(self._bucket_id, aggregate.id, 0, aggregate.version)
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

    def get(self, cls: typing.Type[TSaga], id: str) -> TSaga:
        stream = self._store.get(self._bucket_id, id)
        saga = cls(stream)
        return saga

    def save(self, saga: TSaga) -> None:
        stream = self._store.get(self._bucket_id, saga.id)
        if stream is None:
            stream = EventStream.create('test', saga.id)
        for event in saga.uncommitted:
            stream.add(event)
        self._store.commit(stream, uuid.uuid4())
