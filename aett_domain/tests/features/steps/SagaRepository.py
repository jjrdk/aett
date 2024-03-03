import datetime
import typing

from behave import *

from aett.domain import SagaRepository, Saga
from aett.eventstore import EventStream
from aett_domain.tests.features.steps.Types import TestSaga, TestEvent

use_step_matcher("re")


class TestSagaRepository(SagaRepository):
    TSaga = typing.TypeVar('TSaga', bound=Saga)

    def __init__(self):
        self.storage = {}

    def get(self, cls: typing.Type[TSaga], id: str) -> TSaga:
        stream = self.storage.get(id)
        if stream is None:
            stream = EventStream.create('test', id)
            self.storage[stream.stream_id] = stream
        return cls(stream)

    def save(self, saga: Saga) -> None:
        stream = self.storage.get(saga.id)
        if stream is None:
            stream = EventStream.create('test', saga.id)
        for event in saga.uncommitted:
            stream.add(event)
        stream.set_persisted(stream.commit_sequence + 1)
        self.storage[stream.stream_id] = stream


@given("a saga repository")
def step_impl(context):
    context.repository = TestSagaRepository()


@then("a specific saga type can be loaded from the repository")
def step_impl(context):
    saga = context.repository.get(TestSaga, EventStream.create('test', 'test'))
    assert isinstance(saga, TestSaga)


@when("a saga is loaded from the repository and modified")
def step_impl(context):
    saga = context.repository.get(TestSaga, 'test')
    saga.transition(TestEvent(value=1, source='test', version=0, timestamp=datetime.datetime.now(datetime.UTC)))
    context.saga = saga


@step("the saga is saved to the repository")
def step_impl(context):
    repository = context.repository
    repository.save(context.saga)


@step("the saga is loaded again")
def step_impl(context):
    context.saga = context.repository.get(TestSaga, 'test')


@then("the modified saga is loaded from storage")
def step_impl(context):
    version = context.saga.version
    assert version == 1
