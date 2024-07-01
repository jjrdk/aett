import datetime
import typing
import uuid

from behave import *

from aett.domain import SagaRepository, Saga
from aett.eventstore import Commit
from aett_domain.tests.features.steps.Types import TestSaga, TestEvent

use_step_matcher("re")


class TestSagaRepository(SagaRepository):

    def __init__(self):
        self._storage: typing.Dict[str, typing.List[Commit]] = {}

    def get(self, cls: typing.Type[SagaRepository.TSaga], saga_id: str) -> SagaRepository.TSaga:
        stream = self._storage.get(saga_id, None)
        if stream is None:
            stream = []
            self._storage[saga_id] = stream
        saga: SagaRepository.TSaga = cls(saga_id, 0 if len(stream) == 0 else stream[-1].commit_sequence)
        for commit in stream:
            saga.transition(commit.events)
        saga.uncommitted.clear()
        return saga

    def save(self, saga: Saga) -> None:
        stream = self._storage.get(saga.id)
        if stream is None:
            stream = []
        stream.append(Commit(tenant_id='test', stream_id=saga.id, stream_revision=0, commit_id=uuid.uuid4(),
                             commit_sequence=saga.commit_sequence + 1,
                             commit_stamp=datetime.datetime.now(datetime.timezone.utc), headers={}, events=saga.uncommitted,
                             checkpoint_token=0))
        self._storage[saga.id] = stream


@given("a saga repository")
def step_impl(context):
    context.repository = TestSagaRepository()


@then("a specific saga type can be loaded from the repository")
def step_impl(context):
    saga = context.repository.get(TestSaga, 'test')
    assert isinstance(saga, TestSaga)


@when("a saga is loaded from the repository and modified")
def step_impl(context):
    saga = context.repository.get(TestSaga, 'test')
    saga.transition(TestEvent(value=1, source='test', version=0, timestamp=datetime.datetime.now(datetime.timezone.utc)))
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
