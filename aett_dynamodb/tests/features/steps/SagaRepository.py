import datetime
import uuid
import Types
from behave import *

from aett.domain.default_saga_repository import DefaultSagaRepository
from aett.dynamodb.commit_store import CommitStore
from aett.eventstore import TopicMap
from Types import TestSaga, TestEvent

use_step_matcher("re")


@step("a persistent saga repository")
def step_impl(context):
    tm = TopicMap()
    tm.register_module(Types)
    context.stream_id = str(uuid.uuid4())
    context.repository = DefaultSagaRepository(str(uuid.uuid4()), CommitStore(region='localhost', topic_map=tm))


@then("a specific saga type can be loaded from the repository")
def step_impl(context):
    saga = context.repository.get(TestSaga, context.stream_id)
    assert isinstance(saga, TestSaga)


@when("a saga is loaded from the repository and modified")
def step_impl(context):
    saga = context.repository.get(TestSaga, context.stream_id)
    saga.transition(TestEvent(value=1, source='test', version=0, timestamp=datetime.datetime.now(datetime.timezone.utc)))
    context.saga = saga


@step("the saga is saved to the repository")
def step_impl(context):
    repository = context.repository
    repository.save(context.saga)


@step("the saga is loaded again")
def step_impl(context):
    context.saga = context.repository.get(TestSaga, context.stream_id)


@then("the modified saga is loaded from storage")
def step_impl(context):
    version = context.saga.version
    assert version == 1
