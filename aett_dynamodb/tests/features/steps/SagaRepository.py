import datetime
import uuid
import features
from behave import *

from aett.domain import DefaultSagaRepository
from aett.dynamodb import CommitStore
from aett.eventstore import TopicMap
from features.steps.Types import TestSaga, TestEvent

use_step_matcher("re")


@step("a persistent saga repository")
def step_impl(context):
    tm = TopicMap()
    tm.register_module(features.steps.Types)
    context.repository = DefaultSagaRepository(str(uuid.uuid4()), CommitStore(region='localhost', topic_map=tm))


@then("a specific saga type can be loaded from the repository")
def step_impl(context):
    saga = context.repository.get(TestSaga, 'test')
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
