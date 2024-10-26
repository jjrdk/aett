import datetime
import inspect
import uuid

from behave import *

from aett.domain.default_saga_repository import DefaultSagaRepository
from aett.eventstore import TopicMap
from aett.s3 import CommitStore
from aett_s3.tests.features.steps.Types import TestEvent, TestSaga

use_step_matcher("re")


@given("I have a persistent saga repository")
def step_impl(context):
    tm = TopicMap()
    tm.register_module(inspect.getmodule(TestEvent))
    context.repository = DefaultSagaRepository(str(uuid.uuid4()), CommitStore(context.s3_config, tm))


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
