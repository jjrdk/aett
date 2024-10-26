import datetime
import uuid

from behave import *
from behave.api.async_step import async_run_until_complete

from aett.domain.async_default_saga_repository import AsyncDefaultSagaRepository
from aett.postgresasync import AsyncCommitStore
from features.steps.Types import TestSaga, TestEvent

use_step_matcher("re")


@given("I have a persistent saga repository")
def step_impl(context):
    context.repository = AsyncDefaultSagaRepository(str(uuid.uuid4()), AsyncCommitStore(context.db, context.topic_map))


@then("a specific saga type can be loaded from the repository")
@async_run_until_complete
async def step_impl(context):
    saga = await context.repository.get(TestSaga, 'test')
    assert isinstance(saga, TestSaga)


@when("a saga is loaded from the repository and modified")
@async_run_until_complete
async def step_impl(context):
    saga = await context.repository.get(TestSaga, 'test')
    saga.transition(TestEvent(value=1, source='test', version=0, timestamp=datetime.datetime.now(datetime.timezone.utc)))
    context.saga = saga


@step("the saga is saved to the repository")
@async_run_until_complete
async def step_impl(context):
    repository = context.repository
    await repository.save(context.saga)


@step("the saga is loaded again")
@async_run_until_complete
async def step_impl(context):
    context.saga = await context.repository.get(TestSaga, 'test')


@then("the modified saga is loaded from storage")
def step_impl(context):
    version = context.saga.version
    assert version == 1
