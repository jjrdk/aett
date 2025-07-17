import datetime
import uuid

from behave import *
from behave.api.async_step import async_run_until_complete

from aett.domain.async_default_saga_repository import AsyncDefaultSagaRepository
from test_types import TestSaga, TestEvent
from aett.domain.default_saga_repository import DefaultSagaRepository
from storage_factory import create_async_commit_store, create_commit_store

use_step_matcher("re")


@given("I have a persistent async saga repository")
def step_impl(context):
    commit_store = create_async_commit_store(
        connection_string=context.db,
        storage_type=context.storage_type,
        topic_map=context.topic_map,
        context=context,
    )
    context.repository = AsyncDefaultSagaRepository(str(uuid.uuid4()), commit_store)


@given("I have a persistent saga repository")
def step_impl(context):
    commit_store = create_commit_store(
        connection_string=context.db,
        storage_type=context.storage_type,
        topic_map=context.topic_map,
        context=context,
    )
    context.repository = DefaultSagaRepository(str(uuid.uuid4()), commit_store)


@then("a specific saga type can be loaded async from the repository")
@async_run_until_complete
async def step_impl(context):
    saga = await context.repository.get(TestSaga, "test")
    assert isinstance(saga, TestSaga), (
        f"Expected saga to be of type TestSaga, but was {type(saga)}"
    )


@then("a specific saga type can be loaded from the repository")
def step_impl(context):
    saga = context.repository.get(TestSaga, "test")
    assert isinstance(saga, TestSaga), (
        f"Expected saga to be of type TestSaga, but was {type(saga)}"
    )


@when("a saga is loaded async from the repository and modified")
@async_run_until_complete
async def step_impl(context):
    saga = await context.repository.get(TestSaga, "test")
    saga.transition(
        TestEvent(
            value=1,
            source="test",
            version=0,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
        )
    )
    context.saga = saga


@when("a saga is loaded from the repository and modified")
def step_impl(context):
    saga = context.repository.get(TestSaga, "test")
    saga.transition(
        TestEvent(
            value=1,
            source="test",
            version=0,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
        )
    )
    context.saga = saga


@step("the saga is saved async to the repository")
@async_run_until_complete
async def step_impl(context):
    repository = context.repository
    await repository.save(context.saga)


@step("the saga is saved to the repository")
def step_impl(context):
    repository = context.repository
    repository.save(context.saga)


@step("the saga is loaded again async")
@async_run_until_complete
async def step_impl(context):
    context.saga = await context.repository.get(TestSaga, "test")


@step("the saga is loaded again")
def step_impl(context):
    context.saga = context.repository.get(TestSaga, "test")


@then("the modified saga is loaded from storage")
def step_impl(context):
    version = context.saga.version
    assert version == 1


@step("the loaded saga has no headers")
def step_impl(context):
    saga = context.saga
    assert len(saga.headers) == 0, f"Expected no headers, but found {saga.headers}"
