import datetime
import uuid

from behave import *
from behave.api.async_step import async_run_until_complete

from aett.eventstore import EventMessage, Commit, ICommitEventsAsync, ICommitEvents
from test_types import TestEvent, NestedEvent, NestedValue, DeepNestedValue
from storage_factory import create_async_commit_store, create_commit_store

use_step_matcher("re")


@given("I have an async (?P<storage>.+) commit store")
def step_impl(context, storage: str):
    context.storage_type = storage
    context.store = create_async_commit_store(
        connection_string=context.db,
        storage_type=storage,
        topic_map=context.topic_map,
        context=context,
    )


@given("I have an (?P<storage>.+) commit store")
def step_impl(context, storage: str):
    context.storage_type = storage
    context.store = create_commit_store(
        connection_string=context.db,
        storage_type=storage,
        topic_map=context.topic_map,
        context=context,
    )


@when("I commit an async event to the stream")
@async_run_until_complete
async def step_impl(context):
    commit = Commit(
        tenant_id=context.tenant_id,
        stream_id=context.stream_id,
        stream_revision=1,
        commit_id=uuid.uuid4(),
        commit_sequence=1,
        commit_stamp=datetime.datetime.now(datetime.timezone.utc),
        headers={},
        events=[
            EventMessage(
                body=TestEvent(
                    source="test", timestamp=datetime.datetime.now(), version=1, value=0
                )
            )
        ],
        checkpoint_token=0,
    )
    await context.store.commit(commit)


@when("I commit an event to the stream")
def step_impl(context):
    commit = Commit(
        tenant_id=context.tenant_id,
        stream_id=context.stream_id,
        stream_revision=1,
        commit_id=uuid.uuid4(),
        commit_sequence=1,
        commit_stamp=datetime.datetime.now(datetime.timezone.utc),
        headers={},
        events=[
            EventMessage(
                body=TestEvent(
                    source="test", timestamp=datetime.datetime.now(), version=1, value=0
                )
            )
        ],
        checkpoint_token=0,
    )
    context.store.commit(commit)


@then("the event is persisted async to the store")
@async_run_until_complete
async def step_impl(context):
    store: ICommitEventsAsync = context.store
    stream = []
    ait = store.get(context.tenant_id, context.stream_id, 0, 1)
    async for commit in ait:
        stream.append(commit)
    context.stream = stream
    assert stream[0].events[0].body.version == 1


@then("the event is persisted to the store")
def step_impl(context):
    store: ICommitEvents = context.store
    stream = []
    ait = store.get(context.tenant_id, context.stream_id, 0, 1)
    for commit in ait:
        stream.append(commit)
    context.stream = stream
    assert stream[0].events[0].body.version == 1


@when("I commit an async event with nested base models to the stream")
@async_run_until_complete
async def step_impl(context):
    evt = NestedEvent(
        source="test",
        timestamp=datetime.datetime.now(),
        version=1,
        value=NestedValue(value=DeepNestedValue(value=0)),
    )
    commit = Commit(
        tenant_id=context.tenant_id,
        stream_id=context.stream_id,
        stream_revision=1,
        commit_id=uuid.uuid4(),
        commit_sequence=1,
        commit_stamp=datetime.datetime.now(datetime.timezone.utc),
        headers={},
        events=[EventMessage(body=evt)],
        checkpoint_token=0,
    )
    await context.store.commit(commit)


@when("I commit an event with nested base models to the stream")
def step_impl(context):
    evt = NestedEvent(
        source="test",
        timestamp=datetime.datetime.now(),
        version=1,
        value=NestedValue(value=DeepNestedValue(value=0)),
    )
    commit = Commit(
        tenant_id=context.tenant_id,
        stream_id=context.stream_id,
        stream_revision=1,
        commit_id=uuid.uuid4(),
        commit_sequence=1,
        commit_stamp=datetime.datetime.now(datetime.timezone.utc),
        headers={},
        events=[EventMessage(body=evt)],
        checkpoint_token=0,
    )
    context.store.commit(commit)


@step("the nested types are preserved")
def step_impl(context):
    stream = context.stream
    value = stream[0].events[0].body.value.value
    assert isinstance(value, DeepNestedValue), (
        f"Expected DeepNestedValue, got {type(value)}"
    )
