from behave import *
from behave.api.async_step import async_run_until_complete

from aett.eventstore import Snapshot
from storage_factory import create_async_snapshot_store, create_snapshot_store

use_step_matcher("re")


@given("I have an async snapshot store")
def step_impl(context):
    context.store = create_async_snapshot_store(
        connection_string=context.db, storage_type=context.storage_type
    )


@given("I have an snapshot store")
def step_impl(context):
    context.store = create_snapshot_store(
        connection_string=context.db, storage_type=context.storage_type
    )


@step("make an async snapshot of the stream")
@async_run_until_complete
async def step_impl(context):
    snapshot = Snapshot(
        tenant_id=context.tenant_id,
        stream_id=context.stream_id,
        stream_revision=1,
        payload='{"key": "test"}',
        headers={},
        commit_sequence=1,
    )
    await context.store.add(snapshot, {})


@step("make a snapshot of the stream")
def step_impl(context):
    snapshot = Snapshot(
        tenant_id=context.tenant_id,
        stream_id=context.stream_id,
        stream_revision=1,
        payload='{"key": "test"}',
        headers={},
        commit_sequence=1,
    )
    context.store.add(snapshot, {})


@then("the snapshot is persisted async to the store")
@async_run_until_complete
async def step_impl(context):
    loaded_back: Snapshot = await context.store.get(
        context.tenant_id, context.stream_id, 1
    )
    assert loaded_back.payload == '{"key": "test"}'


@then("the snapshot is persisted to the store")
def step_impl(context):
    loaded_back: Snapshot = context.store.get(context.tenant_id, context.stream_id, 1)
    assert loaded_back.payload == '{"key": "test"}'
