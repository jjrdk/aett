from behave import *
from behave.api.async_step import async_run_until_complete

from aett.eventstore import Snapshot
from aett.postgresasync import AsyncSnapshotStore

use_step_matcher("re")


@given("I have a snapshot store")
def step_impl(context):
    context.store = AsyncSnapshotStore(context.db)


@step("make a snapshot of the stream")
@async_run_until_complete
async def step_impl(context):
    snapshot = Snapshot(tenant_id=context.tenant_id, stream_id=context.stream_id, stream_revision=1,
                        payload='{"key": "test"}', headers={}, commit_sequence=1)
    await context.store.add(snapshot, {})


@then("the snapshot is persisted to the store")
@async_run_until_complete
async def step_impl(context):
    loaded_back: Snapshot = await context.store.get(context.tenant_id, context.stream_id, 1)
    assert loaded_back.payload == '{"key": "test"}'
