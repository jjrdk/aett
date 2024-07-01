import uuid

from behave import *

from aett.postgres import SnapshotStore
from aett.eventstore import Snapshot

use_step_matcher("re")


@given("I have a snapshot store")
def step_impl(context):
    context.store = SnapshotStore(context.db)


@step("make a snapshot of the stream")
def step_impl(context):
    snapshot = Snapshot(tenant_id=context.tenant_id, stream_id=context.stream_id, stream_revision=1,
                        payload='{"key": "test"}', headers={}, commit_sequence=1)
    context.store.add(snapshot, {})


@then("the snapshot is persisted to the store")
def step_impl(context):
    loaded_back: Snapshot = context.store.get(context.tenant_id, context.stream_id, 1)
    assert loaded_back.payload == '{"key": "test"}'
