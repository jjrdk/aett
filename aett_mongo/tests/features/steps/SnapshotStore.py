import uuid

from behave import *

from aett.mongodb import SnapshotStore
from aett.eventstore import Snapshot

use_step_matcher("re")


@step("I have a snapshot store")
def step_impl(context):
    context.store = SnapshotStore(context.db)


@step("make a snapshot of the stream")
def step_impl(context):
    context.tenant_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    snapshot = Snapshot(tenant_id=context.tenant_id, stream_id=context.stream_id, stream_revision=1,
                        payload='{"key": "test"}', headers={})
    context.store.add(snapshot)


@then("the snapshot is persisted to the store")
def step_impl(context):
    loaded_back: Snapshot = context.store.get(context.tenant_id, context.stream_id, 1)
    assert loaded_back.payload == '{"key": "test"}'
