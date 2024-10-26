import datetime
import uuid

from behave.api.async_step import async_run_until_complete

import Types
from behave import *

from aett.eventstore import EventMessage, TopicMap, Commit
from aett.mongodbasync.async_commit_store import AsyncCommitStore
from Types import TestEvent

use_step_matcher("re")


@step("I have a commit store")
def step_impl(context):
    tm = TopicMap()
    tm.register_module(Types)
    context.store = AsyncCommitStore(context.db, tm)


@step("I commit an event to the stream")
@async_run_until_complete
async def step_impl(context):
    context.tenant_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    commit = Commit(tenant_id=context.tenant_id,
                    stream_id=context.stream_id,
                    stream_revision=1,
                    commit_id=uuid.uuid4(),
                    commit_sequence=1,
                    commit_stamp=datetime.datetime.now(datetime.timezone.utc),
                    headers={},
                    events=[EventMessage(body=TestEvent(source='test',
                                                        timestamp=datetime.datetime.now(),
                                                        version=1,
                                                        value=0))],
                    checkpoint_token=0)
    commit_store: AsyncCommitStore = context.store
    await commit_store.commit(commit)


@then("the event is persisted to the store")
@async_run_until_complete
async def step_impl(context):
    store: AsyncCommitStore = context.store
    stream = [commit async for commit in store.get(context.tenant_id, context.stream_id, 0, 1)]
    assert stream[0].events[0].body.version == 1
