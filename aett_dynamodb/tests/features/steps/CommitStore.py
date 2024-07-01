import datetime
import uuid

from behave import *

from Types import TestEvent
from aett.dynamodb import CommitStore
from aett.eventstore import EventMessage, TopicMap, Commit

use_step_matcher("re")


@step("I have a commit store")
def step_impl(context):
    topic_map = TopicMap()
    topic_map.register(TestEvent)
    context.store = CommitStore(region='localhost', topic_map=topic_map)


@step("I commit an event to the stream")
def step_impl(context):
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
    context.store.commit(commit)


@then("the event is persisted to the store")
def step_impl(context):
    store: CommitStore = context.store
    stream = [commit for commit in store.get(context.tenant_id, context.stream_id, 0, 1)]
    assert stream[0].events[0].body.version == 1
