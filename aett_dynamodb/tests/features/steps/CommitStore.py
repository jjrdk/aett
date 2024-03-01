import datetime
import uuid

from behave import *

from aett.dyanmodb.EventStore import CommitStore
from aett.eventstore.EventStream import EventStream
from features.steps.Types import TestEvent

use_step_matcher("re")


@step("I have a commit store")
def step_impl(context):
    context.store = CommitStore(region='localhost')


@step("I commit an event to the stream")
def step_impl(context):
    context.bucket_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    stream: EventStream = EventStream.create(context.bucket_id, context.stream_id)
    stream.add(TestEvent(id='test', timestamp=datetime.datetime.now(), version=1, value=0))
    context.store.commit(stream, uuid.uuid4())


@then("the event is persisted to the store")
def step_impl(context):
    store: CommitStore = context.store
    stream: EventStream = store.get(context.bucket_id, context.stream_id, 0, 1)
    assert stream.version == 1
