import datetime
import uuid

from behave import *

from aett.eventstore import EventStream, EventMessage
from aett.inmemory import CommitStore
from Types import TestEvent

use_step_matcher("re")


@given("I have a commit store")
def step_impl(context):
    context.store = CommitStore()


@when("I commit an event to the stream")
def step_impl(context):
    context.bucket_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    stream: EventStream = EventStream.create(context.bucket_id, context.stream_id)
    stream.add(EventMessage(body=TestEvent(source='test',
                                           timestamp=datetime.datetime.now(),
                                           version=1,
                                           value=0)))
    context.store.commit(stream.to_commit())


@then("the event is persisted to the store")
def step_impl(context):
    store: CommitStore = context.store
    stream = [commit for commit in store.get(context.bucket_id, context.stream_id, 0, 1)]
    assert stream[0].events[0].body.version == 1
