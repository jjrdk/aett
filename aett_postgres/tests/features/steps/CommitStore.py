import datetime
import uuid

from behave import *

from aett.eventstore import EventMessage, Commit
from aett.postgres.commit_store import CommitStore
from aett_postgres.tests.features.steps.Types import TestEvent, NestedEvent, NestedValue, DeepNestedValue

use_step_matcher("re")


@given("I have a commit store")
def step_impl(context):
    context.store = CommitStore(context.db, context.topic_map)


@when("I commit an event to the stream")
def step_impl(context):
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
    stream = list(store.get(context.tenant_id, context.stream_id, 0, 1))
    context.stream = stream
    assert stream[0].events[0].body.version == 1


@when("I commit an event with nested base models to the stream")
def step_impl(context):
    evt = NestedEvent(source='test',
                      timestamp=datetime.datetime.now(),
                      version=1,
                      value=NestedValue(value=DeepNestedValue(value=0)))
    commit = Commit(tenant_id=context.tenant_id,
                    stream_id=context.stream_id,
                    stream_revision=1,
                    commit_id=uuid.uuid4(),
                    commit_sequence=1,
                    commit_stamp=datetime.datetime.now(datetime.timezone.utc),
                    headers={},
                    events=[EventMessage(body=evt)],
                    checkpoint_token=0)
    context.store.commit(commit)


@step("the nested types are preserved")
def step_impl(context):
    stream = context.stream
    assert isinstance(stream[0].events[0].body.value.value, DeepNestedValue)
