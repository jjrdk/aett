import datetime
from Types import TestEvent, TestEventConflictDelegate
from behave import *
from aett.domain import ConflictDetector, ConflictingCommitException, NonConflictingCommitException
from aett.postgres import CommitStore
from aett.eventstore import TopicMap, EventStream, EventMessage

use_step_matcher("re")


@when("I have a commit store with a conflict detector")
def step_impl(context):
    topic_map = TopicMap()
    topic_map.register(TestEvent)
    conflict_detector = ConflictDetector([TestEventConflictDelegate()])
    context.store = CommitStore(conflict_detector=conflict_detector, db=context.db, topic_map=topic_map)


@step("I commit a conflicting event to the stream")
def step_impl(context):
    stream: EventStream = EventStream.create(context.tenant_id, context.stream_id)
    stream.add(EventMessage(body=TestEvent(source='test', timestamp=datetime.datetime.now(), version=1, value=0)))
    commit = stream.to_commit()
    try:
        context.store.commit(commit)
    except Exception as e:
        context.exception = e


@step("I commit a non-conflicting event to the stream")
def step_impl(context):
    stream: EventStream = EventStream.create(context.tenant_id, context.stream_id)
    stream.add(EventMessage(body=TestEvent(source='test', timestamp=datetime.datetime.now(), version=1, value=1)))
    commit = stream.to_commit()
    try:
        context.store.commit(commit)
    except Exception as e:
        context.exception = e


@then("then a conflict exception is thrown")
def step_impl(context):
    assert hasattr(context, 'exception')
    assert isinstance(context.exception, ConflictingCommitException)


@then("then a non-conflict exception is thrown")
def step_impl(context):
    assert hasattr(context, 'exception')
    assert isinstance(context.exception, NonConflictingCommitException)
