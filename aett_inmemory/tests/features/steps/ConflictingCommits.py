import datetime
import uuid

from behave import *

from Types import TestEvent, TestEventConflictDelegate
from aett.domain import ConflictDetector, ConflictingCommitException, NonConflictingCommitException
from aett.eventstore import TopicMap, EventMessage, Commit
from aett.inmemory import CommitStore

use_step_matcher("re")


@step("I have a commit store with a conflict detector")
def step_impl(context):
    topic_map = TopicMap()
    topic_map.register(TestEvent)
    conflict_detector = ConflictDetector([TestEventConflictDelegate()])
    context.store = CommitStore(conflict_detector=conflict_detector)


@step("I commit a conflicting event to the stream")
def step_impl(context):
    try:
        context.store.commit(
            Commit(tenant_id=context.tenant_id, stream_id=context.stream_id, stream_revision=1, commit_id=uuid.uuid4(),
                   commit_sequence=1, commit_stamp=datetime.datetime.now(), headers={}, events=[
                    EventMessage(body=TestEvent(source='test', timestamp=datetime.datetime.now(), version=1, value=0))],
                   checkpoint_token=0))
    except Exception as e:
        context.exception = e


@step("I commit a non-conflicting event to the stream")
def step_impl(context):
    try:
        context.store.commit(
            Commit(tenant_id=context.tenant_id, stream_id=context.stream_id, stream_revision=1, commit_id=uuid.uuid4(),
                   commit_sequence=1, commit_stamp=datetime.datetime.now(), headers={}, events=[
                    EventMessage(body=TestEvent(source='test', timestamp=datetime.datetime.now(), version=1, value=1))],
                   checkpoint_token=0))
    except Exception as e:
        context.exception = e


@then("then a conflict exception is thrown")
def step_impl(context):
    assert isinstance(context.exception, ConflictingCommitException)


@then("then a non-conflict exception is thrown")
def step_impl(context):
    assert hasattr(context, 'exception')
    assert isinstance(context.exception, NonConflictingCommitException)
