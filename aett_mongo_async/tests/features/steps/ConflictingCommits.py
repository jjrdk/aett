import datetime
import uuid

from behave import *
from behave.api.async_step import async_run_until_complete

from Types import TestEvent, TestEventConflictDelegate
from aett.domain.conflict_detector import ConflictDetector
from aett.domain.conflicting_commit_exception import ConflictingCommitException
from aett.domain.non_conflicting_commit_exception import NonConflictingCommitException
from aett.eventstore import TopicMap, EventMessage, Commit
from aett.mongodbasync.async_commit_store import AsyncCommitStore

use_step_matcher("re")


@step("I have a commit store with a conflict detector")
def step_impl(context):
    topic_map = TopicMap()
    topic_map.register(TestEvent)
    conflict_detector = ConflictDetector([TestEventConflictDelegate()])
    context.store = AsyncCommitStore(conflict_detector=conflict_detector, db=context.db, topic_map=topic_map)


@step("I commit a conflicting event to the stream")
@async_run_until_complete
async def step_impl(context):
    try:
        await context.store.commit(
            Commit(tenant_id=context.tenant_id,
                   stream_id=context.stream_id,
                   commit_id=uuid.uuid4(),
                   commit_sequence=1,
                   commit_stamp=datetime.datetime.now(datetime.timezone.utc),
                   stream_revision=1,
                   events=[
                       EventMessage(
                           body=TestEvent(source='test',
                                          timestamp=datetime.datetime.now(),
                                          version=1,
                                          value=0))],
                   checkpoint_token=1, headers={}))
    except Exception as e:
        context.exception = e


@step("I commit a non-conflicting event to the stream")
@async_run_until_complete
async def step_impl(context):
    try:
        await context.store.commit(
            Commit(tenant_id=context.tenant_id,
                   stream_id=context.stream_id,
                   commit_id=uuid.uuid4(),
                   commit_sequence=1,
                   commit_stamp=datetime.datetime.now(datetime.timezone.utc),
                   stream_revision=1,
                   events=[
                       EventMessage(
                           body=TestEvent(source='test',
                                          timestamp=datetime.datetime.now(),
                                          version=1,
                                          value=1))],
                   checkpoint_token=1, headers={}))
    except Exception as e:
        context.exception = e


@then("then a conflict exception is thrown")
def step_impl(context):
    assert hasattr(context, 'exception'), "Expected exception to be thrown"
    assert isinstance(context.exception,
                      ConflictingCommitException), \
        f"Expected exception to be of type ConflictingCommitException, but was {type(context.exception)}"


@then("then a non-conflict exception is thrown")
def step_impl(context):
    assert hasattr(context, 'exception'), "Expected exception to be thrown"
    assert isinstance(context.exception,
                      NonConflictingCommitException), \
        f"Expected exception to be of type NonConflictingCommitException, but was {type(context.exception)}"