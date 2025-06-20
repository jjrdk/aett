import datetime
import uuid

from behave import *
from behave.api.async_step import async_run_until_complete

from aett.domain import (
    ConflictDetector,
    ConflictingCommitException,
    NonConflictingCommitException,
)
from aett.eventstore import EventMessage, Commit
from test_types import TestEventConflictDelegate, TestEvent
from storage_factory import create_async_commit_store, create_commit_store

use_step_matcher("re")


@when("I have an async commit store with a conflict detector")
def step_impl(context):
    conflict_detector = ConflictDetector([TestEventConflictDelegate()])
    context.store = create_async_commit_store(
        context.db, context.storage_type, context.topic_map, conflict_detector
    )


@when("I have an commit store with a conflict detector")
def step_impl(context):
    conflict_detector = ConflictDetector([TestEventConflictDelegate()])
    context.store = create_commit_store(
        context.db, context.storage_type, context.topic_map, conflict_detector, context=context
    )


@step("I commit a conflicting event async to the stream")
@async_run_until_complete
async def step_impl(context):
    try:
        await context.store.commit(
            Commit(
                tenant_id=context.tenant_id,
                stream_id=context.stream_id,
                commit_id=uuid.uuid4(),
                commit_sequence=1,
                commit_stamp=datetime.datetime.now(datetime.timezone.utc),
                stream_revision=1,
                events=[
                    EventMessage(
                        body=TestEvent(
                            source="test",
                            timestamp=datetime.datetime.now(),
                            version=1,
                            value=0,
                        )
                    )
                ],
                checkpoint_token=1,
                headers={},
            )
        )
    except Exception as e:
        context.exception = e


@step("I commit a conflicting event to the stream")
def step_impl(context):
    try:
        context.store.commit(
            Commit(
                tenant_id=context.tenant_id,
                stream_id=context.stream_id,
                commit_id=uuid.uuid4(),
                commit_sequence=1,
                commit_stamp=datetime.datetime.now(datetime.timezone.utc),
                stream_revision=1,
                events=[
                    EventMessage(
                        body=TestEvent(
                            source="test",
                            timestamp=datetime.datetime.now(),
                            version=1,
                            value=0,
                        )
                    )
                ],
                checkpoint_token=1,
                headers={},
            )
        )
    except Exception as e:
        context.exception = e


@step("I commit a non-conflicting event async to the stream")
@async_run_until_complete
async def step_impl(context):
    try:
        await context.store.commit(
            Commit(
                tenant_id=context.tenant_id,
                stream_id=context.stream_id,
                commit_id=uuid.uuid4(),
                commit_sequence=1,
                commit_stamp=datetime.datetime.now(datetime.timezone.utc),
                stream_revision=1,
                events=[
                    EventMessage(
                        body=TestEvent(
                            source="test",
                            timestamp=datetime.datetime.now(),
                            version=1,
                            value=1,
                        )
                    )
                ],
                checkpoint_token=1,
                headers={},
            )
        )
    except Exception as e:
        context.exception = e


@step("I commit a non-conflicting event to the stream")
def step_impl(context):
    try:
        context.store.commit(
            Commit(
                tenant_id=context.tenant_id,
                stream_id=context.stream_id,
                commit_id=uuid.uuid4(),
                commit_sequence=1,
                commit_stamp=datetime.datetime.now(datetime.timezone.utc),
                stream_revision=1,
                events=[
                    EventMessage(
                        body=TestEvent(
                            source="test",
                            timestamp=datetime.datetime.now(),
                            version=1,
                            value=1,
                        )
                    )
                ],
                checkpoint_token=1,
                headers={},
            )
        )
    except Exception as e:
        context.exception = e


@then("a conflict exception is thrown")
def step_impl(context):
    assert hasattr(context, "exception")
    assert isinstance(context.exception, ConflictingCommitException), (
        f"Expected ConflictingCommitException, got {context.exception}"
    )


@then("a non-conflict exception is thrown")
def step_impl(context):
    assert hasattr(context, "exception")
    assert isinstance(context.exception, NonConflictingCommitException), (
        f"Expected NonConflictingCommitException, got {context.exception}"
    )
