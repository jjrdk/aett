import datetime
import time
import uuid

import asyncpg
from behave import *
from behave.api.async_step import async_run_until_complete
from pydantic_core import to_json

from Types import TestAggregate, TestEvent, TestMemento
from aett.domain import AsyncDefaultAggregateRepository
from aett.domain.default_aggregate_repository import DefaultAggregateRepository
from aett.eventstore import EventMessage
from aett.postgresasync import AsyncCommitStore, AsyncSnapshotStore

use_step_matcher("re")


@given("I have a persistent aggregate repository")
def step_impl(context):
    context.stream_id = str(uuid.uuid4())
    context.tenant_id = str(uuid.uuid4())
    context.repository = AsyncDefaultAggregateRepository(context.tenant_id,
                                                         AsyncCommitStore(context.db, context.topic_map),
                                                         AsyncSnapshotStore(context.db))


@then("a specific aggregate type can be loaded from the repository")
@async_run_until_complete
async def step_impl(context):
    aggregate = await context.repository.get(TestAggregate, context.stream_id)
    assert isinstance(aggregate, TestAggregate)


@step("an aggregate is loaded from the repository and modified")
@async_run_until_complete
async def step_impl(context):
    aggregate: TestAggregate = await context.repository.get(TestAggregate, context.stream_id)
    aggregate.set_value(10)
    context.aggregate = aggregate


@step("an aggregate is saved to the repository")
@async_run_until_complete
async def step_impl(context):
    await context.repository.save(context.aggregate)


@then("the modified is saved to storage")
@async_run_until_complete
async def step_impl(context):
    m = await context.repository.get(TestAggregate, context.stream_id)
    assert m.value == 10


@step("loaded again")
@async_run_until_complete
async def step_impl(context):
    a = await context.repository.get(TestAggregate, context.stream_id)
    context.aggregate = a


@then("the modified aggregate is loaded from storage")
def step_impl(context):
    m: TestMemento = context.aggregate.get_memento()
    assert m.payload['key'] == 1010


@when("a series of commits is persisted")
@async_run_until_complete
async def step_impl(context):
    start_time = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    for x in range(1, 10):
        time_stamp = (start_time + datetime.timedelta(days=x))
        conn = await asyncpg.connect(context.db)
        await conn.execute(f"""INSERT
                  INTO commits
                     ( TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp, Headers, Payload )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING CheckpointNumber;""", context.tenant_id, context.stream_id, context.stream_id,
                           str(uuid.uuid4()), x, x, 1,
                           time_stamp,
                           to_json({}),
                           to_json([e.to_json() for e in [EventMessage(
                               body=TestEvent(source='time_test', timestamp=time_stamp,
                                              version=x - 1,
                                              value=x))]]))


@step("a specific aggregate is loaded at a specific time")
@async_run_until_complete
async def step_impl(context):
    date_to_load = datetime.datetime.fromtimestamp(0, datetime.timezone.utc) + datetime.timedelta(days=5, hours=12)
    repo: DefaultAggregateRepository = context.repository
    context.aggregate = await repo.get_to(TestAggregate, context.stream_id, date_to_load)


@then('the aggregate is loaded at version (\\d+)')
def step_impl(context, version):
    agg: TestAggregate = context.aggregate
    assert agg.version == int(version)


@when('(\\d+) events are persisted to an aggregate')
@async_run_until_complete
async def step_impl(context, count):
    start_time = time.time()
    for i in range(0, int(count)):
        agg: TestAggregate = await context.repository.get(TestAggregate, context.stream_id)
        agg.raise_event(
            TestEvent(source=context.stream_id, timestamp=datetime.datetime.now(datetime.timezone.utc), version=i + 1,
                      value=i))
        await context.repository.save(agg)
    end_time = time.time()
    elapsed = end_time - start_time
    print(elapsed)


@when("an aggregate is created from multiple events")
@async_run_until_complete
async def step_impl(context):
    agg: TestAggregate = TestAggregate(context.stream_id, 0, None)
    for i in range(0, 10):
        agg.add_value(1)
    repo: DefaultAggregateRepository = context.repository
    await repo.save(agg)


@step("the aggregate is snapshotted")
@async_run_until_complete
async def step_impl(context):
    repo: DefaultAggregateRepository = context.repository
    await repo.snapshot(TestAggregate, context.stream_id)


@step("additional events are added")
@async_run_until_complete
async def step_impl(context):
    repo: DefaultAggregateRepository = context.repository
    agg: TestAggregate = await repo.get(TestAggregate, context.stream_id)
    agg.add_value(1)
    await repo.save(agg)


@step("the latest version is loaded")
@async_run_until_complete
async def step_impl(context):
    context.aggregate = await context.repository.get(TestAggregate, context.stream_id)


@then("the aggregate is loaded from the snapshot and later events")
def step_impl(context):
    assert context.aggregate.version == 11
    assert context.aggregate.value == 1011
