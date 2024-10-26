import datetime
import time
import uuid

from behave import *
from behave.api.async_step import async_run_until_complete
from pydantic_core import to_json
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

import Types
from Types import TestAggregate, TestEvent, TestMemento
from aett.domain import AsyncDefaultAggregateRepository
from aett.eventstore import TopicMap, EventMessage
from aett.mongodbasync.async_commit_store import AsyncCommitStore
from aett.mongodbasync.async_snapshot_store import AsyncSnapshotStore
from aett.mongodbasync.async_persistence_management import AsyncPersistenceManagement

use_step_matcher("re")


@step("I run the setup script")
@async_run_until_complete
async def step_impl(context):
    context.db = AsyncDatabase(AsyncMongoClient('mongodb://localhost:27017'), 'test')
    tm = TopicMap()
    tm.register_module(Types)
    context.mgmt = AsyncPersistenceManagement(context.db, tm)
    await context.mgmt.initialize()


@step("a persistent aggregate repository")
def step_impl(context):
    context.stream_id = str(uuid.uuid4())
    tm = TopicMap()
    tm.register_module(Types)
    context.tenant_id = str(uuid.uuid4())
    context.repository = AsyncDefaultAggregateRepository(context.tenant_id, AsyncCommitStore(context.db, topic_map=tm),
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
    collection: AsyncCollection = context.db.get_collection('commits')
    for x in range(1, 10):
        time_stamp = (start_time + datetime.timedelta(days=x))
        doc = {
            'TenantId': context.tenant_id,
            'StreamId': 'time_test',
            'StreamRevision': x,
            'CommitId': str(uuid.uuid4()),
            'CommitSequence': x,
            'CommitStamp': int(time_stamp.timestamp()),
            'Headers': to_json({}),
            'Events': to_json([e.to_json() for e in [
                EventMessage(body=TestEvent(source='time_test', timestamp=time_stamp, version=x - 1, value=x))]]),
            'CheckpointToken': x
        }
        _ = await collection.insert_one(doc)


@step("a specific aggregate is loaded at a specific time")
@async_run_until_complete
async def step_impl(context):
    date_to_load = datetime.datetime.fromtimestamp(0, datetime.timezone.utc) + datetime.timedelta(days=5, hours=12)
    repo: AsyncDefaultAggregateRepository = context.repository
    context.aggregate = await repo.get_to(TestAggregate, "time_test", date_to_load)


@then('the aggregate is loaded at version (\\d+)')
def step_impl(context, version):
    agg: TestAggregate = context.aggregate
    assert agg.version == int(version), f"Expected version {version} but got {agg.version}"


@when('(\\d+) events are persisted to an aggregate')
@async_run_until_complete
async def step_impl(context, count):
    context.stream_id = str(uuid.uuid4())
    start_time = time.time()
    for i in range(0, int(count)):
        agg: TestAggregate = await context.repository.get(TestAggregate, context.stream_id)
        agg.raise_event(
            TestEvent(source=context.stream_id, timestamp=datetime.datetime.now(datetime.timezone.utc), version=i + 1,
                      value=i))
        await context.repository.save(agg)
        time.sleep(0.1)
    end_time = time.time()
    elapsed = end_time - start_time
    print(elapsed)


@when("an aggregated is created from multiple events")
@async_run_until_complete
async def step_impl(context):
    agg: TestAggregate = TestAggregate(context.stream_id, 0, None)
    for i in range(0, 10):
        agg.add_value(1)
    repo: AsyncDefaultAggregateRepository = context.repository
    await repo.save(agg)


@step("the aggregate is snapshotted")
@async_run_until_complete
async def step_impl(context):
    repo: AsyncDefaultAggregateRepository = context.repository
    await repo.snapshot(TestAggregate, context.stream_id)


@step("additional events are added")
@async_run_until_complete
async def step_impl(context):
    repo: AsyncDefaultAggregateRepository = context.repository
    agg: TestAggregate = await repo.get(TestAggregate, context.stream_id)
    agg.add_value(1)
    await repo.save(agg)


@step("the latest version is loaded")
@async_run_until_complete
async def step_impl(context):
    context.aggregate = await context.repository.get(TestAggregate, context.stream_id)


@then("the aggregate is loaded from the snapshot and later events")
def step_impl(context):
    assert context.aggregate.version == 11, f"Expected version 11 but got {context.aggregate.version}"
    assert context.aggregate.value == 1011, f"Expected value 1011 but got {context.aggregate.value}"
