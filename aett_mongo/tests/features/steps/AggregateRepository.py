import datetime
import time
import uuid
import Types
import pymongo.database
from behave import *
from pydantic_core import to_json
from aett.domain import DefaultAggregateRepository
from aett.eventstore import TopicMap, EventMessage
from aett.mongodb.persistence_management import PersistenceManagement
from aett.mongodb.snapshot_store import SnapshotStore
from aett.mongodb.commit_store import CommitStore
from Types import TestAggregate, TestEvent, TestMemento

use_step_matcher("re")


@step("I run the setup script")
def step_impl(context):
    context.db = pymongo.database.Database(pymongo.MongoClient('mongodb://localhost:27017'), 'test')
    tm = TopicMap()
    tm.register_module(Types)
    context.mgmt = PersistenceManagement(context.db, tm)
    context.mgmt.initialize()


@step("a persistent aggregate repository")
def step_impl(context):
    context.stream_id = str(uuid.uuid4())
    tm = TopicMap()
    tm.register_module(Types)
    context.tenant_id = str(uuid.uuid4())
    context.repository = DefaultAggregateRepository(context.tenant_id, CommitStore(context.db, topic_map=tm),
                                                    SnapshotStore(context.db))


@then("a specific aggregate type can be loaded from the repository")
def step_impl(context):
    aggregate = context.repository.get(TestAggregate, context.stream_id)
    assert isinstance(aggregate, TestAggregate), f"Expected TestAggregate, got {type(aggregate)}"


@step("an aggregate is loaded from the repository and modified")
def step_impl(context):
    aggregate: TestAggregate = context.repository.get(TestAggregate, context.stream_id)
    aggregate.set_value(10)
    context.aggregate = aggregate


@step("an aggregate is saved to the repository")
def step_impl(context):
    context.repository.save(context.aggregate)


@then("the modified is saved to storage")
def step_impl(context):
    m = context.repository.get(TestAggregate, context.stream_id)
    assert m.value == 10


@step("loaded again")
def step_impl(context):
    a = context.repository.get(TestAggregate, context.stream_id)
    context.aggregate = a


@then("the modified aggregate is loaded from storage")
def step_impl(context):
    m: TestMemento = context.aggregate.get_memento()
    assert m.payload['key'] == 1010


@when("a series of commits is persisted")
def step_impl(context):
    start_time = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    collection = context.db.get_collection('commits')
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
        _: pymongo.results.InsertOneResult = collection.insert_one(doc)


@step("a specific aggregate is loaded at a specific time")
def step_impl(context):
    date_to_load = datetime.datetime.fromtimestamp(0, datetime.timezone.utc) + datetime.timedelta(days=5, hours=12)
    repo: DefaultAggregateRepository = context.repository
    context.aggregate = repo.get_to(TestAggregate, "time_test", date_to_load)


@then('the aggregate is loaded at version (\\d+)')
def step_impl(context, version):
    agg: TestAggregate = context.aggregate
    print(agg.version)
    assert agg.version == int(version)


@when('(\\d+) events are persisted to an aggregate')
def step_impl(context, count):
    context.stream_id = str(uuid.uuid4())
    start_time = time.time()
    for i in range(0, int(count)):
        agg: TestAggregate = context.repository.get(TestAggregate, context.stream_id)
        agg.raise_event(
            TestEvent(source=context.stream_id, timestamp=datetime.datetime.now(datetime.timezone.utc), version=i + 1, value=i))
        context.repository.save(agg)
        time.sleep(0.1)
    end_time = time.time()
    elapsed = end_time - start_time
    print(elapsed)


@when("an aggregated is created from multiple events")
def step_impl(context):
    agg: TestAggregate = TestAggregate(context.stream_id, 0, None)
    for i in range(0, 10):
        agg.add_value(1)
    repo: DefaultAggregateRepository = context.repository
    repo.save(agg)


@step("the aggregate is snapshotted")
def step_impl(context):
    repo: DefaultAggregateRepository = context.repository
    repo.snapshot(TestAggregate,context.stream_id)


@step("additional events are added")
def step_impl(context):
    repo: DefaultAggregateRepository = context.repository
    agg: TestAggregate = repo.get(TestAggregate, context.stream_id)
    agg.add_value(1)
    repo.save(agg)


@step("the latest version is loaded")
def step_impl(context):
    context.aggregate = context.repository.get(TestAggregate, context.stream_id)


@then("the aggregate is loaded from the snapshot and later events")
def step_impl(context):
    assert context.aggregate.version == 11
    assert context.aggregate.value == 1011
