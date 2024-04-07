import datetime
import uuid

import jsonpickle

import features
import pymongo.database
from behave import *

from aett.domain import DefaultAggregateRepository
from aett.eventstore import TopicMap, EventMessage
from aett.mongodb import PersistenceManagement, CommitStore, SnapshotStore
from features.steps.Types import TestAggregate, TestEvent

use_step_matcher("re")


@step("I run the setup script")
def step_impl(context):
    context.db = pymongo.database.Database(pymongo.MongoClient('mongodb://localhost:27017'), 'test')
    context.mgmt = PersistenceManagement(context.db)
    context.mgmt.initialize()


@step("a persistent aggregate repository")
def step_impl(context):
    tm = TopicMap()
    tm.register_module(features.steps.Types)
    context.bucket_id = str(uuid.uuid4())
    context.repository = DefaultAggregateRepository(context.bucket_id, CommitStore(context.db, topic_map=tm),
                                                    SnapshotStore(context.db))


@then("a specific aggregate type can be loaded from the repository")
def step_impl(context):
    aggregate = context.repository.get(TestAggregate, "test")
    assert isinstance(aggregate, TestAggregate)


@step("an aggregate is loaded from the repository and modified")
def step_impl(context):
    aggregate: TestAggregate = context.repository.get(TestAggregate, "test")
    aggregate.set_value(10)
    context.aggregate = aggregate


@step("an aggregate is saved to the repository")
def step_impl(context):
    context.repository.save(context.aggregate)


@then("the modified is saved to storage")
def step_impl(context):
    m = context.repository.get(TestAggregate, "test")
    assert m.value == 10


@step("loaded again")
def step_impl(context):
    a = context.repository.get(TestAggregate, "test")
    context.aggregate = a


@then("the modified aggregate is loaded from storage")
def step_impl(context):
    m = context.aggregate.get_memento()
    assert m.value == 10


@when("a series of commits is persisted")
def step_impl(context):
    start_time = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    collection = context.db.get_collection('commits')
    for x in range(1, 10):
        time_stamp = (start_time + datetime.timedelta(days=x))
        doc = {
            'BucketId': context.bucket_id,
            'StreamId': 'time_test',
            'StreamRevision': x,
            'CommitId': str(uuid.uuid4()),
            'CommitSequence': x,
            'CommitStamp': int(time_stamp.timestamp()),
            'Headers': jsonpickle.encode({}, unpicklable=False),
            'Events': jsonpickle.encode([e.to_json() for e in [
                EventMessage(body=TestEvent(source='time_test', timestamp=time_stamp, version=x - 1, value=x))]],
                                        unpicklable=False),
            'CheckpointToken': x
        }
        _: pymongo.results.InsertOneResult = collection.insert_one(doc)


@step("a specific aggregate is loaded at a specific time")
def step_impl(context):
    date_to_load = datetime.datetime.fromtimestamp(0, datetime.timezone.utc) + datetime.timedelta(days=5, hours=12)
    repo: DefaultAggregateRepository = context.repository
    context.aggregate = repo.get_to(TestAggregate, "time_test", date_to_load)


@then("the aggregate is loaded at the correct state")
def step_impl(context):
    agg: TestAggregate = context.aggregate
    assert agg.version == 5
