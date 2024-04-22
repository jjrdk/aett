import datetime
import time
import uuid

import boto3
import jsonpickle
from behave import *
import Types
from aett.domain import DefaultAggregateRepository
from aett.dynamodb import PersistenceManagement, CommitStore, SnapshotStore
from aett.eventstore import TopicMap, EventMessage
from Types import TestAggregate, TestEvent

use_step_matcher("re")


@step("I run the setup script")
def step_impl(context):
    context.mgmt = PersistenceManagement(region='localhost')
    context.mgmt.initialize()


@step("a persistent aggregate repository")
def step_impl(context):
    tm = TopicMap()
    tm.register_module(Types)
    context.stream_id = str(uuid.uuid4())
    context.tenant_id = str(uuid.uuid4())
    context.repository = DefaultAggregateRepository(context.tenant_id, CommitStore(region='localhost', topic_map=tm),
                                                    SnapshotStore(region='localhost'))


@then("a specific aggregate type can be loaded from the repository")
def step_impl(context):
    aggregate = context.repository.get(TestAggregate, context.stream_id)
    assert isinstance(aggregate, TestAggregate)


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
    m = context.aggregate.get_memento()
    assert m.value == 10


@when("a series of commits is persisted")
def step_impl(context):
    resource = boto3.resource('dynamodb',
                              region_name='localhost',
                              endpoint_url='http://localhost:8000')
    table = resource.Table('commits')
    start_time = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    for x in range(1, 10):
        time_stamp = (start_time + datetime.timedelta(days=x))
        item = {
            'TenantAndStream': f'{context.tenant_id}{context.stream_id}',
            'TenantId': context.tenant_id,
            'StreamId': context.stream_id,
            'StreamRevision': x,
            'CommitId': str(uuid.uuid4()),
            'CommitSequence': x,
            'CommitStamp': int(time_stamp.timestamp()),
            'Headers': jsonpickle.encode({}, unpicklable=False),
            'Events': jsonpickle.encode([e.to_json() for e in [
                EventMessage(body=TestEvent(source=context.stream_id, timestamp=time_stamp, version=x - 1, value=x))]],
                                        unpicklable=False)
        }
        response = table.put_item(
            TableName='commits',
            Item=item,
            ReturnValues='NONE',
            ReturnValuesOnConditionCheckFailure='NONE',
            ConditionExpression='attribute_not_exists(TenantAndStream) AND attribute_not_exists(CommitSequence)')
        print(response)


@step("a specific aggregate is loaded at a specific time")
def step_impl(context):
    date_to_load = datetime.datetime.fromtimestamp(0, datetime.timezone.utc) + datetime.timedelta(days=5, hours=12)
    repo: DefaultAggregateRepository = context.repository
    context.aggregate = repo.get_to(TestAggregate, context.stream_id, date_to_load)


@then('the aggregate is loaded at version (\\d+)')
def step_impl(context, version):
    agg: TestAggregate = context.aggregate
    print(agg.version)
    assert agg.version == int(version)


@when('(\\d+) events are persisted to an aggregate')
def step_impl(context, count):
    start_time = time.time()
    for i in range(0, int(count)):
        agg: TestAggregate = context.repository.get(TestAggregate, context.stream_id)
        agg.raise_event(
            TestEvent(source=context.stream_id, timestamp=datetime.datetime.now(datetime.UTC), version=i + 1, value=i))
        context.repository.save(agg)
    end_time = time.time()
    elapsed = end_time - start_time
    print(elapsed)
