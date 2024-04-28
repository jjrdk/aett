import datetime
import time
import uuid

import jsonpickle

import features
from behave import *

from aett.domain import DefaultAggregateRepository
from aett.eventstore import TopicMap, Commit, EventMessage
from aett.s3 import CommitStore, SnapshotStore
from Types import TestAggregate, TestEvent, TestMemento

use_step_matcher("re")


@given("I have a persistent aggregate repository")
def step_impl(context):
    tm = TopicMap()
    tm.register_module(features.steps.Types)
    context.tenant_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    context.repository = DefaultAggregateRepository(context.tenant_id,
                                                    CommitStore(s3_config=context.s3_config, topic_map=tm),
                                                    SnapshotStore(s3_config=context.s3_config))


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
    m: TestMemento = context.aggregate.get_memento()
    assert m.payload['key'] == 1010


@when("a series of commits is persisted")
def step_impl(context):
    start_time = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)
    for x in range(1, 10):
        time_stamp = (start_time + datetime.timedelta(days=x))
        commit = Commit(tenant_id=context.tenant_id, stream_id=context.stream_id, commit_stamp=time_stamp,
                        commit_sequence=x,
                        stream_revision=1,
                        events=[EventMessage(body=TestEvent(source=context.stream_id,
                                                            timestamp=time_stamp,
                                                            version=x - 1,
                                                            value=x))],
                        headers={},
                        checkpoint_token=0,
                        commit_id=uuid.uuid4())
        commit_key = f'commits/{commit.tenant_id}/{commit.stream_id}/{int(commit.commit_stamp.timestamp())}_{commit.commit_sequence}_{commit.stream_revision}.json'
        d = commit.__dict__
        d['events'] = [e.to_json() for e in commit.events]
        d['headers'] = {k: jsonpickle.encode(v, unpicklable=False) for k, v in commit.headers.items()}
        body = jsonpickle.encode(d, unpicklable=False).encode('utf-8')
        client = context.s3_config.to_client()
        client.put_object(Bucket=context.s3_config.bucket,
                          Key=commit_key,
                          Body=body,
                          ContentLength=len(body),
                          Metadata={k: jsonpickle.encode(v, unpicklable=False) for k, v in
                                    commit.headers.items()})


@step("a specific aggregate is loaded at a specific time")
def step_impl(context):
    date_to_load = datetime.datetime.fromtimestamp(0, datetime.timezone.utc) + datetime.timedelta(days=5, hours=12)
    repo: DefaultAggregateRepository = context.repository
    context.aggregate = repo.get_to(TestAggregate, context.stream_id, date_to_load)


@then('the aggregate is loaded at version (\\d+)')
def step_impl(context, version):
    agg: TestAggregate = context.aggregate
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
    repo.snapshot(TestAggregate, context.stream_id)


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
