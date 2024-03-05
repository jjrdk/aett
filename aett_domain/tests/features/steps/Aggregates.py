import datetime

import jsonpickle.pickler
from behave import *

from aett.eventstore import EventMessage
from aett_domain.tests.features.steps.Types import TestEvent, TestAggregate

use_step_matcher("re")


@when("an event is applied to the aggregate")
def step_impl(context):
    a: TestAggregate = context.aggregate
    a.raise_event(TestEvent(source='test', timestamp=datetime.datetime.now(datetime.UTC), version=1, value=1))


@then("the aggregate version is (\\d+)")
def step_impl(context, version: str):
    a: TestAggregate = context.aggregate
    assert a.version == int(version)


@given("a deserialized stream of events")
def step_impl(context):
    event1: TestEvent = TestEvent(source='test', timestamp=datetime.datetime.now(datetime.UTC), version=1, value=1)
    event2: TestEvent = TestEvent(source='test', timestamp=datetime.datetime.now(datetime.UTC), version=2, value=1)
    msgs = [EventMessage(body=event1, headers=None, topic='TestEvent'),
            EventMessage(body=event2, headers=None, topic='TestEvent')]
    j = jsonpickle.encode(msgs)
    context.events = jsonpickle.decode(j)


@given("an aggregate")
def step_impl(context):
    agg = TestAggregate('test')
    context.aggregate = agg


@when("the events are applied to the aggregate")
def step_impl(context):
    a: TestAggregate = context.aggregate
    for e in context.events:
        a.raise_event(e.body)
