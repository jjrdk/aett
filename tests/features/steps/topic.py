import datetime

from behave import *

from aett.eventstore import TopicMap, Topic
from test_types import (
    SampleClass,
    SampleModel,
    DerivedClass,
    SecondDerivedClass,
    TestEvent,
)

use_step_matcher("re")


@given("an empty topic map")
def step_impl(context):
    context.topic_map = TopicMap()


@given("a sample class with a topic definition")
def step_impl(context):
    context.instance = SampleClass()


@given("a model with a topic definition")
def step_impl(context):
    context.instance = SampleModel()


@given("a derived class with a topic definition")
def step_impl(context):
    context.instance = DerivedClass()


@given("a two level derived class with a topic definition")
def step_impl(context):
    context.instance = SecondDerivedClass()


@then("then (?P<topic>.+) can be resolved from the type attribute")
def step_impl(context, topic: str):
    resolved = Topic.get(context.instance)
    assert resolved == topic, f"Expected topic to be '{topic}', but was '{resolved}'"


@when("I register it with the TopicMap")
def step_impl(context):
    context.topic_map = TopicMap()
    context.topic_map.register(context.instance)


@then("the topic is list of all topics")
def step_impl(context):
    topic_map: TopicMap = context.topic_map
    assert len(topic_map.get_all()) == 1, (
        f"Expected 1 topic, but got {len(topic_map.get_all())}"
    )


@given("a class with a topic annotation")
def step_impl(context):
    context.instance = TestEvent(
        source="test",
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        version=0,
        value=100,
    )


@then("the topic can be resolved from the type")
def step_impl(context):
    topic = Topic.get(context.instance)
    assert topic == "Test", f"Expected topic to be 'Test', but was '{topic}'"


@then("the topic map can resolve the type from the topic")
def step_impl(context):
    topic_map: TopicMap = context.topic_map
    resolved = topic_map.get(Topic.get(type(context.instance)))

    assert isinstance(context.instance, resolved), (
        f"Expected type to be '{type(context.instance)}', but was '{resolved}'"
    )
