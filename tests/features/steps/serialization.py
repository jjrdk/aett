import datetime
import uuid

from behave import *
from pydantic_core import from_json, to_json

from aett.eventstore import EventMessage, Snapshot, Commit
from aett.eventstore.event_message import TOPIC_HEADER
from features.steps.test_types import TestEvent, SimpleMessage

use_step_matcher("re")


@given("a simple message")
def step_impl(context):
    context.message = SimpleMessage(
        id="abc",
        value="test",
        count=1,
        created=datetime.datetime.fromisoformat("2023-11-01T00:00:00.000000Z"),
        contents=["1"],
    )


@when("serializing the simple message")
def step_impl(context):
    context.serialized_message = context.message.model_dump_json()


@step("deserializing it back")
def step_impl(context):
    context.deserialized_message = SimpleMessage.model_validate_json(
        context.serialized_message
    )


@then(
    "should deserialize a message which contains the same values as the serialized message"
)
def step_impl(context):
    for key in context.message.model_dump():
        assert getattr(context.deserialized_message, key) == getattr(
            context.message, key
        )


@given("a list of event messages")
def step_impl(context):
    context.event_messages = [
        EventMessage(body="some value"),
        EventMessage(body=42),
        EventMessage(
            body=SimpleMessage(
                id="abc",
                value="test",
                count=1,
                created=datetime.datetime.fromisoformat("2023-11-01T00:00:00.000000Z"),
                contents=["1"],
            )
        ),
    ]


@when("serializing the event messages")
def step_impl(context):
    context.serialized_event_messages = to_json(
        EventMessage.to_json(msg) for msg in context.event_messages
    )


@step("deserializing them back")
def step_impl(context):
    context.deserialized_event_messages = from_json(context.serialized_event_messages)


@then("should deserialize the same number of event messages as it serialized")
def step_impl(context):
    assert len(context.deserialized_event_messages) == len(context.event_messages)


@step("should deserialize the complex types within the event messages")
def step_impl(context):
    deserialized = [
        EventMessage.from_dict(json_dict=x, topic_map=context.topic_map)
        for x in context.deserialized_event_messages
    ]
    for d, m in zip(deserialized, context.event_messages):
        assert d == m, f"{d} != {m}"


@given("a set of headers")
def step_impl(context):
    context.headers = {
        "HeaderKey": "SomeValue",
        "AnotherKey": "42",
        "AndAnotherKey": str(uuid.uuid4()),
        "LastKey": SimpleMessage(
            id="abc",
            value="test",
            count=1,
            created=datetime.datetime.fromisoformat("2023-11-01T00:00:00.000000Z"),
            contents=["1"],
        ),
    }


@when("serializing the headers")
def step_impl(context):
    context.serialized_headers = to_json(context.headers)


@step("deserializing the headers back")
def step_impl(context):
    context.deserialized_headers = from_json(context.serialized_headers)


@then("should deserialize the same number of headers as it serialized")
def step_impl(context):
    assert len(context.deserialized_headers) == len(context.headers)


@given("a snapshot")
def step_impl(context):
    context.snapshot = Snapshot(
        tenant_id="test",
        stream_id=str(uuid.uuid4()),
        stream_revision=42,
        payload={},
        headers=dict(),
        commit_sequence=1,
    )


@when("serializing the snapshot")
def step_impl(context):
    context.serialized_snapshot = to_json(context.snapshot)


@step("deserializing the snapshot back")
def step_impl(context):
    context.deserialized_snapshot = Snapshot.model_validate_json(
        context.serialized_snapshot
    )


@then("should correctly deserialize the untyped payload contents")
def step_impl(context):
    snapshot: Snapshot = context.deserialized_snapshot
    assert isinstance(snapshot.payload, dict)


@given("an event message with a topic in a commit")
def step_impl(context):
    context.commit = Commit(
        tenant_id="0b169a1b-16c3-4f3d-85de-0008885ce150",
        stream_id="95406804-784a-4649-9e10-217553e62519",
        stream_revision=1,
        commit_id=uuid.UUID("d965f869-f796-4890-8f06-f316a67b9fd4"),
        commit_sequence=1,
        commit_stamp=datetime.datetime.fromtimestamp(0, datetime.timezone.utc),
        checkpoint_token=1,
        headers={},
        events=[
            EventMessage(
                body=TestEvent(
                    source="source",
                    version=1,
                    timestamp=datetime.datetime.fromisoformat(
                        "2023-11-01T00:00:00.000000Z"
                    ),
                    value=42,
                ),
                headers={TOPIC_HEADER: "Test"},
            )
        ],
    )

    @when("serializing the event message")
    def step_impl(context):
        context.serialized_commit = to_json(context.commit)

    @then("serializes the events as an array of strings")
    def step_impl(context):
        j = Commit.model_validate_json(context.serialized_commit)
        assert isinstance(j, Commit)
        event_messages = [
            EventMessage.from_dict(x.model_dump(), topic_map=context.topic_map)
            for x in j.events
        ]
        is_test_event = [type(x.body).__name__ == "TestEvent" for x in event_messages]

        assert all(is_test_event)
