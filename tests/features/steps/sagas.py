import datetime

from behave import *

from test_types import TestEvent, TestSaga

use_step_matcher("re")


@given("a new saga")
def step_impl(context):
    context.saga = TestSaga("saga_id", 0)


@when("an event is applied to the saga")
def step_impl(context):
    context.saga.transition(
        TestEvent(
            source="test",
            version=1,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            value=1,
        )
    )


@then("the saga transitions to the next state")
def step_impl(context):
    assert context.saga.version == 1


@then("a command is emitted from the saga")
def step_impl(context):
    assert 'UndispatchedMessage.0' in context.saga.headers


@step("can be read from the persisted event headers")
def step_impl(context):
    raise NotImplementedError(u'STEP: And can be read from the persisted event headers')