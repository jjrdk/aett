import datetime

from behave import *

from aett.domain.Domain import Saga
from aett.eventstore.EventStream import EventStream
from aett_domain.tests.features.steps.Types import TestEvent

use_step_matcher("re")


class TestSaga(Saga):
    def __init__(self, stream: EventStream):
        super().__init__(stream)

    def _apply(self, event: TestEvent):
        pass


@given("a new saga")
def step_impl(context):
    context.saga = TestSaga(EventStream.create('bucket_id', 'saga_id'))


@when("an event is applied to the saga")
def step_impl(context):
    context.saga.transition(TestEvent(id='test', version=1, timestamp=datetime.datetime.now(datetime.UTC), value=1))


@then("the saga transitions to the next state")
def step_impl(context):
    assert context.saga.version == 1
