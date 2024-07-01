import datetime

from behave import *

from aett.domain import Saga
from aett_domain.tests.features.steps.Types import TestEvent

use_step_matcher("re")


class TestSaga(Saga):
    def __init__(self, saga_id: str, commit_sequence: int):
        super().__init__(saga_id=saga_id, commit_sequence=commit_sequence)

    def _apply(self, event: TestEvent):
        pass


@given("a new saga")
def step_impl(context):
    context.saga = TestSaga('saga_id', 0)


@when("an event is applied to the saga")
def step_impl(context):
    context.saga.transition(TestEvent(source='test', version=1, timestamp=datetime.datetime.now(datetime.timezone.utc), value=1))


@then("the saga transitions to the next state")
def step_impl(context):
    assert context.saga.version == 1
