import dataclasses
import datetime

from aett.eventstore import DomainEvent
from behave import *

use_step_matcher("re")


@dataclasses.dataclass(frozen=True, kw_only=True)
class TestAddEvent(DomainEvent):
    message: str
    author: str


@given("a stream with the following commits")
def step_impl(context):
    factory = lambda row: TestAddEvent(source='test', timestamp=datetime.datetime.now(), version=int(row['version']),
                                       message=row['message'], author=row['author'])
    committed = [factory(row) for row in context.table]


@when("I want to add a non conflicting commits in the stream")
def step_impl(context):
    pass


@then("I should see no conflicting commits")
def step_impl(context):
    pass


@given("a conflict detector")
def step_impl(context):
    pass
