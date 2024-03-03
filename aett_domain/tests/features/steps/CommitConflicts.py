import dataclasses
import datetime

from aett.domain import ConflictDetector, ConflictDelegate
from aett.eventstore import DomainEvent, EventStream, EventMessage
from behave import *

use_step_matcher("re")


@dataclasses.dataclass(frozen=True, kw_only=True)
class TestAddEvent(DomainEvent):
    message: str
    author: str


class VersionChecker(ConflictDelegate):
    def detect(self, uncommitted: TestAddEvent, committed: TestAddEvent) -> bool:
        return uncommitted.version <= committed.version


def convert_row(row):
    return TestAddEvent(source='test', timestamp=datetime.datetime.now(), version=int(row['version']),
                        message=row['message'], author=row['author'])


@given("an empty conflict detector")
def step_impl(context):
    context.detector = ConflictDetector()


@given("a configured conflict detector")
def step_impl(context):
    context.detector = ConflictDetector([VersionChecker()])


@given("a stream with the following commits")
def step_impl(context):
    committed = map(convert_row, context.table)
    stream = EventStream.create('test', 'stream')
    for event in committed:
        stream.add(EventMessage(body=event))
    stream.set_persisted(1)
    context.stream = stream


@when("I want to add commits in the stream")
def step_impl(context):
    new_rows = map(convert_row, context.table)
    for event in new_rows:
        context.stream.add(EventMessage(body=event))


@then("I should see no conflicting commits")
def step_impl(context):
    detector: ConflictDetector = context.detector
    uncommitted_events = [e for e in map(lambda msg: msg.body, context.stream.uncommitted)]
    committed_events = [e for e in map(lambda msg: msg.body, context.stream.committed)]
    assert not detector.conflicts_with(uncommitted_events, committed_events)


@then("I should see a commit conflict")
def step_impl(context):
    detector: ConflictDetector = context.detector
    uncommitted_events = [e for e in map(lambda msg: msg.body, context.stream.uncommitted)]
    committed_events = [e for e in map(lambda msg: msg.body, context.stream.committed)]
    assert detector.conflicts_with(uncommitted_events, committed_events)
