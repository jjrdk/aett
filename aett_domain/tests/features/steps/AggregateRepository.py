import typing
from behave import *

from aett.domain import AggregateRepository, Aggregate
from aett.eventstore import EventStream, MAX_INT
from aett_domain.tests.features.steps.Types import TestAggregate

use_step_matcher("re")


class TestAggregateRepository(AggregateRepository):
    TAggregate = typing.TypeVar('TAggregate', bound=Aggregate)

    def __init__(self, storage: {}):
        self.storage: dict = storage

    def get(self, cls: typing.Type[TAggregate], identifier: str, version: int = MAX_INT) -> TestAggregate:
        m = self.storage.get(identifier)
        agg = cls(identifier)
        agg.apply_memento(m)
        return agg

    def save(self, aggregate: TAggregate) -> None:
        self.storage[aggregate.id] = aggregate.get_memento()


@given("an aggregate repository")
def step_impl(context):
    storage = {}
    context.storage = storage
    context.repository = TestAggregateRepository(storage)


@then("a specific aggregate type can be loaded from the repository")
def step_impl(context):
    aggregate = context.repository.get(TestAggregate, "test")
    assert isinstance(aggregate, TestAggregate)


@step("an aggregate is loaded from the repository and modified")
def step_impl(context):
    aggregate: TestAggregate = context.repository.get(TestAggregate, "test")
    aggregate.set_value(10)
    context.aggregate = aggregate


@step("an aggregate is saved to the repository")
def step_impl(context):
    context.repository.save(context.aggregate)


@then("the modified is saved to storage")
def step_impl(context):
    m = context.repository.get(TestAggregate, "test", 0)
    assert m.value == 10


@step("loaded again")
def step_impl(context):
    a = context.repository.get(TestAggregate, "test", 0)
    context.aggregate = a


@then("the modified aggregate is loaded from storage")
def step_impl(context):
    m = context.aggregate.get_memento()
    assert m.value == 10
