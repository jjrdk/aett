from behave import *

from aett_domain.tests.features.steps.Types import TestAggregate
from features.steps.test_aggregate_repository import TestAggregateRepository

use_step_matcher("re")


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
