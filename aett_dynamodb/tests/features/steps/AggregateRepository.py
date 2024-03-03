import uuid

from behave import *

from aett.domain import DefaultAggregateRepository
from aett.dyanmodb import PersistenceManagement, CommitStore
from features.steps.Types import TestAggregate

use_step_matcher("re")


@step("I run the setup script")
def step_impl(context):
    context.mgmt = PersistenceManagement(region='localhost')
    context.mgmt.initialize()


@step("a persistent aggregate repository")
def step_impl(context):
    context.repository = DefaultAggregateRepository(str(uuid.uuid4()), CommitStore(region='localhost'))


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
    m = context.repository.get(TestAggregate, "test")
    assert m.value == 10


@step("loaded again")
def step_impl(context):
    a = context.repository.get(TestAggregate, "test")
    context.aggregate = a


@then("the modified aggregate is loaded from storage")
def step_impl(context):
    m = context.aggregate.get_memento()
    assert m.value == 10
