import inspect
import os.path
import uuid

from aett.eventstore import TopicMap
from steps.test_types import TestEvent


def before_scenario(context, _):
    context.tenant_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    tm = TopicMap()
    tm.register_module(inspect.getmodule(TestEvent))
    context.topic_map = tm


def after_scenario(context, _):
    if hasattr(context, 'mgmt'):
        context.mgmt.drop()
    if hasattr(context, 'db') and isinstance(context.db, str) and os.path.exists(context.db):
        os.remove(context.db)
