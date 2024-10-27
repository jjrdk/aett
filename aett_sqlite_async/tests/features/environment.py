import inspect
import os.path
import uuid

from aett.eventstore import TopicMap
from aett.sqlite import PersistenceManagement
from aett_sqlite_async.tests.features.steps.Types import TestEvent


def before_scenario(context, _):
    context.tenant_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    context.db = f"{context.tenant_id}.db"
    tm = TopicMap()
    tm.register_module(inspect.getmodule(TestEvent))
    context.topic_map = tm
    mgmt = PersistenceManagement(context.db, tm)
    mgmt.initialize()
    context.mgmt = mgmt


def after_scenario(context, _):
    mgmt: PersistenceManagement = context.mgmt
    mgmt.drop()
    if os.path.exists(context.db):
        os.remove(context.db)
        print(f"Deleted {context.db}")
