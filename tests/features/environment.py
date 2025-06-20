import inspect
import os.path
import uuid
from typing import Coroutine

from behave.api.async_step import async_run_until_complete

from aett.eventstore import TopicMap
from steps.test_types import TestEvent


def before_scenario(context, _):
    context.tenant_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    tm = TopicMap()
    tm.register_module(inspect.getmodule(TestEvent))
    context.topic_map = tm


@async_run_until_complete
async def after_scenario(context, _):
    if hasattr(context, "mgmt"):
        try:
            dropped: Coroutine | None = context.mgmt.drop()
            if dropped:
                await dropped
        except Exception as e:
            print(e)
    if (
        hasattr(context, "db")
        and isinstance(context.db, str)
        and os.path.exists(context.db)
    ):
        try:
            os.remove(context.db)
        finally:
            pass
    if hasattr(context, "process"):
        try:
            context.process.stop()
        finally:
            pass
