import inspect
import subprocess
import time
import uuid

import psycopg

from aett.eventstore import TopicMap
from aett.postgres import PersistenceManagement
from features.steps.Types import TestEvent


def before_scenario(context, _):
    context.tenant_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    context.process = subprocess.Popen(
        "docker run -p 5432:5432 -e POSTGRES_PASSWORD=aett -e POSTGRES_USER=aett -e POSTGRES_DB=aett postgres:15.4-alpine",
        shell=True,
        stdout=None,
        stderr=None)
    time.sleep(1)
    context.db = psycopg.connect("host=localhost port=5432 dbname=aett user=aett password=aett", autocommit=True)
    tm = TopicMap()
    tm.register_module(inspect.getmodule(TestEvent))
    mgmt = PersistenceManagement(context.db, tm)
    mgmt.initialize()
    context.mgmt = mgmt
    cur = context.db.cursor()
    cur.execute("SELECT * FROM pg_catalog.pg_tables WHERE pg_tables.schemaname = 'public';")
    tables = cur.fetchall()
    assert len(tables) == 2


def after_scenario(context, _):
    if context.db:
        context.mgmt.purge(context.tenant_id)
        context.db.close()
        print("Closed DB connection")
    if context.process:
        context.process.terminate()
        print("Terminated Docker process")
