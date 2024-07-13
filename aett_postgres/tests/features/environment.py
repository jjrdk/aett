import inspect
import subprocess
import time
import uuid

import psycopg

from aett.eventstore import TopicMap
from aett.postgres import PersistenceManagement
from aett_postgres.tests.features.steps.Types import TestEvent


def before_scenario(context, _):
    context.tenant_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    context.process = subprocess.Popen(
        "docker run -p 5432:5432 -e POSTGRES_PASSWORD=aett -e POSTGRES_USER=aett -e POSTGRES_DB=aett postgres:alpine",
        shell=True,
        stdout=None,
        stderr=None)
    time.sleep(3)
    context.db = "host=localhost port=5432 dbname=aett user=aett password=aett"
    tm = TopicMap()
    tm.register_module(inspect.getmodule(TestEvent))
    context.topic_map = tm
    mgmt = PersistenceManagement(context.db, tm)
    mgmt.initialize()
    context.mgmt = mgmt
    with psycopg.connect(context.db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pg_catalog.pg_tables WHERE pg_tables.schemaname = 'public';")
            tables = cur.fetchall()
            assert len(tables) == 2


def after_scenario(context, _):
    if context.process:
        context.process.terminate()
        print("Terminated Docker process")
