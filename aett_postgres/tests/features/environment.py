import inspect
import subprocess
import time
import uuid

import psycopg
from testcontainers.postgres import PostgresContainer

from aett.eventstore import TopicMap
from aett.postgres.persistence_management import PersistenceManagement
from aett_postgres.tests.features.steps.Types import TestEvent


def before_scenario(context, _):
    context.tenant_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    context.process = PostgresContainer(image="postgres:alpine", username="aett", password="aett", dbname="aett")
    context.process.start()
    context.db = f"host={context.process.get_container_host_ip()} port={context.process.get_exposed_port(5432)} dbname={context.process.dbname} user={context.process.username} password={context.process.password}"
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
        context.process.stop()
        print("Terminated Docker process")
