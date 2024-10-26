import inspect
import uuid

import asyncpg
from behave.api.async_step import async_run_until_complete
from testcontainers.postgres import PostgresContainer

from aett.eventstore import TopicMap
from aett.postgresasync import AsyncPersistenceManagement
from aett_postgres.tests.features.steps.Types import TestEvent


@async_run_until_complete
async def before_scenario(context, _):
    context.tenant_id = str(uuid.uuid4())
    context.stream_id = str(uuid.uuid4())
    context.process = PostgresContainer(image="postgres:alpine", username="aett", password="aett", dbname="aett")
    context.process.start()
    context.db = context.process.get_connection_url().replace('+psycopg2', '')
        # f"host={context.process.get_container_host_ip()} port={context.process.get_exposed_port(5432)} dbname={context.process.dbname} user={context.process.username} password={context.process.password}"
    tm = TopicMap()
    tm.register_module(inspect.getmodule(TestEvent))
    context.topic_map = tm
    mgmt = AsyncPersistenceManagement(context.db, tm)
    await mgmt.initialize()
    context.mgmt = mgmt
    conn = await asyncpg.connect(context.db)
    tables = await conn.fetch("SELECT * FROM pg_catalog.pg_tables WHERE pg_tables.schemaname = 'public';")

    assert len(tables) == 2


def after_scenario(context, _):
    if context.process:
        context.process.stop()
        print("Terminated Docker process")
