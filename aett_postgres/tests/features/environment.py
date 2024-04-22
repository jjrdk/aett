import subprocess
import time

import psycopg

from aett.postgres import PersistenceManagement


def before_scenario(context, _):
    context.process = subprocess.Popen(
        "docker run -p 5432:5432 -e POSTGRES_PASSWORD=aett -e POSTGRES_USER=aett -e POSTGRES_DB=aett postgres:15.4-alpine",
        shell=True,
        stdout=None,
        stderr=None)
    time.sleep(1)
    context.db = psycopg.connect("host=localhost port=5432 dbname=aett user=aett password=aett", autocommit=True)
    mgmt = PersistenceManagement(context.db)
    mgmt.initialize()
    cur = context.db.cursor()
    cur.execute("SELECT * FROM pg_catalog.pg_tables WHERE pg_tables.schemaname = 'public';")
    tables = cur.fetchall()
    assert len(tables) == 2


def after_scenario(context, _):
    if context.db:
        context.db.close()
        print("Closed DB connection")
    if context.process:
        context.process.terminate()
        print("Terminated Docker process")
