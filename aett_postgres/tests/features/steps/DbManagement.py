import subprocess
import time
import psycopg
from behave import *

from aett.postgres import PersistenceManagement

use_step_matcher("re")


@given("I have a clean database")
def i_have_a_clean_db(context):
    with open("/tmp/output.log", "a") as output:
        context.process = subprocess.Popen(
            "docker run -p 5432:5432 -e POSTGRES_PASSWORD=aett -e POSTGRES_USER=aett -e POSTGRES_DB=aett postgres:15.4-alpine",
            shell=True,
            stdout=output,
            stderr=output)
        time.sleep(1)


@step("I run the setup script")
def i_run_the_setup_script(context):
    context.db = psycopg.connect("host=localhost port=5432 dbname=aett user=aett password=aett")
    context.mgmt = PersistenceManagement(context.db)
    context.mgmt.initialize()


@then("the database should be in a known state")
def database_is_in_known_state(context):
    cur = context.db.cursor()
    cur.execute("SELECT * FROM pg_catalog.pg_tables WHERE pg_tables.schemaname = 'public';")
    tables = cur.fetchall()
    assert len(tables) == 2


@when("I run the teardown script")
def i_run_the_teardown_script(context):
    context.mgmt.drop()


@then("the database should be gone")
def the_database_is_gone(context):
    cur = context.db.cursor()
    cur.execute("SELECT * FROM pg_catalog.pg_tables WHERE pg_tables.schemaname = 'public';")
    tables = cur.fetchall()
    assert len(tables) == 0


@step("I can disconnect from the database")
def i_can_disconnect_from_the_database(context):
    context.db.close()
    context.process.terminate()