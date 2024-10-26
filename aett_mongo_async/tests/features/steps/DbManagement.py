import pymongo.database
from behave import *
import subprocess

from behave.api.async_step import async_run_until_complete
from pymongo.asynchronous.database import AsyncDatabase

from aett.mongodbasync.async_persistence_management import AsyncPersistenceManagement

use_step_matcher("re")


@given("I have a clean database")
def i_have_a_clean_db(context):
    with open("/tmp/output.log", "a") as output:
        context.process = subprocess.Popen("docker run -p 27017:27017 mongo:latest", shell=True, stdout=output,
                                           stderr=output)


@when("I run the setup script")
@async_run_until_complete
async def i_run_the_setup_script(context):
    context.db = AsyncDatabase(pymongo.AsyncMongoClient('mongodb://localhost:27017'), 'test')
    context.mgmt = AsyncPersistenceManagement(context.db, context.topic_map)
    await context.mgmt.initialize()


@then("the database should be in a known state")
@async_run_until_complete
async def database_is_in_known_state(context):
    db: AsyncDatabase = context.db
    collections = await db.list_collection_names()
    assert 'counters' in collections
    assert 'commits' in collections
    assert 'snapshots' in collections


@when("I run the teardown script")
@async_run_until_complete
async def i_run_the_teardown_script(context):
    await context.mgmt.drop()


@then("the database should be gone")
def the_database_is_gone(context):
    pass


@step("I can disconnect from the database")
def i_can_disconnect_from_the_database(context):
    context.process.terminate()
