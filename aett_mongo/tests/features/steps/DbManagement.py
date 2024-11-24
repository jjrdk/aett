import pymongo.database
from behave import *
import subprocess

from aett.mongodb.persistence_management import PersistenceManagement

use_step_matcher("re")


@given("I have a clean database")
def i_have_a_clean_db(context):
    with open("/tmp/output.log", "a") as output:
        context.process = subprocess.Popen("docker run -p 27017:27017 mongo:latest", shell=True, stdout=output,
                                           stderr=output)


@when("I run the setup script")
def i_run_the_setup_script(context):
    context.db = pymongo.database.Database(pymongo.MongoClient('mongodb://localhost:27017'), 'test')
    context.mgmt = PersistenceManagement(context.db, context.topic_map)
    context.mgmt.initialize()


@then("the database should be in a known state")
def database_is_in_known_state(context):
    db: pymongo.database.Database = context.db
    collections = db.list_collection_names()
    assert 'counters' in collections
    assert 'commits' in collections
    assert 'snapshots' in collections


@when("I run the teardown script")
def i_run_the_teardown_script(context):
    context.mgmt.drop()


@then("the database should be gone")
def the_database_is_gone(context):
    pass


@step("I can disconnect from the database")
def i_can_disconnect_from_the_database(context):
    context.process.terminate()
