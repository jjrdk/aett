import pymongo.database
from behave.api.async_step import async_run_until_complete
from testcontainers.mongodb import MongoDbContainer

from aett.eventstore import TopicMap
from aett.mongodbasync.async_persistence_management import AsyncPersistenceManagement
from aett_mongo_async.tests.features.steps import Types


@async_run_until_complete
async def before_scenario(context, _):
    context.process = MongoDbContainer()
    mongo_container = context.process.start()
    client = pymongo.AsyncMongoClient(mongo_container.get_connection_url())
    context.db = client.get_database('test')
    tm = TopicMap()
    tm.register_module(Types)
    context.topic_map = tm
    context.mgmt = AsyncPersistenceManagement(context.db, tm)
    await context.mgmt.initialize()


def after_scenario(context, _):
    context.db.close()
    print("Closed DB connection")
    context.process.stop()
    print("Terminated Docker process")
