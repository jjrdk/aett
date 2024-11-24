import pymongo.database
from testcontainers.mongodb import MongoDbContainer

from aett.eventstore import TopicMap
from aett.mongodb.persistence_management import PersistenceManagement
from aett_mongo.tests.features.steps import Types


def before_scenario(context, _):
    context.process = MongoDbContainer()
    mongo_container = context.process.start()
    client = pymongo.MongoClient(mongo_container.get_connection_url())
    context.db = client.get_database('test')
    tm = TopicMap()
    tm.register_module(Types)
    context.topic_map = tm
    context.mgmt = PersistenceManagement(context.db, tm)
    context.mgmt.initialize()


def after_scenario(context, _):
    context.db.close()
    print("Closed DB connection")
    context.process.stop()
    print("Terminated Docker process")
