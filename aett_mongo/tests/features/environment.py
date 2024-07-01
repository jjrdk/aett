import subprocess
import time

import pymongo.database

from aett.eventstore import TopicMap
from aett.mongodb import PersistenceManagement
from features.steps import Types


def before_scenario(context, _):
    context.process = subprocess.Popen("docker run -p 27017:27017 mongo:latest", shell=True, stdout=None,
                                       stderr=None)
    time.sleep(1)
    context.db = pymongo.database.Database(pymongo.MongoClient('mongodb://localhost:27017'), 'test')
    tm = TopicMap()
    tm.register_module(Types)
    context.mgmt = PersistenceManagement(context.db, tm)
    context.mgmt.initialize()


def after_scenario(context, _):
    if context.db:
        context.db.close()
        print("Closed DB connection")
    if context.process:
        context.process.terminate()
        print("Terminated Docker process")
