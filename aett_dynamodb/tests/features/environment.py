import subprocess
import time

from aett.dynamodb.persistence_management import PersistenceManagement


def before_scenario(context, _):
    context.process = subprocess.Popen(
        'docker run -p 8000:8000 amazon/dynamodb-local',
        shell=True,
        stdout=None,
        stderr=None)
    time.sleep(2)
    mgmt = PersistenceManagement(region='localhost')
    mgmt.initialize()


def after_scenario(context, _):
    if context.process:
        context.process.terminate()
        print("Terminated Docker process")
