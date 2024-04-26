import subprocess
import time

from aett.s3 import S3Config, PersistenceManagement


def before_scenario(context, _):
    context.process = subprocess.Popen(
        'docker run -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"',
        shell=True,
        stdout=None,
        stderr=None)
    time.sleep(2)
    context.s3_config = S3Config(bucket='test',  # str(datetime.datetime.timestamp(datetime.datetime.now())),
                                 endpoint_url='http://localhost:9000',
                                 use_tls=False,
                                 aws_access_key_id='minioadmin',
                                 aws_secret_access_key='minioadmin',
                                 profile_name='')
    mgmt = PersistenceManagement(context.s3_config)
    mgmt.initialize()


def after_scenario(context, _):
    if context.process:
        context.process.terminate()
        print("Terminated Docker process")
