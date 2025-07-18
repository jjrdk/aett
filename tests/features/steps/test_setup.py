from behave import *
from behave.api.async_step import run_until_complete
from pymongo import AsyncMongoClient, MongoClient
from testcontainers.core.container import DockerContainer
from testcontainers.minio import MinioContainer
from testcontainers.mongodb import MongoDbContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

import test_types
from aett.eventstore import TopicMap
from aett.storage.asynchronous.dynamodb.async_persistence_management import (
    AsyncPersistenceManagement as AsyncDynamoDbPersistenceManagement,
)
from aett.storage.asynchronous.mongodb import (
    AsyncPersistenceManagement as MongoAsyncPersistenceManagement,
)
from aett.storage.asynchronous.mysql.async_persistence_management import (
    AsyncPersistenceManagement as MySqlAsyncPersistenceManagement,
)
from aett.storage.asynchronous.postgresql.async_persistence_management import (
    AsyncPersistenceManagement as PostgresAsyncPersistenceManagement,
)
from aett.storage.asynchronous.sqlite.async_persistence_management import (
    AsyncPersistenceManagement as SqliteAsyncPersistenceManagement,
)
from aett.storage.synchronous.dynamodb.persistence_management import (
    PersistenceManagement as DynamoDbPersistenceManagement,
)
from aett.storage.synchronous.mongodb.persistence_management import (
    PersistenceManagement as MongoPersistenceManagement,
)
from aett.storage.synchronous.mysql.persistence_management import (
    PersistenceManagement as MySqlPersistenceManagement,
)
from aett.storage.synchronous.postgresql.persistence_management import (
    PersistenceManagement as PostgresPersistenceManagement,
)
from aett.storage.synchronous.s3 import S3Config
from aett.storage.synchronous.s3.persistence_management import (
    PersistenceManagement as S3PersistenceManagement,
)
from aett.storage.asynchronous.s3 import AsyncS3Config
from aett.storage.asynchronous.s3.async_persistence_management import (
    AsyncPersistenceManagement as AsyncS3PersistenceManagement,
)
from aett.storage.synchronous.sqlite.persistence_management import (
    PersistenceManagement as SqlitePersistenceManagement,
)

use_step_matcher("re")


@given("a running (?P<storage>.+) server")
@run_until_complete
async def step_impl(context, storage: str):
    context.storage_type = storage
    tm = TopicMap()
    tm.register_module(test_types)
    context.topic_map = tm
    match storage:
        case "dynamo":
            context.process = DockerContainer(
                "amazon/dynamodb-local"
            ).with_exposed_ports(8000)
            context.process.start()
            port = int(context.process.get_exposed_port(8000))
            context.db = port
            mgmt = DynamoDbPersistenceManagement(
                region="localhost",
                port=port,
                aws_session_token="dummy",
                aws_access_key_id="dummy",
                aws_secret_access_key="dummy",
            )
            mgmt.initialize()
            context.mgmt = mgmt
        case "dynamodb_async":
            context.process = DockerContainer(
                "amazon/dynamodb-local"
            ).with_exposed_ports(8000)
            context.process.start()
            port = int(context.process.get_exposed_port(8000))
            context.db = port
            mgmt = AsyncDynamoDbPersistenceManagement(
                aws_access_key_id="dummy",
                aws_secret_access_key="dummy",
                aws_session_token="dummy",
                region="localhost",
                port=port)
            await mgmt.initialize()
        case "inmemory":
            context.db = "inmemory"
            pass
        case "mongo":
            context.process = MongoDbContainer()
            mongo_container = context.process.start()
            context.db = mongo_container.get_connection_url()
            client = MongoClient(context.db)
            mgmt = MongoPersistenceManagement(client.get_database("test"), tm)
            mgmt.initialize()
            context.mgmt = mgmt
        case "mongo_async":
            context.process = MongoDbContainer()
            mongo_container = context.process.start()
            context.db = mongo_container.get_connection_url()
            client = AsyncMongoClient(context.db)
            mgmt = MongoAsyncPersistenceManagement(client.get_database("test"), tm)
            await mgmt.initialize()
            context.mgmt = mgmt
        case "postgres":
            context.process = PostgresContainer()
            pg_container = context.process.start()
            context.db = pg_container.get_connection_url().replace("+psycopg2", "")
            mgmt = PostgresPersistenceManagement(
                connection_string=context.db, topic_map=tm
            )
            mgmt.initialize()
            context.mgmt = mgmt
        case "mysql":
            context.process = MySqlContainer()
            mysql_container = context.process.start()
            context.db = mysql_container.get_connection_url()
            context.host = mysql_container.get_container_host_ip()
            context.port = int(mysql_container.get_exposed_port(3306))
            context.user = mysql_container.username
            context.password = mysql_container.password
            context.database = mysql_container.dbname
            mgmt = MySqlPersistenceManagement(
                host=context.host,
                user=context.user,
                password=context.password,
                port=context.port,
                database=context.database,
                topic_map=tm,
            )
            mgmt.initialize()
            context.mgmt = mgmt
        case "postgres_async":
            context.process = PostgresContainer()
            pg_container = context.process.start()
            context.db = pg_container.get_connection_url().replace("+psycopg2", "")
            mgmt = PostgresAsyncPersistenceManagement(
                connection_string=context.db, topic_map=tm
            )
            await mgmt.initialize()
            context.mgmt = mgmt
        case "mysql_async":
            context.process = MySqlContainer()
            mysql_container = context.process.start()
            context.db = mysql_container.get_connection_url()
            context.host = mysql_container.get_container_host_ip()
            context.port = int(mysql_container.get_exposed_port(3306))
            context.user = mysql_container.username
            context.password = mysql_container.password
            context.database = mysql_container.dbname
            mgmt = MySqlAsyncPersistenceManagement(
                host=context.host,
                user=context.user,
                password=context.password,
                port=context.port,
                database=context.database,
                topic_map=tm,
            )
            await mgmt.initialize()
            context.mgmt = mgmt
        case "s3":
            context.process = MinioContainer(image="quay.io/minio/minio")
            context.process.start()
            minio_container = context.process
            exposed_port = minio_container.get_exposed_port(9000)
            context.db = S3Config(
                bucket="test",
                endpoint_url=f"http://localhost:{exposed_port}",
                use_tls=False,
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
            )
            mgmt = S3PersistenceManagement(s3_config=context.db)
            mgmt.initialize()
        case "s3_async":
            context.process = MinioContainer(image="quay.io/minio/minio")
            context.process.start()
            minio_container = context.process
            exposed_port = minio_container.get_exposed_port(9000)
            context.db = AsyncS3Config(
                bucket="test",
                endpoint_url=f"http://localhost:{exposed_port}",
                use_tls=False,
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
            )
            mgmt = AsyncS3PersistenceManagement(s3_config=context.db)
            await mgmt.initialize()
        case "sqlite":
            context.db = f"{context.tenant_id}.db"
            mgmt = SqlitePersistenceManagement(context.db, tm)
            mgmt.initialize()
            context.mgmt = mgmt
        case "sqlite_async":
            context.db = f"{context.tenant_id}.db"
            mgmt = SqliteAsyncPersistenceManagement(context.db, tm)
            await mgmt.initialize()
            context.mgmt = mgmt
