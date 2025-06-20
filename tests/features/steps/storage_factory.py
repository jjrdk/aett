from typing import Any

from pymongo import AsyncMongoClient, MongoClient

from aett.domain import ConflictDetector
from aett.eventstore import (
    ICommitEventsAsync,
    TopicMap,
    IAccessSnapshotsAsync,
    ICommitEvents,
    IAccessSnapshots,
)
from aett.storage.synchronous.dynamodb.commit_store import (
    CommitStore as DynamoDbCommitStore,
)
from aett.storage.synchronous.dynamodb.snapshot_store import (
    SnapshotStore as DynamoDbSnapshotStore,
)
from aett.storage.synchronous.inmemory.commit_store import (
    CommitStore as InMemoryCommitStore,
)
from aett.storage.synchronous.inmemory.snapshot_store import (
    SnapshotStore as InMemorySnapshotStore,
)
from aett.storage.asynchronous.mongodb import AsyncCommitStore as MongoAsyncCommitStore
from aett.storage.synchronous.mongodb.commit_store import (
    CommitStore as MongoCommitStore,
)
from aett.storage.asynchronous.mongodb import (
    AsyncSnapshotStore as MongoAsyncSnapshotStore,
)
from aett.storage.synchronous.mongodb.snapshot_store import (
    SnapshotStore as MongoSnapshotStore,
)
from aett.storage.asynchronous.postgresql.async_commit_store import (
    AsyncCommitStore as PostgresAsyncCommitStore,
)
from aett.storage.synchronous.postgresql.commit_store import (
    CommitStore as PostgresCommitStore,
)
from aett.storage.synchronous.mysql.commit_store import (
    CommitStore as MySqlCommitStore,
)
from aett.storage.asynchronous.postgresql.async_snapshot_store import (
    AsyncSnapshotStore as PostgresAsyncSnapshotStore,
)
from aett.storage.synchronous.postgresql.snapshot_store import (
    SnapshotStore as PostgresSnapshotStore,
)
from aett.storage.synchronous.mysql.snapshot_store import (
    SnapshotStore as MySqlSnapshotStore,
)
from aett.storage.asynchronous.sqlite.async_commit_store import (
    AsyncCommitStore as SqliteAsyncCommitStore,
)
from aett.storage.synchronous.sqlite.commit_store import (
    CommitStore as SqliteCommitStore,
)
from aett.storage.asynchronous.sqlite.async_snapshot_store import (
    AsyncSnapshotStore as SqliteAsyncSnapshotStore,
)
from aett.storage.synchronous.sqlite.snapshot_store import (
    SnapshotStore as SqliteSnapshotStore,
)
from aett.storage.synchronous.s3.commit_store import CommitStore as S3CommitStore
from aett.storage.synchronous.s3.snapshot_store import SnapshotStore as S3SnapshotStore


def create_async_commit_store(
        connection_string: str,
        storage_type: str,
        topic_map: TopicMap,
        conflict_detector: ConflictDetector = None,
) -> ICommitEventsAsync:
    match storage_type:
        case "mongo_async":
            client = AsyncMongoClient(connection_string)
            return MongoAsyncCommitStore(
                client.get_database("test"), topic_map, conflict_detector
            )
        case "postgres_async":
            return PostgresAsyncCommitStore(
                connection_string=connection_string,
                topic_map=topic_map,
                conflict_detector=conflict_detector,
            )
        case "sqlite_async":
            return SqliteAsyncCommitStore(
                connection_string=connection_string,
                topic_map=topic_map,
                conflict_detector=conflict_detector,
            )


def create_commit_store(
        connection_string: Any,
        storage_type: str,
        topic_map: TopicMap,
        conflict_detector: ConflictDetector = None,
        context: Any = None,
) -> ICommitEvents:
    commit_store: ICommitEvents | None = None
    match storage_type:
        case "dynamo":
            commit_store = DynamoDbCommitStore(
                topic_map=topic_map,
                conflict_detector=conflict_detector,
                region="localhost",
                aws_access_key_id="dummy",
                aws_secret_access_key="dummy",
                aws_session_token="dummy",
                port=int(connection_string),
            )
        case "inmemory":
            commit_store = InMemoryCommitStore(conflict_detector=conflict_detector)
        case "mongo":
            client = MongoClient(connection_string)
            commit_store = MongoCommitStore(
                client.get_database("test"), topic_map, conflict_detector
            )
        case "postgres":
            commit_store = PostgresCommitStore(
                connection_string=connection_string,
                topic_map=topic_map,
                conflict_detector=conflict_detector,
            )
        case "mysql":
            commit_store = MySqlCommitStore(
                host=context.host,
                user=context.user,
                password=context.password,
                database=context.database,
                port=context.port,
                topic_map=topic_map,
                conflict_detector=conflict_detector,
            )
        case "s3":
            commit_store = S3CommitStore(
                s3_config=connection_string,
                topic_map=topic_map,
                conflict_detector=conflict_detector,
            )
        case "sqlite":
            commit_store = SqliteCommitStore(
                connection_string=connection_string,
                topic_map=topic_map,
                conflict_detector=conflict_detector,
            )
    if not commit_store:
        raise ValueError(f"Unknown storage type: {storage_type}")
    return commit_store


def create_async_snapshot_store(
        connection_string: str, storage_type: str
) -> IAccessSnapshotsAsync:
    match storage_type:
        case "mongo_async":
            client = AsyncMongoClient(connection_string)
            return MongoAsyncSnapshotStore(client.get_database("test"))
        case "postgres_async":
            return PostgresAsyncSnapshotStore(connection_string=connection_string)
        case "sqlite_async":
            return SqliteAsyncSnapshotStore(connection_string=connection_string)


def create_snapshot_store(
        connection_string: Any, storage_type: str, context: Any = None
) -> IAccessSnapshots:
    snapshot_store: IAccessSnapshots | None = None
    match storage_type:
        case "dynamo":
            snapshot_store = DynamoDbSnapshotStore(
                region="localhost",
                port=int(connection_string),
                aws_access_key_id="dummy",
                aws_secret_access_key="dummy",
                aws_session_token="dummy",
            )
        case "inmemory":
            snapshot_store = InMemorySnapshotStore()
        case "mongo":
            client = MongoClient(connection_string)
            snapshot_store = MongoSnapshotStore(client.get_database("test"))
        case "postgres":
            snapshot_store = PostgresSnapshotStore(connection_string=connection_string)
        case "mysql":
            snapshot_store = MySqlSnapshotStore(
                host=context.host,
                user=context.user,
                password=context.password,
                database=context.database,
                port=context.port,
            )
        case "s3":
            snapshot_store = S3SnapshotStore(s3_config=connection_string)
        case "sqlite":
            snapshot_store = SqliteSnapshotStore(connection_string=connection_string)
    if not snapshot_store:
        raise ValueError(f"Unknown storage type: {storage_type}")
    return snapshot_store
