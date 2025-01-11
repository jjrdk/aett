from typing import Any

from pymongo import AsyncMongoClient, MongoClient

from aett.domain import ConflictDetector
from aett.eventstore import ICommitEventsAsync, TopicMap, IAccessSnapshotsAsync, ICommitEvents, IAccessSnapshots
from aett.storage.synchronous.dynamodb.commit_store import CommitStore as DynamoDbCommitStore
from aett.storage.synchronous.dynamodb.snapshot_store import SnapshotStore as DynamoDbSnapshotStore
from aett.storage.synchronous.inmemory.commit_store import CommitStore as InMemoryCommitStore
from aett.storage.synchronous.inmemory.snapshot_store import SnapshotStore as InMemorySnapshotStore
from aett.storage.asynchronous.mongodb import AsyncCommitStore as MongoAsyncCommitStore
from aett.storage.synchronous.mongodb.commit_store import CommitStore as MongoCommitStore
from aett.storage.asynchronous.mongodb import AsyncSnapshotStore as MongoAsyncSnapshotStore
from aett.storage.synchronous.mongodb.snapshot_store import SnapshotStore as MongoSnapshotStore
from aett.storage.asynchronous.postgresql.async_commit_store import AsyncCommitStore as PostgresAsyncCommitStore
from aett.storage.synchronous.postgresql.commit_store import CommitStore as PostgresCommitStore
from aett.storage.asynchronous.postgresql.async_snapshot_store import AsyncSnapshotStore as PostgresAsyncSnapshotStore
from aett.storage.synchronous.postgresql.snapshot_store import SnapshotStore as PostgresSnapshotStore
from aett.storage.asynchronous.sqlite.async_commit_store import AsyncCommitStore as SqliteAsyncCommitStore
from aett.storage.synchronous.sqlite.commit_store import CommitStore as SqliteCommitStore
from aett.storage.asynchronous.sqlite.async_snapshot_store import AsyncSnapshotStore as SqliteAsyncSnapshotStore
from aett.storage.synchronous.sqlite.snapshot_store import SnapshotStore as SqliteSnapshotStore
from aett.storage.synchronous.s3.commit_store import CommitStore as S3CommitStore
from aett.storage.synchronous.s3.snapshot_store import SnapshotStore as S3SnapshotStore


def create_async_commit_store(connection_string: str, storage_type: str, topic_map: TopicMap,
                              conflict_detector: ConflictDetector = None) -> ICommitEventsAsync:
    match storage_type:
        case 'mongo_async':
            client = AsyncMongoClient(connection_string)
            return MongoAsyncCommitStore(client.get_database('test'), topic_map, conflict_detector)
        case 'postgres_async':
            return PostgresAsyncCommitStore(connection_string=connection_string, topic_map=topic_map,
                                            conflict_detector=conflict_detector)
        case 'sqlite_async':
            return SqliteAsyncCommitStore(connection_string=connection_string, topic_map=topic_map,
                                          conflict_detector=conflict_detector)


def create_commit_store(connection_string: Any, storage_type: str, topic_map: TopicMap,
                        conflict_detector: ConflictDetector = None) -> ICommitEvents:
    match storage_type:
        case 'dynamo':
            return DynamoDbCommitStore(topic_map=topic_map,
                                       conflict_detector=conflict_detector,
                                       region='localhost',
                                       aws_access_key_id='dummy',
                                       aws_secret_access_key='dummy',
                                       aws_session_token='dummy',
                                       port=int(connection_string))
        case 'inmemory':
            return InMemoryCommitStore(conflict_detector=conflict_detector)
        case 'mongo':
            client = MongoClient(connection_string)
            return MongoCommitStore(client.get_database('test'), topic_map, conflict_detector)
        case 'postgres':
            return PostgresCommitStore(connection_string=connection_string, topic_map=topic_map,
                                       conflict_detector=conflict_detector)
        case 's3':
            return S3CommitStore(s3_config=connection_string, topic_map=topic_map, conflict_detector=conflict_detector)
        case 'sqlite':
            return SqliteCommitStore(connection_string=connection_string, topic_map=topic_map,
                                     conflict_detector=conflict_detector)


def create_async_snapshot_store(connection_string: str, storage_type: str) -> IAccessSnapshotsAsync:
    match storage_type:
        case 'mongo_async':
            client = AsyncMongoClient(connection_string)
            return MongoAsyncSnapshotStore(client.get_database('test'))
        case 'postgres_async':
            return PostgresAsyncSnapshotStore(connection_string=connection_string)
        case 'sqlite_async':
            return SqliteAsyncSnapshotStore(connection_string=connection_string)


def create_snapshot_store(connection_string: Any, storage_type: str) -> IAccessSnapshots:
    match storage_type:
        case 'dynamo':
            return DynamoDbSnapshotStore(region='localhost',
                                         port=int(connection_string),
                                         aws_access_key_id='dummy',
                                         aws_secret_access_key='dummy',
                                         aws_session_token='dummy')
        case 'inmemory':
            return InMemorySnapshotStore()
        case 'mongo':
            client = MongoClient(connection_string)
            return MongoSnapshotStore(client.get_database('test'))
        case 'postgres':
            return PostgresSnapshotStore(connection_string=connection_string)
        case 's3':
            return S3SnapshotStore(s3_config=connection_string)
        case 'sqlite':
            return SqliteSnapshotStore(connection_string=connection_string)
