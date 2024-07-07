import datetime
import typing
from typing import Iterable
from uuid import UUID
import pymongo
from pymongo import database, results, errors
from pydantic_core import from_json, to_json
from aett.domain import ConflictingCommitException, NonConflictingCommitException, ConflictDetector
from aett.eventstore import ICommitEvents, IAccessSnapshots, Snapshot, Commit, MAX_INT, EventMessage, \
    TopicMap, COMMITS, SNAPSHOTS, IManagePersistence


def _doc_to_commit(doc: dict, topic_map: TopicMap) -> Commit:
    loads = from_json(doc['Events'])
    events_ = [EventMessage.from_json(e, topic_map) for e in loads]
    return Commit(tenant_id=doc['TenantId'],
                  stream_id=doc['StreamId'],
                  stream_revision=int(doc['StreamRevision']),
                  commit_id=UUID(doc['CommitId']),
                  commit_sequence=int(doc['CommitSequence']),
                  commit_stamp=datetime.datetime.fromtimestamp(int(doc['CommitStamp']), datetime.UTC),
                  headers=from_json(doc['Headers']),
                  events=events_,
                  checkpoint_token=doc['CheckpointToken'])


# noinspection DuplicatedCode
class CommitStore(ICommitEvents):
    def __init__(self, db: database.Database, topic_map: TopicMap, conflict_detector: ConflictDetector = None,
                 table_name=COMMITS):
        self._topic_map = topic_map
        self._collection: database.Collection = db.get_collection(table_name)
        self._counters_collection: database.Collection = db.get_collection('counters')
        self._conflict_detector = conflict_detector if conflict_detector is not None else ConflictDetector()

    def get(self, tenant_id: str, stream_id: str, min_revision: int = 0,
            max_revision: int = MAX_INT) -> typing.Iterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        filters = {"TenantId": tenant_id, "StreamId": stream_id}
        if min_revision > 0:
            filters['StreamRevision'] = {'$gte': min_revision}
        if max_revision < MAX_INT:
            if 'StreamRevision' in filters:
                filters['StreamRevision']['$lte'] = max_revision
            else:
                filters['StreamRevision'] = {'$lte': max_revision}

        query_response: pymongo.cursor.Cursor = self._collection.find({'$and': [filters]})
        for doc in query_response.sort('CheckpointToken', direction=pymongo.ASCENDING):
            yield _doc_to_commit(doc, self._topic_map)

    def get_to(self, tenant_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        filters = {"TenantId": tenant_id, "StreamId": stream_id, "CommitStamp": {'$lte': int(max_time.timestamp())}}

        query_response: pymongo.cursor.Cursor = self._collection.find({'$and': [filters]})
        for doc in query_response.sort('CheckpointToken', direction=pymongo.ASCENDING):
            yield _doc_to_commit(doc, self._topic_map)

    def get_all_to(self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        filters = {"TenantId": tenant_id, "CommitStamp": {'$lte': int(max_time.timestamp())}}

        query_response: pymongo.cursor.Cursor = self._collection.find({'$and': [filters]})
        for doc in query_response.sort('CheckpointToken', direction=pymongo.ASCENDING):
            yield _doc_to_commit(doc, self._topic_map)

    def commit(self, commit: Commit):
        try:
            ret = self._counters_collection.find_one_and_update(
                filter={'_id': 'CheckpointToken'},
                update={'$inc': {'seq': 1}}).get('seq')
            doc = {
                'TenantId': commit.tenant_id,
                'StreamId': commit.stream_id,
                'StreamRevision': commit.stream_revision,
                'CommitId': str(commit.commit_id),
                'CommitSequence': commit.commit_sequence,
                'CommitStamp': int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
                'Headers': to_json(commit.headers),
                'Events': to_json([e.to_json() for e in commit.events]),
                'CheckpointToken': int(ret)
            }
            _: pymongo.results.InsertOneResult = self._collection.insert_one(doc)
        except Exception as e:
            if isinstance(e, pymongo.errors.DuplicateKeyError):
                if self._detect_duplicate(commit.commit_id, commit.tenant_id, commit.stream_id,
                                          commit.commit_sequence):
                    raise Exception(
                        f"Commit {commit.commit_id} already exists in stream {commit.stream_id}")
                else:
                    conflicts, revision = self._detect_conflicts(commit=commit)
                    if conflicts:
                        raise ConflictingCommitException(
                            f"Conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}")
                    else:
                        raise NonConflictingCommitException(
                            f'Non-conflicting version conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}')
            else:
                raise Exception(
                    f"Failed to commit event to stream {commit.stream_id} with status code {e.response['ResponseMetadata']['HTTPStatusCode']}")

    def _detect_duplicate(self, commit_id: UUID, tenant_id: str, stream_id: str, commit_sequence: int) -> bool:
        duplicate_check = self._collection.find_one(
            {'TenantId': tenant_id, 'StreamId': stream_id, 'CommitSequence': commit_sequence})
        s = str(duplicate_check.get('CommitId'))
        return s == str(commit_id)

    def _detect_conflicts(self, commit: Commit) -> (bool, int):
        filters = {"TenantId": commit.tenant_id, "StreamId": commit.stream_id,
                   "CommitSequence": {'$lte': commit.commit_sequence}}
        query_response: pymongo.cursor.Cursor = \
            self._collection.find({'$and': [filters]}).sort('CheckpointToken',
                                                            direction=pymongo.ASCENDING)

        latest_revision = 0
        for doc in query_response:
            c = _doc_to_commit(doc, self._topic_map)
            if self._conflict_detector.conflicts_with(list(map(self._get_body, commit.events)),
                                                      list(map(self._get_body, c.events))):
                return True, -1
            i = int(doc['StreamRevision'])
            if i > latest_revision:
                latest_revision = i
        return False, latest_revision

    @staticmethod
    def _get_body(e):
        return e.body


class SnapshotStore(IAccessSnapshots):
    def __init__(self, db: database.Database, table_name: str = SNAPSHOTS):
        self.collection: database.Collection = db.get_collection(table_name)

    def get(self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        try:
            filters = {'TenantId': tenant_id, 'StreamId': stream_id, 'StreamRevision': {'$lte': max_revision}}
            cursor = self.collection.find({'$and': [filters]}).sort('StreamRevision',
                                                                    direction=pymongo.DESCENDING).limit(1)
            item = next(cursor, None)
            if item is None:
                return None

            return Snapshot(tenant_id=item['TenantId'],
                            stream_id=item['StreamId'],
                            stream_revision=int(item['StreamRevision']),
                            commit_sequence=int(item['CommitSequence']),
                            payload=from_json(item['Payload']),
                            headers=from_json(item['Headers']))
        except Exception as e:
            raise Exception(
                f"Failed to get snapshot for stream {stream_id} with status code {e.response['ResponseMetadata']['HTTPStatusCode']}")

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] = None):
        if headers is None:
            headers = {}
        try:
            doc = {
                'TenantId': snapshot.tenant_id,
                'StreamId': snapshot.stream_id,
                'StreamRevision': snapshot.stream_revision,
                'CommitSequence': snapshot.commit_sequence,
                'Payload': to_json(snapshot.payload),
                'Headers': to_json(headers)
            }
            _ = self.collection.insert_one(doc)
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with status code {e.response['ResponseMetadata']['HTTPStatusCode']}")


class PersistenceManagement(IManagePersistence):
    def __init__(self,
                 db: database.Database,
                 topic_map: TopicMap,
                 commits_table_name: str = COMMITS,
                 snapshots_table_name: str = SNAPSHOTS):
        self._topic_map = topic_map
        self.db: database.Database = db
        self.commits_table_name = commits_table_name
        self.snapshots_table_name = snapshots_table_name

    def initialize(self):
        try:
            counters_collection: database.Collection = self.db.create_collection('counters', check_exists=True)
            if counters_collection.count_documents({'_id': 'CheckpointToken'}) == 0:
                counters_collection.insert_one({'_id': 'CheckpointToken', 'seq': 0})
        except pymongo.errors.CollectionInvalid:
            pass
        try:
            commits_collection: database.Collection = self.db.create_collection(self.commits_table_name,
                                                                                check_exists=True)
            commits_collection.create_index([("TenantId", pymongo.ASCENDING), ("CheckpointToken", pymongo.ASCENDING)],
                                            comment="GetFromCheckpoint", unique=True)
            commits_collection.create_index([("TenantId", pymongo.ASCENDING), ("StreamId", pymongo.ASCENDING),
                                             ("StreamRevision", pymongo.ASCENDING)], comment="GetFrom", unique=True)
            commits_collection.create_index([("TenantId", pymongo.ASCENDING), ("StreamId", pymongo.ASCENDING),
                                             ("CommitSequence", pymongo.ASCENDING)], comment="LogicalKey", unique=True)
            commits_collection.create_index([("CommitStamp", pymongo.ASCENDING)], comment="CommitStamp", unique=False)
            commits_collection.create_index([("TenantId", pymongo.ASCENDING), ("StreamId", pymongo.ASCENDING),
                                             ("CommitId", pymongo.ASCENDING)], comment="CommitId", unique=True)
        except pymongo.errors.CollectionInvalid:
            pass

        try:
            snapshots_collection: database.Collection = self.db.create_collection(self.snapshots_table_name,
                                                                                  check_exists=True)
            snapshots_collection.create_index([("TenantId", pymongo.ASCENDING), ("StreamId", pymongo.ASCENDING),
                                               ("StreamRevision", pymongo.ASCENDING)], comment="LogicalKey",
                                              unique=True)
        except pymongo.errors.CollectionInvalid:
            pass

    def drop(self):
        self.db.drop_collection(self.commits_table_name)
        self.db.drop_collection(self.snapshots_table_name)

    def purge(self, tenant_id: str):
        collection = (self.db.get_collection(self.commits_table_name))
        collection.delete_many({'TenantId': tenant_id})

    def get_from(self, checkpoint: int) -> Iterable[Commit]:
        collection = (self.db.get_collection(self.commits_table_name))
        filters = {"CommitSequence": {'$gte': checkpoint}}
        query_response: pymongo.cursor.Cursor = collection.find({'$and': [filters]})
        for doc in query_response.sort('CheckpointToken', direction=pymongo.ASCENDING):
            yield _doc_to_commit(doc, self._topic_map)
