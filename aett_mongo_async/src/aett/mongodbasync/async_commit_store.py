import typing
from typing import Iterable
import datetime
from uuid import UUID

from aett.domain import ConflictDetector, ConflictingCommitException, NonConflictingCommitException
from aett.eventstore import Commit, TopicMap, ICommitEventsAsync, COMMITS, MAX_INT
import pymongo
from pydantic_core import to_json
from pymongo.asynchronous import database, collection, cursor
from pymongo.asynchronous.cursor import AsyncCursor

from aett.mongodbasync.mapping import _doc_to_commit


class AsyncCommitStore(ICommitEventsAsync):
    def __init__(self, db: database.AsyncDatabase, topic_map: TopicMap, conflict_detector: ConflictDetector = None,
                 table_name=COMMITS):
        self._topic_map = topic_map
        self._collection: collection.AsyncCollection = db.get_collection(table_name)
        self._counters_collection: collection.AsyncCollection = db.get_collection('counters')
        self._conflict_detector = conflict_detector if conflict_detector is not None else ConflictDetector()

    async def get(self, tenant_id: str, stream_id: str, min_revision: int = 0,
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

        query_response: AsyncCursor = self._collection.find({'$and': [filters]})
        async for doc in query_response.sort('CheckpointToken', direction=pymongo.ASCENDING):
            yield _doc_to_commit(doc, self._topic_map)

    async def get_to(self, tenant_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        filters = {"TenantId": tenant_id, "StreamId": stream_id, "CommitStamp": {'$lte': int(max_time.timestamp())}}

        query_response: cursor.AsyncCursor = self._collection.find({'$and': [filters]})
        async for doc in query_response.sort('CheckpointToken', direction=pymongo.ASCENDING):
            yield _doc_to_commit(doc, self._topic_map)

    async def get_all_to(self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        filters = {"TenantId": tenant_id, "CommitStamp": {'$lte': int(max_time.timestamp())}}

        query_response: cursor.AsyncCursor = self._collection.find({'$and': [filters]})
        async for doc in query_response.sort('CheckpointToken', direction=pymongo.ASCENDING):
            yield _doc_to_commit(doc, self._topic_map)

    async def commit(self, commit: Commit) -> None:
        try:
            seq_ = await self._counters_collection.find_one_and_update(filter={'_id': 'CheckpointToken'},
                                                                       update={'$inc': {'seq': 1}})
            ret = seq_.get('seq')
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
            _ = await self._collection.insert_one(doc)
        except Exception as e:
            if isinstance(e, pymongo.errors.DuplicateKeyError):
                if await self._detect_duplicate(commit.commit_id, commit.tenant_id, commit.stream_id,
                                                commit.commit_sequence):
                    raise Exception(
                        f"Commit {commit.commit_id} already exists in stream {commit.stream_id}")
                else:
                    conflicts, revision = await self._detect_conflicts(commit=commit)
                    if conflicts:
                        raise ConflictingCommitException(
                            f"Conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}")
                    else:
                        raise NonConflictingCommitException(
                            f'Non-conflicting version conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}')
            else:
                raise Exception(
                    f"Failed to commit event to stream {commit.stream_id} with status code {e.response['ResponseMetadata']['HTTPStatusCode']}")

    async def _detect_duplicate(self, commit_id: UUID, tenant_id: str, stream_id: str, commit_sequence: int) -> bool:
        duplicate_check = await self._collection.find_one(
            {'TenantId': tenant_id, 'StreamId': stream_id, 'CommitSequence': commit_sequence})
        s = str(duplicate_check.get('CommitId'))
        return s == str(commit_id)

    async def _detect_conflicts(self, commit: Commit) -> (bool, int):
        filters = {"TenantId": commit.tenant_id, "StreamId": commit.stream_id,
                   "CommitSequence": {'$lte': commit.commit_sequence}}
        query_response: cursor.AsyncCursor = \
            self._collection.find({'$and': [filters]}).sort('CheckpointToken',
                                                            direction=pymongo.ASCENDING)

        latest_revision = 0
        async for doc in query_response:
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
