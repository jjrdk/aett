from typing import Iterable

import pymongo
from pymongo.asynchronous import collection, cursor
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import CollectionInvalid

from aett.eventstore import Commit, IManagePersistenceAsync, TopicMap, COMMITS, SNAPSHOTS
from aett.mongodbasync.mapping import _doc_to_commit


class AsyncPersistenceManagement(IManagePersistenceAsync):
    def __init__(self,
                 db: AsyncDatabase,
                 topic_map: TopicMap,
                 commits_table_name: str = COMMITS,
                 snapshots_table_name: str = SNAPSHOTS):
        self._topic_map = topic_map
        self.db: AsyncDatabase = db
        self.commits_table_name = commits_table_name
        self.snapshots_table_name = snapshots_table_name

    async def initialize(self):
        try:
            counters_collection: AsyncCollection = await self.db.create_collection('counters',
                                                                                            check_exists=True)
            if await counters_collection.count_documents({'_id': 'CheckpointToken'}) == 0:
                await counters_collection.insert_one({'_id': 'CheckpointToken', 'seq': 0})
        except pymongo.errors.CollectionInvalid:
            pass
        try:
            commits_collection: collection.AsyncCollection = await self.db.create_collection(self.commits_table_name,
                                                                                             check_exists=True)
            await commits_collection.create_index(
                [("TenantId", pymongo.ASCENDING), ("CheckpointToken", pymongo.ASCENDING)],
                comment="GetFromCheckpoint", unique=True)
            await commits_collection.create_index([("TenantId", pymongo.ASCENDING), ("StreamId", pymongo.ASCENDING),
                                                   ("StreamRevision", pymongo.ASCENDING)], comment="GetFrom",
                                                  unique=True)
            await commits_collection.create_index([("TenantId", pymongo.ASCENDING), ("StreamId", pymongo.ASCENDING),
                                                   ("CommitSequence", pymongo.ASCENDING)], comment="LogicalKey",
                                                  unique=True)
            await commits_collection.create_index([("CommitStamp", pymongo.ASCENDING)], comment="CommitStamp",
                                                  unique=False)
            await commits_collection.create_index([("TenantId", pymongo.ASCENDING), ("StreamId", pymongo.ASCENDING),
                                                   ("CommitId", pymongo.ASCENDING)], comment="CommitId", unique=True)
        except CollectionInvalid:
            pass

        try:
            snapshots_collection: collection.AsyncCollection = await self.db.create_collection(
                self.snapshots_table_name,
                check_exists=True)
            await snapshots_collection.create_index([("TenantId", pymongo.ASCENDING), ("StreamId", pymongo.ASCENDING),
                                                     ("StreamRevision", pymongo.ASCENDING)], comment="LogicalKey",
                                                    unique=True)
        except pymongo.errors.CollectionInvalid:
            pass

    async def drop(self):
        await self.db.drop_collection(self.commits_table_name)
        await self.db.drop_collection(self.snapshots_table_name)

    async def purge(self, tenant_id: str):
        c = (self.db.get_collection(self.commits_table_name))
        await c.delete_many({'TenantId': tenant_id})

    async def get_from(self, checkpoint: int) -> Iterable[Commit]:
        c = (self.db.get_collection(self.commits_table_name))
        filters = {"CommitSequence": {'$gte': checkpoint}}
        query_response: cursor.AsyncCursor = c.find({'$and': [filters]})
        async for doc in query_response.sort('CheckpointToken', direction=pymongo.ASCENDING):
            yield _doc_to_commit(doc, self._topic_map)
