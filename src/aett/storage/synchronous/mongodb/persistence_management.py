from typing import Iterable

import pymongo
from pymongo import database

from aett.eventstore import IManagePersistence, TopicMap, COMMITS, SNAPSHOTS, Commit
from aett.storage.synchronous.mongodb import _doc_to_commit


class PersistenceManagement(IManagePersistence):
    def __init__(
        self,
        db: database.Database,
        topic_map: TopicMap,
        commits_table_name: str = COMMITS,
        snapshots_table_name: str = SNAPSHOTS,
    ):
        self._topic_map = topic_map
        self.db: database.Database = db
        self.commits_table_name = commits_table_name
        self.snapshots_table_name = snapshots_table_name

    def initialize(self):
        try:
            counters_collection: database.Collection = self.db.create_collection(
                "counters", check_exists=True
            )
            if counters_collection.count_documents({"_id": "CheckpointToken"}) == 0:
                counters_collection.insert_one({"_id": "CheckpointToken", "seq": 0})
        except pymongo.errors.CollectionInvalid:
            pass
        try:
            commits_collection: database.Collection = self.db.create_collection(
                self.commits_table_name, check_exists=True
            )
            commits_collection.create_index(
                [
                    ("TenantId", pymongo.ASCENDING),
                    ("CheckpointToken", pymongo.ASCENDING),
                ],
                comment="GetFromCheckpoint",
                unique=True,
            )
            commits_collection.create_index(
                [
                    ("TenantId", pymongo.ASCENDING),
                    ("StreamId", pymongo.ASCENDING),
                    ("StreamRevision", pymongo.ASCENDING),
                ],
                comment="GetFrom",
                unique=True,
            )
            commits_collection.create_index(
                [
                    ("TenantId", pymongo.ASCENDING),
                    ("StreamId", pymongo.ASCENDING),
                    ("CommitSequence", pymongo.ASCENDING),
                ],
                comment="LogicalKey",
                unique=True,
            )
            commits_collection.create_index(
                [("CommitStamp", pymongo.ASCENDING)],
                comment="CommitStamp",
                unique=False,
            )
            commits_collection.create_index(
                [
                    ("TenantId", pymongo.ASCENDING),
                    ("StreamId", pymongo.ASCENDING),
                    ("CommitId", pymongo.ASCENDING),
                ],
                comment="CommitId",
                unique=True,
            )
        except pymongo.errors.CollectionInvalid:
            pass

        try:
            snapshots_collection: database.Collection = self.db.create_collection(
                self.snapshots_table_name, check_exists=True
            )
            snapshots_collection.create_index(
                [
                    ("TenantId", pymongo.ASCENDING),
                    ("StreamId", pymongo.ASCENDING),
                    ("StreamRevision", pymongo.ASCENDING),
                ],
                comment="LogicalKey",
                unique=True,
            )
        except pymongo.errors.CollectionInvalid:
            pass

    def drop(self):
        self.db.drop_collection(self.commits_table_name)
        self.db.drop_collection(self.snapshots_table_name)

    def purge(self, tenant_id: str):
        collection = self.db.get_collection(self.commits_table_name)
        collection.delete_many({"TenantId": tenant_id})

    def get_from(self, checkpoint: int) -> Iterable[Commit]:
        collection = self.db.get_collection(self.commits_table_name)
        filters = {"CommitSequence": {"$gte": checkpoint}}
        query_response: pymongo.cursor.Cursor = collection.find({"$and": [filters]})
        for doc in query_response.sort("CheckpointToken", direction=pymongo.ASCENDING):
            yield _doc_to_commit(doc, self._topic_map)
