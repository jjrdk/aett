import typing

import pymongo
from pydantic_core import from_json, to_json
from pymongo.asynchronous import database, collection

from aett.eventstore import IAccessSnapshotsAsync, SNAPSHOTS, MAX_INT, Snapshot


class AsyncSnapshotStore(IAccessSnapshotsAsync):
    def __init__(self, db: database.AsyncDatabase, table_name: str = SNAPSHOTS):
        self.collection: collection.AsyncCollection = db.get_collection(table_name)

    async def get(self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        try:
            filters = {'TenantId': tenant_id, 'StreamId': stream_id, 'StreamRevision': {'$lte': max_revision}}
            cursor = self.collection.find({'$and': [filters]}).sort('StreamRevision',
                                                                    direction=pymongo.DESCENDING).limit(1)
            items = await cursor.to_list(length=1)
            if not items:
                return None
            item = items[0]

            return Snapshot(tenant_id=item['TenantId'],
                            stream_id=item['StreamId'],
                            stream_revision=int(item['StreamRevision']),
                            commit_sequence=int(item['CommitSequence']),
                            payload=from_json(item['Payload']),
                            headers=from_json(item['Headers']))
        except Exception as e:
            raise Exception(
                f"Failed to get snapshot for stream {stream_id} with status code {e.response['ResponseMetadata']['HTTPStatusCode']}")

    async def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] = None):
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
            _ = await self.collection.insert_one(doc)
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with status code {e.response['ResponseMetadata']['HTTPStatusCode']}")
