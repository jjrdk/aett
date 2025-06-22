import typing

from pymongo import DESCENDING
from pymongo.errors import PyMongoError
from pydantic_core import from_json, to_json
from pymongo import database

from aett.eventstore import Snapshot
from aett.eventstore.i_access_snapshots import IAccessSnapshots

from aett.eventstore.constants import SNAPSHOTS, MAX_INT


class SnapshotStore(IAccessSnapshots):
    def __init__(self, db: database.Database, table_name: str = SNAPSHOTS):
        self.collection: database.Collection = db.get_collection(table_name)

    def get(
            self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT
    ) -> Snapshot | None:
        try:
            filters = {
                "TenantId": tenant_id,
                "StreamId": stream_id,
                "StreamRevision": {"$lte": max_revision},
            }
            cursor = (
                self.collection.find({"$and": [filters]})
                .sort("StreamRevision", direction=DESCENDING)
                .limit(1)
            )
            item = next(cursor, None)
            if item is None:
                return None

            return Snapshot(
                tenant_id=item["TenantId"],
                stream_id=item["StreamId"],
                stream_revision=int(item["StreamRevision"]),
                commit_sequence=int(item["CommitSequence"]),
                payload=from_json(item["Payload"]),
                headers=from_json(item["Headers"]),
            )
        except Exception as e:
            raise Exception(
                f"Failed to get snapshot for stream {stream_id} with status code {e}"
            )

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] | None = None):
        if headers is None:
            headers = {}
        try:
            doc = {
                "TenantId": snapshot.tenant_id,
                "StreamId": snapshot.stream_id,
                "StreamRevision": snapshot.stream_revision,
                "CommitSequence": snapshot.commit_sequence,
                "Payload": to_json(snapshot.payload),
                "Headers": to_json(headers),
            }
            _ = self.collection.insert_one(doc)
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with status code {e}"
            )
