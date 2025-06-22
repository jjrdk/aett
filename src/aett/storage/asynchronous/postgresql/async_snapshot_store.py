import typing

import asyncpg
from pydantic_core import from_json, to_json

from aett.eventstore import IAccessSnapshotsAsync, SNAPSHOTS, MAX_INT, Snapshot


class AsyncSnapshotStore(IAccessSnapshotsAsync):
    def __init__(self, connection_string: str, table_name: str = SNAPSHOTS):
        self._connection_string: str = connection_string
        self._table_name = table_name

    async def get(
        self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT
    ) -> Snapshot | None:
        try:
            connection = await asyncpg.connect(self._connection_string)
            results = await connection.fetch(
                f"""SELECT *
          FROM {self._table_name}
         WHERE TenantId = $1
           AND StreamId = $2
           AND StreamRevision <= $3
         ORDER BY StreamRevision DESC
         LIMIT 1;""",
                tenant_id,
                stream_id,
                max_revision,
            )
            if not results:
                return None
            item = results[0]

            return Snapshot(
                tenant_id=item[0],
                stream_id=item[1],
                stream_revision=int(item[2]),
                commit_sequence=int(item[3]),
                payload=from_json(item[4]),
                headers=dict(from_json(item[5])),
            )
        except Exception as e:
            raise Exception(
                f"Failed to get snapshot for stream {stream_id} with error {e}"
            )

    async def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] | None = None):
        if headers is None:
            headers = {}
        try:
            connection = await asyncpg.connect(self._connection_string)
            await connection.execute(
                f"""INSERT INTO {self._table_name} ( TenantId, StreamId, StreamRevision, CommitSequence, Payload, Headers) VALUES ($1, $2, $3, $4, $5, $6);""",
                snapshot.tenant_id,
                snapshot.stream_id,
                snapshot.stream_revision,
                snapshot.commit_sequence,
                to_json(snapshot.payload),
                to_json(headers),
            )
            await connection.close()
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with error {e}"
            )
