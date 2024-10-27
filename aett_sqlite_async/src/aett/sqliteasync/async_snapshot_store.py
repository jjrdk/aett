import aiosqlite
import typing

from pydantic_core import from_json, to_json

from aett.eventstore import IAccessSnapshotsAsync, SNAPSHOTS, MAX_INT, Snapshot


class AsyncSnapshotStore(IAccessSnapshotsAsync):
    def __init__(self, connection_string: str, table_name: str = SNAPSHOTS):
        self._connection_string: str = connection_string
        self._table_name = table_name

    async def get(self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        try:
            async with aiosqlite.connect(self._connection_string, autocommit=True) as connection:
                cur = await connection.cursor()
                await cur.execute(f"""SELECT *
              FROM {self._table_name}
              WHERE TenantId = ?
               AND StreamId = ?
               AND StreamRevision <= ?
             ORDER BY StreamRevision DESC
             LIMIT 1;""", (tenant_id, stream_id, max_revision))
                item = await cur.fetchone()
                if item is None:
                    return None

                return Snapshot(tenant_id=item[0],
                                stream_id=item[1],
                                stream_revision=int(item[2]),
                                commit_sequence=int(item[3]),
                                payload=from_json(item[4]),
                                headers={})

        except Exception as e:
            raise Exception(
                f"Failed to get snapshot for stream {stream_id} with error {e}")

    async def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] = None):
        try:
            async with aiosqlite.connect(self._connection_string, autocommit=True) as connection:
                cur = await connection.cursor()
                await cur.execute(
                    f"""INSERT INTO {self._table_name} ( TenantId, StreamId, StreamRevision, CommitSequence, Payload) VALUES (?, ?, ?, ?, ?);""",
                    (snapshot.tenant_id,
                     snapshot.stream_id,
                     snapshot.stream_revision,
                     snapshot.commit_sequence,
                     to_json(snapshot.payload)))
                await cur.close()

        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with error {e}")
