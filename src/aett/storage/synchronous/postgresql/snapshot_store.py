import typing

import psycopg
from pydantic_core import from_json, to_json

from aett.eventstore import IAccessSnapshots, SNAPSHOTS, MAX_INT, Snapshot


class SnapshotStore(IAccessSnapshots):
    def __init__(self, connection_string: str, table_name: str = SNAPSHOTS):
        self._connection_string: str = connection_string
        self._table_name = table_name

    def get(
        self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT
    ) -> Snapshot | None:
        try:
            with psycopg.connect(
                self._connection_string, autocommit=True
            ) as connection:
                with connection.cursor() as cur:
                    cur.execute(
                        f"""SELECT *
          FROM {self._table_name}
         WHERE TenantId = %s
           AND StreamId = %s
           AND StreamRevision <= %s
         ORDER BY StreamRevision DESC
         LIMIT 1;""",
                        (tenant_id, stream_id, max_revision),
                    )
                    item = cur.fetchone()
                    if item is None:
                        return None

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

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] | None = None):
        if headers is None:
            headers = {}
        try:
            with psycopg.connect(
                self._connection_string, autocommit=True
            ) as connection:
                with connection.cursor() as cur:
                    cur.execute(
                        f"""INSERT INTO {self._table_name} ( TenantId, StreamId, StreamRevision, CommitSequence, Payload, Headers) VALUES (%s, %s, %s, %s, %s, %s);""",
                        (
                            snapshot.tenant_id,
                            snapshot.stream_id,
                            snapshot.stream_revision,
                            snapshot.commit_sequence,
                            to_json(snapshot.payload),
                            to_json(headers),
                        ),
                    )
                    connection.commit()
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with error {e}"
            )
