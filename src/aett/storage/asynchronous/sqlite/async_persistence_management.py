import logging

import aiosqlite
from typing import Iterable, AsyncIterable

from aett.eventstore import (
    IManagePersistenceAsync,
    TopicMap,
    COMMITS,
    SNAPSHOTS,
    Commit,
)
from aett.storage.asynchronous.sqlite.functions import _item_to_commit


class AsyncPersistenceManagement(IManagePersistenceAsync):
    def __init__(
        self,
        connection_string: str,
        topic_map: TopicMap,
        commits_table_name: str = COMMITS,
        snapshots_table_name: str = SNAPSHOTS,
    ):
        self._connection_string: str = connection_string
        self._topic_map = topic_map
        self._commits_table_name = commits_table_name
        self._snapshots_table_name = snapshots_table_name

    async def initialize(self):
        try:
            async with aiosqlite.connect(self._connection_string) as connection:
                c: aiosqlite.Cursor = await connection.cursor()
                await c.execute(f"""CREATE TABLE IF NOT EXISTS {self._commits_table_name}
    (
           TenantId varchar(40) NOT NULL,
           StreamId char(40) NOT NULL,
           StreamIdOriginal text NOT NULL,
           StreamRevision int NOT NULL CHECK (StreamRevision > 0),
           Items int NOT NULL CHECK (Items > 0),
           CommitId guid NOT NULL CHECK (CommitId != 0),
           CommitSequence int NOT NULL CHECK (CommitSequence > 0),
           CommitStamp datetime NOT NULL,
           CheckpointNumber INTEGER PRIMARY KEY AUTOINCREMENT,
           Headers blob NULL,
           Payload blob NOT NULL
    );""")
                await c.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS IX_Commits_CommitSequence ON {self._commits_table_name} (TenantId, StreamId, CommitSequence);"
                )
                await c.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS IX_Commits_CommitId ON {self._commits_table_name} (TenantId, StreamId, CommitId);"
                )
                await c.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS IX_Commits_Revisions ON {self._commits_table_name} (TenantId, StreamId, StreamRevision, Items);"
                )
                await c.execute(
                    f"CREATE INDEX IF NOT EXISTS IX_Commits_Stamp ON {self._commits_table_name} (CommitStamp);"
                )
                await c.execute(f"""CREATE TABLE IF NOT EXISTS {self._snapshots_table_name}
    (
           TenantId varchar(40) NOT NULL,
           StreamId char(40) NOT NULL,
           StreamRevision int NOT NULL CHECK (StreamRevision > 0),
           CommitSequence int NOT NULL CHECK (CommitSequence > 0),
           Payload blob NOT NULL,
           CONSTRAINT PK_Snapshots PRIMARY KEY (TenantId, StreamId, StreamRevision)
    );""")
                await connection.commit()
        except Exception as e:
            logging.error(
                f"Failed to initialize persistence with error {e}", exc_info=True
            )
            pass

    async def drop(self):
        async with aiosqlite.connect(self._connection_string, timeout=30) as connection:
            c: aiosqlite.Cursor = await connection.cursor()
            await c.execute(f"DROP TABLE {self._snapshots_table_name};")
            await c.execute(f"DROP TABLE {self._commits_table_name};")
            await connection.commit()

    async def purge(self, tenant_id: str):
        async with aiosqlite.connect(self._connection_string) as connection:
            c: aiosqlite.Cursor = await connection.cursor()
            await c.execute(
                f"""DELETE FROM {self._commits_table_name} WHERE TenantId = %s;""",
                tenant_id,
            )
            await c.execute(
                f"""DELETE FROM {self._snapshots_table_name} WHERE TenantId = %s;""",
                tenant_id,
            )
            await connection.commit()

    async def get_from(self, checkpoint: int) -> AsyncIterable[Commit]:
        async with aiosqlite.connect(self._connection_string) as connection:
            cur: aiosqlite.Cursor = await connection.cursor()
            await cur.execute(
                f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                                  FROM {self._commits_table_name}
                                 WHERE CommitStamp >= %s
                                 ORDER BY CheckpointNumber;""",
                (checkpoint,),
            )
            fetchall = await cur.fetchall()
            for doc in fetchall:
                yield _item_to_commit(doc, self._topic_map)
