import sqlite3
from typing import Iterable

from sqlite3 import Cursor

from aett.eventstore import IManagePersistence, TopicMap, COMMITS, SNAPSHOTS, Commit
from aett.sqlite import _item_to_commit


class PersistenceManagement(IManagePersistence):
    def __init__(self,
                 connection_string: str,
                 topic_map: TopicMap,
                 commits_table_name: str = COMMITS,
                 snapshots_table_name: str = SNAPSHOTS):
        self._connection_string: str = connection_string
        self._topic_map = topic_map
        self._commits_table_name = commits_table_name
        self._snapshots_table_name = snapshots_table_name

    def initialize(self):
        try:
            with sqlite3.connect(self._connection_string, autocommit=True) as connection:
                c: sqlite3.Cursor = connection.cursor()
                c.execute(f"""CREATE TABLE IF NOT EXISTS {self._commits_table_name}
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
                c.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS IX_Commits_CommitSequence ON Commits (TenantId, StreamId, CommitSequence);")
                c.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS IX_Commits_CommitId ON Commits (TenantId, StreamId, CommitId);")
                c.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS IX_Commits_Revisions ON Commits (TenantId, StreamId, StreamRevision, Items);")
                c.execute("CREATE INDEX IF NOT EXISTS IX_Commits_Stamp ON Commits (CommitStamp);")
                c.execute(f"""CREATE TABLE IF NOT EXISTS {self._snapshots_table_name}
    (
           TenantId varchar(40) NOT NULL,
           StreamId char(40) NOT NULL,
           StreamRevision int NOT NULL CHECK (StreamRevision > 0),
           CommitSequence int NOT NULL CHECK (CommitSequence > 0),
           Payload blob NOT NULL,
           CONSTRAINT PK_Snapshots PRIMARY KEY (TenantId, StreamId, StreamRevision)
    );""")
        except Exception as e:
            print(e)
            pass

    def drop(self):
        with sqlite3.connect(self._connection_string, autocommit=True) as connection:
            c: Cursor = connection.cursor()
            c.execute(f"DROP TABLE {self._snapshots_table_name};")
            c.execute(f"DROP TABLE {self._commits_table_name};")

    def purge(self, tenant_id: str):
        with sqlite3.connect(self._connection_string, autocommit=True) as connection:
            c: sqlite3.Cursor = connection.cursor()
            c.execute(f"""DELETE FROM {self._commits_table_name} WHERE TenantId = %s;""", tenant_id)
            c.execute(f"""DELETE FROM {self._snapshots_table_name} WHERE TenantId = %s;""", tenant_id)
            c.close()

    def get_from(self, checkpoint: int) -> Iterable[Commit]:
        with sqlite3.connect(self._connection_string, autocommit=True) as connection:
            cur: sqlite3.Cursor = connection.cursor()
            cur.execute(f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                                  FROM {self._commits_table_name}
                                 WHERE CommitStamp >= %s
                                 ORDER BY CheckpointNumber;""", (checkpoint,))
            fetchall = cur.fetchall()
            for doc in fetchall:
                yield _item_to_commit(doc, self._topic_map)
            cur.close()