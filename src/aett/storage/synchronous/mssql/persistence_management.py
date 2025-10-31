from typing import Iterable

from mssql_python import connect

from aett.eventstore import IManagePersistence, TopicMap, COMMITS, SNAPSHOTS, Commit
from aett.storage.synchronous.mssql import _item_to_commit


class PersistenceManagement(IManagePersistence):
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

    def initialize(self):
        try:
            with connect(
                    self._connection_string, autocommit=True
            ) as connection:
                with connection.cursor() as c:
                    c.execute(f"""IF EXISTS(SELECT * FROM sysobjects WHERE name='{self._commits_table_name}' AND xtype = 'U') RETURN;
                    CREATE TABLE [dbo].[{self._commits_table_name}]
                    (
                           [TenantId] [varchar](64) NOT NULL,
                           [StreamId] [char](64) NOT NULL,
                           [StreamIdOriginal] [nvarchar](1000) NOT NULL,
                           [StreamRevision] [int] NOT NULL CHECK ([StreamRevision] > 0),
                           [Items] [tinyint] NOT NULL CHECK ([Items] > 0),
                           [CommitId] [uniqueidentifier] NOT NULL CHECK ([CommitId] != 0x0),
                           [CommitSequence] [int] NOT NULL CHECK ([CommitSequence] > 0),
                           [CommitStamp] [datetime] NOT NULL,
                           [CheckpointNumber] [bigint] IDENTITY NOT NULL,
                           [Headers] [varbinary](MAX) NULL CHECK ([Headers] IS NULL OR DATALENGTH([Headers]) > 0),
                           [Payload] [varbinary](MAX) NOT NULL CHECK (DATALENGTH([Payload]) > 0),
                           CONSTRAINT [PK_Commits] PRIMARY KEY CLUSTERED ([CheckpointNumber])
                    );
                    CREATE UNIQUE NONCLUSTERED INDEX [IX_Commits_CommitSequence] ON {self._commits_table_name} (TenantId, StreamId, CommitSequence);
                    CREATE UNIQUE NONCLUSTERED INDEX [IX_Commits_CommitId] ON [dbo].[{self._commits_table_name}] ([TenantId], [StreamId], [CommitId]);
                    CREATE UNIQUE NONCLUSTERED INDEX [IX_Commits_Revisions] ON [dbo].[{self._commits_table_name}] ([TenantId], [StreamId], [StreamRevision], [Items]);
                    CREATE INDEX [IX_Commits_Stamp] ON {self._commits_table_name} ([CommitStamp]);
                    CREATE TABLE [dbo].[{self._snapshots_table_name}]
                    (
                           [TenantId] [varchar](64) NOT NULL,
                           [StreamId] [char](64) NOT NULL,
                           [StreamRevision] [int] NOT NULL CHECK ([StreamRevision] > 0),
                           [CommitSequence] [int] NOT NULL CHECK ([CommitSequence] > 0),
                           [Payload] [varbinary](MAX) NOT NULL CHECK (DATALENGTH([Payload]) > 0),
                           [Headers] [varbinary](MAX) NULL CHECK ([Headers] IS NULL OR DATALENGTH([Headers]) > 0),
                           CONSTRAINT [PK_Snapshots] PRIMARY KEY CLUSTERED ([TenantId], [StreamId], [StreamRevision])
                    );""")
                connection.commit()
        except Exception:
            pass

    def drop(self):
        with connect(self._connection_string, autocommit=True) as connection:
            with connection.cursor() as c:
                c.execute(
                    f"""DROP TABLE {self._snapshots_table_name};"""
                )
                c.execute(
                    f"""DROP TABLE {self._commits_table_name};"""
                )

    def purge(self, tenant_id: str):
        with connect(self._connection_string, autocommit=True) as connection:
            with connection.cursor() as c:
                c.execute(
                    f"""DELETE FROM {self._commits_table_name} WHERE TenantId = ?;""",
                    tenant_id,
                )
                c.execute(
                    f"""DELETE FROM {self._snapshots_table_name} WHERE TenantId = ?;""",
                    tenant_id,
                )

    def get_from(self, checkpoint: int) -> Iterable[Commit]:
        with connect(self._connection_string, autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(
                    f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                                  FROM {self._commits_table_name}
                                 WHERE CommitStamp >= ?
                                 ORDER BY CheckpointNumber;""",
                    (checkpoint,),
                )
                fetchall = cur.fetchall()
                for doc in fetchall:
                    yield _item_to_commit(doc, self._topic_map)
