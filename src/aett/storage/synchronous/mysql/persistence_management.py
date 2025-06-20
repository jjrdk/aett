from typing import Iterable

import pymysql
from aett.eventstore import IManagePersistence, TopicMap, COMMITS, SNAPSHOTS, Commit
from aett.storage.synchronous.mysql import _item_to_commit


class PersistenceManagement(IManagePersistence):
    def __init__(
            self,
            host: str,
            user: str,
            password: str,
            database: str,
            topic_map: TopicMap,
            port: int = 3306,
            commits_table_name: str = COMMITS,
            snapshots_table_name: str = SNAPSHOTS,
    ):
        self.host: str = host
        self._user: str = user
        self._password: str = password
        self._database: str = database
        self._port: int = port
        self._topic_map = topic_map
        self._commits_table_name = commits_table_name
        self._snapshots_table_name = snapshots_table_name

    def initialize(self):
        try:
            stmts = [stmt.strip() for stmt in f"""CREATE TABLE IF NOT EXISTS {self._commits_table_name}
                     (
                         TenantId varchar(36) charset utf8 NOT NULL,
                         StreamId varchar(36) charset utf8 NOT NULL,
                         StreamIdOriginal varchar(1000) charset utf8 NOT NULL,
                         StreamRevision int NOT NULL CHECK (StreamRevision > 0),
                         Items tinyint NOT NULL CHECK (Items > 0),
                         CommitId binary(16) NOT NULL,
                         CommitSequence int NOT NULL CHECK (CommitSequence > 0),
                         CommitStamp TIMESTAMP NOT NULL,
                         CheckpointNumber bigint AUTO_INCREMENT,
                         Headers blob NULL,
                         Payload mediumblob NOT NULL,
                         PRIMARY KEY (CheckpointNumber)
                     );
                     CREATE UNIQUE INDEX IX_Commits ON {self._commits_table_name} (TenantId, StreamId, CommitSequence);
                     CREATE UNIQUE INDEX IX_Commits_CommitId ON {self._commits_table_name} (TenantId, StreamId, CommitId);
                     CREATE UNIQUE INDEX IX_Commits_Revisions ON {self._commits_table_name} (TenantId, StreamId, StreamRevision, Items);
                     CREATE INDEX IX_Commits_Stamp ON {self._commits_table_name} (CommitStamp);

                     CREATE TABLE IF NOT EXISTS {self._snapshots_table_name}
                     (
                         TenantId varchar(64) charset utf8 NOT NULL,
                         StreamId varchar(64) charset utf8 NOT NULL,
                         StreamRevision int NOT NULL CHECK (StreamRevision > 0),
                         CommitSequence int NOT NULL CHECK (CommitSequence > 0),
                         Payload blob NOT NULL,
                         Headers blob NOT NULL,
                         CONSTRAINT PK_Snapshots PRIMARY KEY (TenantId, StreamId, StreamRevision)
                     );""".split(';') if stmt.strip()]
            with pymysql.connect(host=self.host,
                                 user=self._user,
                                 password=self._password,
                                 database=self._database,
                                 port=self._port,
                                 autocommit=True) as connection:
                with connection.cursor() as c:
                    for stmt in stmts:
                        c.execute(query=stmt)
                connection.commit()
        except Exception as e:
            print(e)
            pass

    def drop(self):
        with pymysql.connect(host=self.host,
                             user=self._user,
                             password=self._password,
                             database=self._database,
                             port=self._port,
                             autocommit=True) as connection:
            with connection.cursor() as c:
                c.execute(
                    f"""DROP TABLE {self._snapshots_table_name};"""
                )
                c.execute(
                    f"""DROP TABLE {self._commits_table_name};"""
                )
            connection.commit()

    def purge(self, tenant_id: str):
        with pymysql.connect(host=self.host,
                             user=self._user,
                             password=self._password,
                             database=self._database,
                             port=self._port,
                             autocommit=True) as connection:
            with connection.cursor() as c:
                c.execute(
                    f"""DELETE FROM {self._commits_table_name} WHERE TenantId = %s;""",
                    tenant_id,
                )
                c.execute(
                    f"""DELETE FROM {self._snapshots_table_name} WHERE TenantId = %s;""",
                    tenant_id,
                )
            connection.commit()

    def get_from(self, checkpoint: int) -> Iterable[Commit]:
        with pymysql.connect(host=self.host,
                             user=self._user,
                             password=self._password,
                             database=self._database,
                             port=self._port,
                             autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(
                    f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                                  FROM {self._commits_table_name}
                                 WHERE CommitStamp >= %s
                                 ORDER BY CheckpointNumber;""",
                    (checkpoint,),
                )
                fetchall = cur.fetchall()
                for doc in fetchall:
                    yield _item_to_commit(doc, self._topic_map)
