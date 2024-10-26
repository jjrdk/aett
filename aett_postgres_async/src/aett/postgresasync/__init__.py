import datetime
import typing
from typing import Iterable
from uuid import UUID

import asyncpg
from pydantic_core import to_json, from_json

from aett.domain.non_conflicting_commit_exception import NonConflictingCommitException
from aett.domain.conflicting_commit_exception import ConflictingCommitException
from aett.domain.duplicate_commit_exception import DuplicateCommitException
from aett.domain.conflict_detector import ConflictDetector
from aett.eventstore import Snapshot, Commit, MAX_INT, TopicMap, \
    EventMessage, COMMITS, SNAPSHOTS, IAccessSnapshotsAsync, ICommitEventsAsync
from aett.eventstore.i_manage_persistence_async import IManagePersistenceAsync


def _item_to_commit(item, topic_map: TopicMap):
    return Commit(tenant_id=item[0],
                  stream_id=item[1],
                  stream_revision=item[3],
                  commit_id=item[4],
                  commit_sequence=item[5],
                  commit_stamp=item[6],
                  headers=from_json(item[8]),
                  events=[EventMessage.from_json(e, topic_map) for e in from_json(item[9])],
                  checkpoint_token=item[7])


# noinspection DuplicatedCode
class AsyncCommitStore(ICommitEventsAsync):
    def __init__(self, connection_string: str, topic_map: TopicMap, conflict_detector: ConflictDetector = None,
                 table_name=COMMITS):
        self._topic_map = topic_map
        self._connection_string = connection_string
        self._conflict_detector = conflict_detector if conflict_detector is not None else ConflictDetector()
        self._table_name = table_name

    async def get(self, tenant_id: str, stream_id: str, min_revision: int = 0,
                  max_revision: int = MAX_INT) -> typing.AsyncIterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        connection = await asyncpg.connect(self._connection_string)
        fetchall = await connection.fetch(f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
          FROM {self._table_name}
         WHERE TenantId = $1
           AND StreamId = $2
           AND StreamRevision >= $3
           AND (StreamRevision - Items) < $4
           AND CommitSequence > $5
         ORDER BY CommitSequence;""", tenant_id, stream_id, min_revision, max_revision, 0)
        for doc in fetchall:
            yield _item_to_commit(doc, self._topic_map)

    async def get_to(self, tenant_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            typing.AsyncIterable[Commit]:
        connection = await asyncpg.connect(self._connection_string)
        fetchall = await connection.fetch(f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                  FROM {self._table_name}
                 WHERE TenantId = $1
                   AND StreamId = $2
                   AND CommitStamp <= $3
                 ORDER BY CommitSequence;""", tenant_id, stream_id, max_time)
        for doc in fetchall:
            yield _item_to_commit(doc, self._topic_map)

    async def get_all_to(self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        connection = await asyncpg.connect(self._connection_string)
        fetchall = await connection.fetch(f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                          FROM {self._table_name}
                         WHERE TenantId = %s
                           AND CommitStamp <= %s
                         ORDER BY CheckpointNumber;""", (tenant_id, max_time))
        for doc in fetchall:
            yield _item_to_commit(doc, self._topic_map)

    async def commit(self, commit: Commit):
        try:
            connection = await asyncpg.connect(self._connection_string)
            json = to_json([e.to_json() for e in commit.events])
            fetchrow = await connection.fetchrow(f"""INSERT
              INTO {self._table_name}
                 ( TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp, Headers, Payload )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING CheckpointNumber;""", commit.tenant_id, commit.stream_id, commit.stream_id,
                                                 commit.commit_id, commit.commit_sequence, commit.stream_revision,
                                                 len(commit.events),
                                                 commit.commit_stamp,
                                                 to_json(commit.headers),
                                                 json)
            checkpoint_number = fetchrow['checkpointnumber']
            await connection.close()
            return Commit(tenant_id=commit.tenant_id,
                          stream_id=commit.stream_id,
                          stream_revision=commit.stream_revision,
                          commit_id=commit.commit_id,
                          commit_sequence=commit.commit_sequence,
                          commit_stamp=commit.commit_stamp,
                          headers=commit.headers,
                          events=commit.events,
                          checkpoint_token=checkpoint_number)

        except asyncpg.exceptions.UniqueViolationError:
            if await self._detect_duplicate(commit.commit_id, commit.tenant_id, commit.stream_id):
                raise DuplicateCommitException(
                    f"Commit {commit.commit_id} already exists in stream {commit.stream_id}")
            else:
                conflicts, revision = await self._detect_conflicts(commit=commit)
            if conflicts:
                raise ConflictingCommitException(
                    f"Conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}")
            else:
                raise NonConflictingCommitException(
                    f'Non-conflicting version conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}')
        except Exception as e:
            raise Exception(f"Failed to commit {commit.commit_id} with error {e}")

    async def _detect_duplicate(self, commit_id: UUID, tenant_id: str, stream_id: str) -> bool:
        try:
            connection = await asyncpg.connect(self._connection_string)
            result = await connection.fetch(f"""SELECT COUNT(*)
                  FROM {self._table_name}
                 WHERE TenantId = $1
                   AND StreamId = $2
                   AND CommitId = $3;""", tenant_id, stream_id, str(commit_id))
            await connection.close()
            count = result[0][0]
            return count > 0
        except Exception as e:
            raise Exception(f"Failed to detect duplicate commit {commit_id} with error {e}")

    async def _detect_conflicts(self, commit: Commit) -> (bool, int):
        connection = await asyncpg.connect(self._connection_string)
        fetchall = await connection.fetch(f"""SELECT StreamRevision, Payload
                                  FROM {self._table_name}
                                 WHERE TenantId = $1
                                   AND StreamId = $2
                                   AND StreamRevision <= $3
                                 ORDER BY CommitSequence;""",
                                          commit.tenant_id, commit.stream_id, commit.stream_revision)
        latest_revision = 0
        for doc in fetchall:
            events = [EventMessage.from_json(e, self._topic_map) for e in from_json(doc[1])]
            uncommitted_events = list(map(self._get_body, commit.events))
            committed_events = list(map(self._get_body, events))
            if self._conflict_detector.conflicts_with(uncommitted_events, committed_events):
                return True, -1
            if doc[0] > latest_revision:
                latest_revision = int(doc[0])
        return False, latest_revision

    @staticmethod
    def _get_body(e):
        return e.body


class AsyncSnapshotStore(IAccessSnapshotsAsync):
    def __init__(self, connection_string: str, table_name: str = SNAPSHOTS):
        self._connection_string: str = connection_string
        self._table_name = table_name

    async def get(self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        try:
            connection = await asyncpg.connect(self._connection_string)
            results = await connection.fetch(f"""SELECT *
          FROM {self._table_name}
         WHERE TenantId = $1
           AND StreamId = $2
           AND StreamRevision <= $3
         ORDER BY StreamRevision DESC
         LIMIT 1;""", tenant_id, stream_id, max_revision)
            if not results:
                return None
            item = results[0]

            return Snapshot(tenant_id=item[0],
                            stream_id=item[1],
                            stream_revision=int(item[2]),
                            commit_sequence=int(item[3]),
                            payload=from_json(item[4]),
                            headers=dict(from_json(item[5])))
        except Exception as e:
            raise Exception(
                f"Failed to get snapshot for stream {stream_id} with error {e}")

    async def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] = None):
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
                to_json(headers))
            await connection.close()
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with error {e}")


class AsyncPersistenceManagement(IManagePersistenceAsync):
    def __init__(self,
                 connection_string: str,
                 topic_map: TopicMap,
                 commits_table_name: str = COMMITS,
                 snapshots_table_name: str = SNAPSHOTS):
        self._connection_string: str = connection_string
        self._topic_map = topic_map
        self._commits_table_name = commits_table_name
        self._snapshots_table_name = snapshots_table_name

    async def initialize(self):
        try:
            connection = await asyncpg.connect(self._connection_string)
            await connection.execute(f"""CREATE TABLE {self._commits_table_name}
        (
            TenantId varchar(64) NOT NULL,
            StreamId char(64) NOT NULL,
            StreamIdOriginal varchar(1000) NOT NULL,
            StreamRevision int NOT NULL CHECK (StreamRevision > 0),
            Items smallint NOT NULL CHECK (Items > 0),
            CommitId uuid NOT NULL,
            CommitSequence int NOT NULL CHECK (CommitSequence > 0),
            CommitStamp timestamp with time zone NOT NULL,
            CheckpointNumber BIGSERIAL NOT NULL,
            Headers bytea NULL,
            Payload bytea NOT NULL,
            CONSTRAINT PK_Commits PRIMARY KEY (CheckpointNumber)
        );
        CREATE UNIQUE INDEX IX_Commits_CommitSequence ON {self._commits_table_name} (TenantId, StreamId, CommitSequence);
        CREATE UNIQUE INDEX IX_Commits_CommitId ON {self._commits_table_name} (TenantId, StreamId, CommitId);
        CREATE UNIQUE INDEX IX_Commits_Revisions ON {self._commits_table_name} (TenantId, StreamId, StreamRevision, Items);
        CREATE INDEX IX_Commits_Stamp ON {self._commits_table_name} (CommitStamp);
        
        CREATE TABLE {self._snapshots_table_name}
        (
            TenantId varchar(40) NOT NULL,
            StreamId char(40) NOT NULL,
            StreamRevision int NOT NULL CHECK (StreamRevision > 0),
            CommitSequence int NOT NULL CHECK (CommitSequence > 0),
            Payload bytea NOT NULL,
            Headers bytea NOT NULL,
            CONSTRAINT PK_Snapshots PRIMARY KEY (TenantId, StreamId, StreamRevision)
        );""")
            # await connection.commit()

        except Exception as e:
            print(e)

    async def drop(self):
        with await asyncpg.connect(self._connection_string) as connection:
            await connection.execute(
                f"""DROP TABLE {self._snapshots_table_name};DROP TABLE {self._commits_table_name};""")
            await connection.commit()

    async def purge(self, tenant_id: str):
        with await asyncpg.connect(self._connection_string) as connection:
            await connection.execute(f"""DELETE FROM {self._commits_table_name} WHERE TenantId = %s;""", tenant_id)
            await connection.execute(f"""DELETE FROM {self._snapshots_table_name} WHERE TenantId = %s;""", tenant_id)
            await connection.commit()

    async def get_from(self, checkpoint: int) -> Iterable[Commit]:
        with await asyncpg.connect(self._connection_string) as connection:
            fetchall = await connection.execute(f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                                      FROM {self._commits_table_name}
                                     WHERE CommitStamp >= %s
                                     ORDER BY CheckpointNumber;""", (checkpoint,))
            for doc in fetchall:
                yield _item_to_commit(doc, self._topic_map)
