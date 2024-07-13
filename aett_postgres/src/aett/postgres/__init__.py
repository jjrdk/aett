import datetime
import typing
from pydantic_core import to_json, from_json
from typing import Iterable
from uuid import UUID
import psycopg

from aett.domain import ConflictDetector, ConflictingCommitException, NonConflictingCommitException, \
    DuplicateCommitException
from aett.eventstore import ICommitEvents, IAccessSnapshots, Snapshot, Commit, MAX_INT, TopicMap, \
    EventMessage, COMMITS, SNAPSHOTS, IManagePersistence


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
class CommitStore(ICommitEvents):
    def __init__(self, connection_string: str, topic_map: TopicMap, conflict_detector: ConflictDetector = None,
                 table_name=COMMITS):
        self._topic_map = topic_map
        self._connection_string = connection_string
        self._conflict_detector = conflict_detector if conflict_detector is not None else ConflictDetector()
        self._table_name = table_name

    def get(self, tenant_id: str, stream_id: str, min_revision: int = 0,
            max_revision: int = MAX_INT) -> typing.Iterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        with psycopg.connect(self._connection_string, autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
          FROM {self._table_name}
         WHERE TenantId = %s
           AND StreamId = %s
           AND StreamRevision >= %s
           AND (StreamRevision - Items) < %s
           AND CommitSequence > %s
         ORDER BY CommitSequence;""", (tenant_id, stream_id, min_revision, max_revision, 0))
                fetchall = cur.fetchall()
                for doc in fetchall:
                    yield _item_to_commit(doc, self._topic_map)

    def get_to(self, tenant_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        with psycopg.connect(self._connection_string, autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                  FROM {self._table_name}
                 WHERE TenantId = %s
                   AND StreamId = %s
                   AND CommitStamp <= %s
                 ORDER BY CommitSequence;""", (tenant_id, stream_id, max_time))
                fetchall = cur.fetchall()
                for doc in fetchall:
                    yield _item_to_commit(doc, self._topic_map)

    def get_all_to(self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        with psycopg.connect(self._connection_string, autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                          FROM {self._table_name}
                         WHERE TenantId = %s
                           AND CommitStamp <= %s
                         ORDER BY CheckpointNumber;""", (tenant_id, max_time))
                fetchall = cur.fetchall()
                for doc in fetchall:
                    yield _item_to_commit(doc, self._topic_map)

    def commit(self, commit: Commit):
        try:
            with psycopg.connect(self._connection_string, autocommit=True) as connection:
                with connection.cursor() as cur:
                    json = to_json([e.to_json() for e in commit.events])
                    cur.execute(f"""INSERT
          INTO {self._table_name}
             ( TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp, Headers, Payload )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING CheckpointNumber;""", (commit.tenant_id, commit.stream_id, commit.stream_id,
                                         commit.commit_id, commit.commit_sequence, commit.stream_revision,
                                         len(commit.events),
                                         commit.commit_stamp,
                                         to_json(commit.headers),
                                         json))
                    checkpoint_number = cur.fetchone()
                    cur.close()
                    connection.commit()
                    return Commit(tenant_id=commit.tenant_id,
                                  stream_id=commit.stream_id,
                                  stream_revision=commit.stream_revision,
                                  commit_id=commit.commit_id,
                                  commit_sequence=commit.commit_sequence,
                                  commit_stamp=commit.commit_stamp,
                                  headers=commit.headers,
                                  events=commit.events,
                                  checkpoint_token=checkpoint_number[0])
        except psycopg.errors.UniqueViolation:
            if self._detect_duplicate(commit.commit_id, commit.tenant_id, commit.stream_id):
                raise DuplicateCommitException(
                    f"Commit {commit.commit_id} already exists in stream {commit.stream_id}")
            else:
                conflicts, revision = self._detect_conflicts(commit=commit)
                if conflicts:
                    raise ConflictingCommitException(
                        f"Conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}")
                else:
                    raise NonConflictingCommitException(
                        f'Non-conflicting version conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}')
        except Exception as e:
            raise Exception(f"Failed to commit {commit.commit_id} with error {e}")

    def _detect_duplicate(self, commit_id: UUID, tenant_id: str, stream_id: str) -> bool:
        try:
            with psycopg.connect(self._connection_string, autocommit=True) as connection:
                with connection.cursor() as cur:
                    cur.execute(f"""SELECT COUNT(*)
          FROM {self._table_name}
         WHERE TenantId = %s
           AND StreamId = %s
           AND CommitId = %s;""", (tenant_id, stream_id, str(commit_id)))
                    result = cur.fetchone()
                    cur.close()
                    return result[0] > 0
        except Exception as e:
            raise Exception(f"Failed to detect duplicate commit {commit_id} with error {e}")

    def _detect_conflicts(self, commit: Commit) -> (bool, int):
        with psycopg.connect(self._connection_string, autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(f"""SELECT StreamRevision, Payload
                          FROM {self._table_name}
                         WHERE TenantId = %s
                           AND StreamId = %s
                           AND StreamRevision <= %s
                         ORDER BY CommitSequence;""", (commit.tenant_id, commit.stream_id, commit.stream_revision))
                fetchall = cur.fetchall()
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


class SnapshotStore(IAccessSnapshots):
    def __init__(self, connection_string: str, table_name: str = SNAPSHOTS):
        self._connection_string: str = connection_string
        self._table_name = table_name

    def get(self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        try:
            with psycopg.connect(self._connection_string, autocommit=True) as connection:
                with connection.cursor() as cur:
                    cur.execute(f"""SELECT *
          FROM {self._table_name}
         WHERE TenantId = %s
           AND StreamId = %s
           AND StreamRevision <= %s
         ORDER BY StreamRevision DESC
         LIMIT 1;""", (tenant_id, stream_id, max_revision))
                    item = cur.fetchone()
                    if item is None:
                        return None

                    return Snapshot(tenant_id=item[0],
                                    stream_id=item[1],
                                    stream_revision=int(item[2]),
                                    commit_sequence=int(item[3]),
                                    payload=from_json(item[4]),
                                    headers=dict(from_json(item[5])))
        except Exception as e:
            raise Exception(
                f"Failed to get snapshot for stream {stream_id} with error {e}")

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] = None):
        if headers is None:
            headers = {}
        try:
            with psycopg.connect(self._connection_string, autocommit=True) as connection:
                with connection.cursor() as cur:
                    cur.execute(
                        f"""INSERT INTO {self._table_name} ( TenantId, StreamId, StreamRevision, CommitSequence, Payload, Headers) VALUES (%s, %s, %s, %s, %s, %s);""",
                        (snapshot.tenant_id,
                         snapshot.stream_id,
                         snapshot.stream_revision,
                         snapshot.commit_sequence,
                         to_json(snapshot.payload),
                         to_json(headers)))
                    connection.commit()
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with error {e}")


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
            with psycopg.connect(self._connection_string, autocommit=True) as connection:
                with connection.cursor() as c:
                    c.execute(f"""CREATE TABLE {self._commits_table_name}
        (
            TenantId varchar(64) NOT NULL,
            StreamId char(64) NOT NULL,
            StreamIdOriginal varchar(1000) NOT NULL,
            StreamRevision int NOT NULL CHECK (StreamRevision > 0),
            Items smallint NOT NULL CHECK (Items > 0),
            CommitId uuid NOT NULL,
            CommitSequence int NOT NULL CHECK (CommitSequence > 0),
            CommitStamp timestamp NOT NULL,
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
                    c.commit()
        except Exception:
            pass

    def drop(self):
        with psycopg.connect(self._connection_string, autocommit=True) as connection:
            with connection.cursor() as c:
                c.execute(f"""DROP TABLE {self._snapshots_table_name};DROP TABLE {self._commits_table_name};""")

    def purge(self, tenant_id: str):
        with psycopg.connect(self._connection_string, autocommit=True) as connection:
            with connection.cursor() as c:
                c.execute(f"""DELETE FROM {self._commits_table_name} WHERE TenantId = %s;""", tenant_id)
                c.execute(f"""DELETE FROM {self._snapshots_table_name} WHERE TenantId = %s;""", tenant_id)

    def get_from(self, checkpoint: int) -> Iterable[Commit]:
        with psycopg.connect(self._connection_string, autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                                  FROM {self._commits_table_name}
                                 WHERE CommitStamp >= %s
                                 ORDER BY CheckpointNumber;""", (checkpoint,))
                fetchall = cur.fetchall()
                for doc in fetchall:
                    yield _item_to_commit(doc, self._topic_map)
