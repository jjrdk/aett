import datetime
import typing
from typing import Iterable
from uuid import UUID

import jsonpickle
import psycopg

from aett.domain import ConflictDetector, ConflictingCommitException, NonConflictingCommitException, \
    DuplicateCommitException
from aett.eventstore import ICommitEvents, IAccessSnapshots, Snapshot, Commit, MAX_INT, TopicMap, \
    EventMessage


# noinspection DuplicatedCode
class CommitStore(ICommitEvents):
    def __init__(self, db: psycopg.connect, topic_map: TopicMap, conflict_detector: ConflictDetector = None,
                 table_name='commits'):
        self._topic_map = topic_map
        self._connection: psycopg.connect = db
        self._conflict_detector = conflict_detector if conflict_detector is not None else ConflictDetector()
        self._table_name = table_name

    def get(self, bucket_id: str, stream_id: str, min_revision: int = 0,
            max_revision: int = MAX_INT) -> typing.Iterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        cur = self._connection.cursor()
        cur.execute(f"""SELECT BucketId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
  FROM {self._table_name}
 WHERE BucketId = %s
   AND StreamId = %s
   AND StreamRevision >= %s
   AND (StreamRevision - Items) < %s
   AND CommitSequence > %s
 ORDER BY CommitSequence;""", (bucket_id, stream_id, min_revision, max_revision, 0))
        fetchall = cur.fetchall()
        for doc in fetchall:
            yield self._item_to_commit(doc)

    def get_to(self, bucket_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        cur = self._connection.cursor()
        cur.execute(f"""SELECT BucketId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
          FROM {self._table_name}
         WHERE BucketId = %s
           AND StreamId = %s
           AND CommitStamp <= %s
         ORDER BY CommitSequence;""", (bucket_id, stream_id, max_time))
        fetchall = cur.fetchall()
        for doc in fetchall:
            yield self._item_to_commit(doc)

    def get_all_to(self, bucket_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        cur = self._connection.cursor()
        cur.execute(f"""SELECT BucketId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                  FROM {self._table_name}
                 WHERE BucketId = %s
                   AND CommitStamp <= %s
                 ORDER BY CheckpointNumber;""", (bucket_id, max_time))
        fetchall = cur.fetchall()
        for doc in fetchall:
            yield self._item_to_commit(doc)

    def _item_to_commit(self, item):
        return Commit(bucket_id=item[0],
                      stream_id=item[1],
                      stream_revision=item[3],
                      commit_id=item[4],
                      commit_sequence=item[5],
                      commit_stamp=item[6],
                      headers=jsonpickle.decode(item[8]),
                      events=[EventMessage.from_json(e, self._topic_map) for e in jsonpickle.decode(item[9])],
                      checkpoint_token=item[7])

    def commit(self, commit: Commit):
        try:
            commit_seq_cur: psycopg.Cursor = self._connection.cursor()
            commit_seq_cur.execute(
                f"""SELECT MAX(CommitSequence) FROM {self._table_name} WHERE BucketId = %s AND StreamId = %s;""",
                (commit.bucket_id, commit.stream_id))
            commit_sequence = commit_seq_cur.fetchone()
            commit_sequence = 0 if commit_sequence[0] is None else int(commit_sequence[0])
            commit_seq_cur.close()
            cur = self._connection.cursor()
            cur.execute(f"""INSERT
  INTO {self._table_name}
     ( BucketId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp, Headers, Payload )
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING CheckpointNumber;""", (commit.bucket_id, commit.stream_id, commit.stream_id,
                                 commit.commit_id, commit_sequence + 1, commit.stream_revision, len(commit.events),
                                 commit.commit_stamp,
                                 jsonpickle.encode(commit.headers, unpicklable=False).encode('utf-8'),
                                 jsonpickle.encode([e.to_json() for e in commit.events], unpicklable=False).encode(
                                     'utf-8')))
            checkpoint_number = cur.fetchone()
            cur.close()
            self._connection.commit()
            return Commit(bucket_id=commit.bucket_id,
                          stream_id=commit.stream_id,
                          stream_revision=commit.stream_revision,
                          commit_id=commit.commit_id,
                          commit_sequence=commit_sequence + 1,
                          commit_stamp=commit.commit_stamp,
                          headers=commit.headers,
                          events=commit.events,
                          checkpoint_token=checkpoint_number)
        except psycopg.errors.UniqueViolation as e:
            if self._detect_duplicate(commit.commit_id, commit.bucket_id, commit.stream_id):
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

    def _detect_duplicate(self, commit_id: UUID, bucket_id: str, stream_id: str) -> bool:
        try:
            cur: psycopg.Cursor = self._connection.cursor()
            cur.execute(f"""SELECT COUNT(*)
  FROM {self._table_name}
 WHERE BucketId = %s
   AND StreamId = %s
   AND CommitId = %s;""", (bucket_id, stream_id, str(commit_id)))
            result = cur.fetchone()
            cur.close()
            return result[0] > 0
        except Exception as e:
            raise Exception(f"Failed to detect duplicate commit {commit_id} with error {e}")

    def _detect_conflicts(self, commit: Commit) -> (bool, int):
        cur = self._connection.cursor()
        cur.execute(f"""SELECT StreamRevision, Payload
                  FROM {self._table_name}
                 WHERE BucketId = %s
                   AND StreamId = %s
                   AND StreamRevision <= %s
                 ORDER BY CommitSequence;""", (commit.bucket_id, commit.stream_id, commit.stream_revision))
        fetchall = cur.fetchall()
        latest_revision = 0
        for doc in fetchall:
            events = [EventMessage.from_json(e, self._topic_map) for e in jsonpickle.decode(doc[1])]
            if self._conflict_detector.conflicts_with(list(map(self._get_body, commit.events)),
                                                      list(map(self._get_body, events))):
                return True, -1
            if doc[0] > latest_revision:
                latest_revision = int(doc[0])
        return False, latest_revision

    @staticmethod
    def _get_body(e):
        return e.body


class SnapshotStore(IAccessSnapshots):
    def __init__(self, db: psycopg.connect, table_name: str = 'snapshots'):
        self.connection: psycopg.connect = db
        self._table_name = table_name

    def get(self, bucket_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        try:
            cur = self.connection.cursor()
            cur.execute(f"""SELECT *
  FROM {self._table_name}
 WHERE BucketId = %s
   AND StreamId = %s
   AND StreamRevision <= %s
 ORDER BY StreamRevision DESC
 LIMIT 1;""", (bucket_id, stream_id, max_revision))
            item = cur.fetchone()
            if item is None:
                return None

            return Snapshot(bucket_id=item[0],
                            stream_id=item[1],
                            stream_revision=int(item[2]),
                            payload=jsonpickle.decode(item[3].decode('utf-8')),
                            headers=dict(jsonpickle.decode(item[4].decode('utf-8'))))
        except Exception as e:
            raise Exception(
                f"Failed to get snapshot for stream {stream_id} with error {e}")

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] = None):
        if headers is None:
            headers = {}
        try:
            cur = self.connection.cursor()
            j = jsonpickle.encode(snapshot.payload, unpicklable=False)
            cur.execute(
                f"""INSERT INTO {self._table_name} ( BucketId, StreamId, StreamRevision, Payload, Headers) VALUES (%s, %s, %s, %s, %s);""",
                (snapshot.bucket_id,
                 snapshot.stream_id,
                 snapshot.stream_revision,
                 j.encode('utf-8'),
                 jsonpickle.encode(headers, unpicklable=False).encode('utf-8')))
            self.connection.commit()
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with error {e}")


class PersistenceManagement:
    def __init__(self,
                 db: psycopg.connect,
                 commits_table_name: str = 'commits',
                 snapshots_table_name: str = 'snapshots'):
        self.db: psycopg.connect = db
        self.commits_table_name = commits_table_name
        self.snapshots_table_name = snapshots_table_name

    def initialize(self):
        try:
            c = self.db.cursor()
            c.execute(f"""CREATE TABLE {self.commits_table_name}
(
    BucketId varchar(64) NOT NULL,
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
CREATE UNIQUE INDEX IX_Commits_CommitSequence ON {self.commits_table_name} (BucketId, StreamId, CommitSequence);
CREATE UNIQUE INDEX IX_Commits_CommitId ON {self.commits_table_name} (BucketId, StreamId, CommitId);
CREATE UNIQUE INDEX IX_Commits_Revisions ON {self.commits_table_name} (BucketId, StreamId, StreamRevision, Items);
CREATE INDEX IX_Commits_Stamp ON {self.commits_table_name} (CommitStamp);

CREATE TABLE {self.snapshots_table_name}
(
    BucketId varchar(40) NOT NULL,
    StreamId char(40) NOT NULL,
    StreamRevision int NOT NULL CHECK (StreamRevision > 0),
    Payload bytea NOT NULL,
    Headers bytea NOT NULL,
    CONSTRAINT PK_Snapshots PRIMARY KEY (BucketId, StreamId, StreamRevision)
);""")
            c.commit()
        except Exception as e:
            pass

    def drop(self):
        self.db.cursor().execute(f"""DROP TABLE {self.snapshots_table_name};DROP TABLE {self.commits_table_name};""")
