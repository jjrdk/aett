import datetime
import typing
from typing import Iterable
from uuid import UUID

import jsonpickle
import psycopg

from aett.eventstore import ICommitEvents, EventStream, IAccessSnapshots, Snapshot, Commit, MAX_INT, TopicMap, \
    EventMessage


# noinspection DuplicatedCode
class CommitStore(ICommitEvents):
    def __init__(self, db: psycopg.connect, topic_map: TopicMap, table_name='commits'):
        self.topic_map = topic_map
        self.connection: psycopg.connect = db
        self._table_name = table_name

    def get(self, bucket_id: str, stream_id: str, min_revision: int = 0,
            max_revision: int = MAX_INT) -> typing.Iterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        cur = self.connection.cursor()
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
        cur = self.connection.cursor()
        cur.execute(f"""SELECT BucketId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
          FROM {self._table_name}
         WHERE BucketId = %s
           AND StreamId = %s
           AND CommitStamp <= %s
         ORDER BY CommitSequence;""", (bucket_id, stream_id, max_time))
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
                      events=[EventMessage.from_json(e, self.topic_map) for e in jsonpickle.decode(item[9])],
                      checkpoint_token=item[7])

    def commit(self, event_stream: EventStream, commit_id: UUID):
        try:
            commit = event_stream.to_commit(commit_id)
            cur = self.connection.cursor()
            cur.execute(f"""INSERT
  INTO {self._table_name}
     ( BucketId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp, Headers, Payload )
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING CheckpointNumber;""", (commit.bucket_id, commit.stream_id, commit.stream_id,
                                 commit_id, commit.commit_sequence, commit.stream_revision, len(commit.events),
                                 commit.commit_stamp,
                                 jsonpickle.encode(commit.headers, unpicklable=False).encode('utf-8'),
                                 jsonpickle.encode([e.to_json() for e in commit.events], unpicklable=False).encode(
                                     'utf-8')))
            checkpoint_number = cur.fetchone()
            self.connection.commit()
            return Commit(bucket_id=commit.bucket_id,
                          stream_id=commit.stream_id,
                          stream_revision=commit.stream_revision,
                          commit_id=commit_id,
                          commit_sequence=commit.commit_sequence,
                          commit_stamp=commit.commit_stamp,
                          headers=commit.headers,
                          events=commit.events,
                          checkpoint_token=checkpoint_number)
        except Exception as e:
            if e.__class__.__name__ == 'ConditionalCheckFailedException':
                if self.detect_duplicate(commit_id, event_stream.bucket_id, event_stream.stream_id):
                    raise Exception(
                        f"Commit {commit_id} already exists in stream {event_stream.stream_id}")
                else:
                    event_stream.__update__(self)
                    return self.commit(event_stream, commit_id)
            else:
                raise Exception(
                    f"Failed to commit event to stream {event_stream.stream_id} with error {e}")

    def detect_duplicate(self, commit_id: UUID, bucket_id: str, stream_id: str) -> bool:
        cur = self.connection.cursor()
        cur.execute(f"""SELECT COUNT(*)
  FROM {self._table_name}
 WHERE BucketId = %s
   AND StreamId = %s
   AND CommitId = %s;""", (bucket_id, stream_id, commit_id))
        return cur.fetchone() > 0


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
