import datetime
import typing
from typing import Iterable
from uuid import UUID

from pymysql import connect, IntegrityError
from pydantic_core import to_json, from_json

from aett.domain import (
    ConflictDetector,
    DuplicateCommitException,
    ConflictingCommitException,
    NonConflictingCommitException,
)
from aett.eventstore import (
    ICommitEvents,
    TopicMap,
    COMMITS,
    MAX_INT,
    Commit,
    EventMessage, BaseEvent,
)
from aett.storage.synchronous.mysql import _item_to_commit


class CommitStore(ICommitEvents):
    def __init__(
            self,
            host: str,
            user: str,
            password: str,
            database: str,
            topic_map: TopicMap,
            port: int = 3306,
            conflict_detector: ConflictDetector | None = None,
            table_name=COMMITS,
    ):
        self._topic_map = topic_map if topic_map else TopicMap()
        self._host: str = host
        self._user: str = user
        self._password: str = password
        self._database: str = database
        self._port: int = port
        self._conflict_detector = (
            conflict_detector if conflict_detector is not None else ConflictDetector()
        )
        self._table_name = table_name

    def get(
            self,
            tenant_id: str,
            stream_id: str,
            min_revision: int = 0,
            max_revision: int = MAX_INT,
    ) -> typing.Iterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        with connect(host=self._host,
                     user=self._user,
                     password=self._password,
                     database=self._database,
                     port=self._port,
                     autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(
                    f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
          FROM {self._table_name}
         WHERE TenantId = %s
           AND StreamId = %s
           AND StreamRevision >= %s
           AND (StreamRevision - Items) < %s
           AND CommitSequence > %s
         ORDER BY CommitSequence;""",
                    (tenant_id, stream_id, min_revision, max_revision, 0),
                )
                fetchall = cur.fetchall()
                for doc in fetchall:
                    yield _item_to_commit(doc, self._topic_map)

    def get_to(
            self,
            tenant_id: str,
            stream_id: str,
            max_time: datetime.datetime = datetime.datetime.max,
    ) -> Iterable[Commit]:
        with connect(host=self._host,
                     user=self._user,
                     password=self._password,
                     database=self._database,
                     port=self._port,
                     autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(
                    f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                  FROM {self._table_name}
                 WHERE TenantId = %s
                   AND StreamId = %s
                   AND CommitStamp <= %s
                 ORDER BY CommitSequence;""",
                    (tenant_id, stream_id, max_time),
                )
                fetchall = cur.fetchall()
                for doc in fetchall:
                    yield _item_to_commit(doc, self._topic_map)

    def get_all_to(
            self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max
    ) -> Iterable[Commit]:
        with connect(host=self._host,
                     user=self._user,
                     password=self._password,
                     database=self._database,
                     port=self._port,
                     autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(
                    f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                          FROM {self._table_name}
                         WHERE TenantId = %s
                           AND CommitStamp <= %s
                         ORDER BY CheckpointNumber;""",
                    (tenant_id, max_time),
                )
                fetchall = cur.fetchall()
                for doc in fetchall:
                    yield _item_to_commit(doc, self._topic_map)

    def commit(self, commit: Commit):
        try:
            with connect(host=self._host,
                         user=self._user,
                         password=self._password,
                         database=self._database,
                         port=self._port,
                         autocommit=True) as connection:
                with connection.cursor() as cur:
                    json = to_json([e.to_json() for e in commit.events])
                    cur.execute(
                        f"""INSERT
          INTO {self._table_name}
             ( TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp, Headers, Payload )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                        (
                            commit.tenant_id,
                            commit.stream_id,
                            commit.stream_id,
                            commit.commit_id.bytes,
                            commit.commit_sequence,
                            commit.stream_revision,
                            len(commit.events),
                            commit.commit_stamp,
                            to_json(commit.headers),
                            json,
                        ),
                    )
                    checkpoint_number = cur.lastrowid
                    cur.close()
                    connection.commit()
                    return Commit(
                        tenant_id=commit.tenant_id,
                        stream_id=commit.stream_id,
                        stream_revision=commit.stream_revision,
                        commit_id=commit.commit_id,
                        commit_sequence=commit.commit_sequence,
                        commit_stamp=commit.commit_stamp,
                        headers=commit.headers,
                        events=commit.events,
                        checkpoint_token=checkpoint_number,
                    )
        except IntegrityError:
            if self._detect_duplicate(
                    commit.commit_id, commit.tenant_id, commit.stream_id
            ):
                raise DuplicateCommitException(
                    f"Commit {commit.commit_id} already exists in stream {commit.stream_id}"
                )
            else:
                conflicts, revision = self._detect_conflicts(commit=commit)
                if conflicts:
                    raise ConflictingCommitException(
                        f"Conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}"
                    )
                else:
                    raise NonConflictingCommitException(
                        f"Non-conflicting version conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}"
                    )
        except Exception as e:
            raise Exception(f"Failed to commit {commit.commit_id} with error {e}")

    def _detect_duplicate(
            self, commit_id: UUID, tenant_id: str, stream_id: str
    ) -> bool:
        try:
            with connect(host=self._host,
                         user=self._user,
                         password=self._password,
                         database=self._database,
                         port=self._port,
                         autocommit=True) as connection:
                with connection.cursor() as cur:
                    cur.execute(
                        f"""SELECT COUNT(*)
          FROM {self._table_name}
         WHERE TenantId = %s
           AND StreamId = %s
           AND CommitId = %s;""",
                        (tenant_id, stream_id, str(commit_id)),
                    )
                    result = cur.fetchone()
                    cur.close()
                    return result[0] > 0
        except Exception as e:
            raise Exception(
                f"Failed to detect duplicate commit {commit_id} with error {e}"
            )

    def _detect_conflicts(self, commit: Commit) -> typing.Tuple[bool, int]:
        with connect(host=self._host,
                     user=self._user,
                     password=self._password,
                     database=self._database,
                     port=self._port,
                     autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute(
                    f"""SELECT StreamRevision, Payload
                          FROM {self._table_name}
                         WHERE TenantId = %s
                           AND StreamId = %s
                           AND StreamRevision <= %s
                         ORDER BY CommitSequence;""",
                    (commit.tenant_id, commit.stream_id, commit.stream_revision),
                )
                fetchall = cur.fetchall()
                latest_revision = 0
                for doc in fetchall:
                    events = [
                        EventMessage.from_dict(e, self._topic_map)
                        for e in from_json(doc[1])
                    ]
                    uncommitted_events = list(map(self._get_body, commit.events))
                    committed_events = list(map(self._get_body, events))
                    if self._conflict_detector.conflicts_with(
                            uncommitted_events, committed_events
                    ):
                        return True, -1
                    if doc[0] > latest_revision:
                        latest_revision = int(doc[0])
                return False, latest_revision

    @staticmethod
    def _get_body(e: EventMessage) -> BaseEvent:
        return e.body
