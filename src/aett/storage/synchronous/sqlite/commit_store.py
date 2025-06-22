import datetime
import sqlite3
import typing
from typing import Iterable
from uuid import UUID

from pydantic_core import to_json, from_json

from aett.domain import (
    ConflictDetector,
    DuplicateCommitException,
    ConflictingCommitException,
    NonConflictingCommitException,
)
from aett.eventstore import TopicMap, COMMITS, MAX_INT, Commit, EventMessage, BaseEvent
from aett.eventstore.i_commit_events import ICommitEvents
from aett.storage.synchronous.sqlite import _item_to_commit


class CommitStore(ICommitEvents):
    def __init__(
            self,
            connection_string: str,
            topic_map: TopicMap,
            conflict_detector: ConflictDetector | None = None,
            table_name=COMMITS,
    ):
        self._topic_map = topic_map
        self._connection_string = connection_string
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
        with sqlite3.connect(self._connection_string) as connection:
            cur: sqlite3.Cursor = connection.cursor()
            cur.execute(
                f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
          FROM {self._table_name}
         WHERE TenantId = ?
           AND StreamId = ?
           AND StreamRevision >= ?
           AND (StreamRevision - Items) < ?
           AND CommitSequence > ?
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
        with sqlite3.connect(self._connection_string) as connection:
            cur = connection.cursor()
            cur.execute(
                f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                      FROM {self._table_name}
                     WHERE TenantId = ?
                       AND StreamId = ?
                       AND CommitStamp <= ?
                     ORDER BY CommitSequence;""",
                (tenant_id, stream_id, max_time),
            )
            fetchall = cur.fetchall()
            for doc in fetchall:
                yield _item_to_commit(doc, self._topic_map)

    def get_all_to(
            self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max
    ) -> Iterable[Commit]:
        with sqlite3.connect(self._connection_string) as connection:
            cur = connection.cursor()
            cur.execute(
                f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                              FROM {self._table_name}
                             WHERE TenantId = ?
                              AND CommitStamp <= ?
                             ORDER BY CheckpointNumber;""",
                (tenant_id, max_time),
            )
            fetchall = cur.fetchall()
            for doc in fetchall:
                yield _item_to_commit(doc, self._topic_map)
            cur.close()

    def commit(self, commit: Commit):
        try:
            with sqlite3.connect(self._connection_string) as connection:
                cur: sqlite3.Cursor = connection.cursor()
                json = to_json([e.to_json() for e in commit.events])
                cur.execute(
                    f"""INSERT
              INTO {self._table_name}
                 ( TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp, Headers, Payload )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING CheckpointNumber;""",
                    (
                        commit.tenant_id,
                        commit.stream_id,
                        commit.stream_id,
                        commit.commit_id.bytes_le,
                        commit.commit_sequence,
                        commit.stream_revision,
                        len(commit.events),
                        commit.commit_stamp,
                        to_json(commit.headers),
                        json,
                    ),
                )
                checkpoint_number = cur.fetchone()
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
                    checkpoint_token=checkpoint_number[0],
                )

        except sqlite3.IntegrityError:
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
            with sqlite3.connect(self._connection_string) as connection:
                cur = connection.cursor()
                cur.execute(
                    f"""SELECT COUNT(*)
                  FROM {self._table_name}
                 WHERE TenantId = ?
                   AND StreamId = ?
                   AND CommitId = ?;""",
                    (tenant_id, stream_id, str(commit_id)),
                )
                result = cur.fetchone()
                cur.close()
                connection.commit()
                return result[0] > 0
        except Exception as e:
            raise Exception(
                f"Failed to detect duplicate commit {commit_id} with error {e}"
            )

    def _detect_conflicts(self, commit: Commit) -> typing.Tuple[bool, int]:
        with sqlite3.connect(self._connection_string) as connection:
            cur = connection.cursor()
            cur.execute(
                f"""SELECT StreamRevision, Payload
                                  FROM {self._table_name}
                                 WHERE TenantId = ?
                                   AND StreamId = ?
                                   AND StreamRevision <= ?
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
