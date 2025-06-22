import datetime
import typing
from uuid import UUID

from asyncpg import connect
from asyncpg.exceptions import UniqueViolationError
from pydantic_core import to_json, from_json

from aett.domain import (
    ConflictDetector,
    DuplicateCommitException,
    ConflictingCommitException,
    NonConflictingCommitException,
)
from aett.eventstore import (
    ICommitEventsAsync,
    COMMITS,
    TopicMap,
    MAX_INT,
    Commit,
    EventMessage, BaseEvent,
)
from aett.storage.asynchronous.postgresql import _item_to_commit


class AsyncCommitStore(ICommitEventsAsync):
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

    async def get(
            self,
            tenant_id: str,
            stream_id: str,
            min_revision: int = 0,
            max_revision: int = MAX_INT,
    ) -> typing.AsyncIterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        connection = await connect(self._connection_string)
        fetchall = await connection.fetch(
            f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
          FROM {self._table_name}
         WHERE TenantId = $1
           AND StreamId = $2
           AND StreamRevision >= $3
           AND (StreamRevision - Items) < $4
           AND CommitSequence > $5
         ORDER BY CommitSequence;""",
            tenant_id,
            stream_id,
            min_revision,
            max_revision,
            0,
        )
        for doc in fetchall:
            yield _item_to_commit(doc, self._topic_map)

    async def get_to(
            self,
            tenant_id: str,
            stream_id: str,
            max_time: datetime.datetime = datetime.datetime.max,
    ) -> typing.AsyncIterable[Commit]:
        connection = await connect(self._connection_string)
        fetchall = await connection.fetch(
            f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                  FROM {self._table_name}
                 WHERE TenantId = $1
                   AND StreamId = $2
                   AND CommitStamp <= $3
                 ORDER BY CommitSequence;""",
            tenant_id,
            stream_id,
            max_time,
        )
        for doc in fetchall:
            yield _item_to_commit(doc, self._topic_map)

    async def get_all_to(
            self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max
    ) -> typing.AsyncIterable[Commit]:
        connection = await connect(self._connection_string)
        fetchall = await connection.fetch(
            f"""SELECT TenantId, StreamId, StreamIdOriginal, StreamRevision, CommitId, CommitSequence, CommitStamp,  CheckpointNumber, Headers, Payload
                          FROM {self._table_name}
                         WHERE TenantId = %s
                           AND CommitStamp <= %s
                         ORDER BY CheckpointNumber;""",
            (tenant_id, max_time),
        )
        for doc in fetchall:
            yield _item_to_commit(doc, self._topic_map)

    async def commit(self, commit: Commit) -> Commit:
        try:
            connection = await connect(self._connection_string)
            json = to_json([e.to_json() for e in commit.events])
            fetchrow = await connection.fetchrow(
                f"""INSERT
              INTO {self._table_name}
                 ( TenantId, StreamId, StreamIdOriginal, CommitId, CommitSequence, StreamRevision, Items, CommitStamp, Headers, Payload )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING CheckpointNumber;""",
                commit.tenant_id,
                commit.stream_id,
                commit.stream_id,
                commit.commit_id,
                commit.commit_sequence,
                commit.stream_revision,
                len(commit.events),
                commit.commit_stamp,
                to_json(commit.headers),
                json,
            )
            checkpoint_number = fetchrow["checkpointnumber"]
            await connection.close()
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

        except UniqueViolationError:
            if await self._detect_duplicate(
                    commit.commit_id, commit.tenant_id, commit.stream_id
            ):
                raise DuplicateCommitException(
                    f"Commit {commit.commit_id} already exists in stream {commit.stream_id}"
                )
            else:
                conflicts, revision = await self._detect_conflicts(commit=commit)
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

    async def _detect_duplicate(
            self, commit_id: UUID, tenant_id: str, stream_id: str
    ) -> bool:
        try:
            connection = await connect(self._connection_string)
            result = await connection.fetch(
                f"""SELECT COUNT(*)
                  FROM {self._table_name}
                 WHERE TenantId = $1
                   AND StreamId = $2
                   AND CommitId = $3;""",
                tenant_id,
                stream_id,
                str(commit_id),
            )
            await connection.close()
            count = result[0][0]
            return count > 0
        except Exception as e:
            raise Exception(
                f"Failed to detect duplicate commit {commit_id} with error {e}"
            )

    async def _detect_conflicts(self, commit: Commit) -> typing.Tuple[bool, int]:
        connection = await connect(self._connection_string)
        fetchall = await connection.fetch(
            f"""SELECT StreamRevision, Payload
                                  FROM {self._table_name}
                                 WHERE TenantId = $1
                                   AND StreamId = $2
                                   AND StreamRevision <= $3
                                 ORDER BY CommitSequence;""",
            commit.tenant_id,
            commit.stream_id,
            commit.stream_revision,
        )
        latest_revision = 0
        for doc in fetchall:
            events = [
                EventMessage.from_dict(e, self._topic_map) for e in from_json(doc[1])
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
