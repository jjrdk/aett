import datetime
import typing
import uuid
from typing import Iterable
from uuid import UUID

from aett.eventstore import ICommitEvents, EventStream, IAccessSnapshots, Snapshot, Commit, MAX_INT


class CommitStore(ICommitEvents):
    def __init__(self):
        self._buckets: typing.Dict[str, typing.Dict[str, typing.List[Commit]]] = {}

    def get(self, bucket_id: str, stream_id: str, min_revision: int = 0,
            max_revision: int = MAX_INT) -> typing.Iterable[Commit]:
        if not self._ensure_stream(bucket_id=bucket_id, stream_id=stream_id):
            return []
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision
        min_revision = 0 if min_revision < 0 else min_revision
        commits: typing.List[Commit] = self._buckets[bucket_id][stream_id]
        return (commit for commit in commits if min_revision <= commit.stream_revision <= max_revision)

    def get_to(self, bucket_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        if not self._ensure_stream(bucket_id=bucket_id, stream_id=stream_id):
            return []
        commits: typing.List[Commit] = self._buckets[bucket_id][stream_id]
        return (commit for commit in commits if commit.commit_stamp <= max_time)

    def commit(self, event_stream: EventStream, commit_id: UUID):
        self._ensure_stream(event_stream.bucket_id, event_stream.stream_id)
        existing = self._buckets[event_stream.bucket_id][event_stream.stream_id]
        if len(existing) > 0 and existing[-1].stream_revision >= event_stream.version:
            raise ValueError('Conflicting commit')

        existing.append(
            Commit(bucket_id=event_stream.bucket_id,
                   stream_id=event_stream.stream_id,
                   stream_revision=event_stream.version,
                   commit_id=uuid.uuid4(),
                   commit_sequence=len(existing) + 1,
                   commit_stamp=datetime.datetime.now(datetime.UTC),
                   headers=event_stream.uncommitted_headers,
                   events=event_stream.uncommitted,
                   checkpoint_token=len(existing)))

    def _ensure_stream(self, bucket_id: str, stream_id: str) -> bool:
        if bucket_id not in self._buckets:
            self._buckets[bucket_id] = {stream_id: list()}
            return False
        if stream_id not in self._buckets[bucket_id]:
            self._buckets[bucket_id] = {stream_id: list()}
            return False
        return True


class SnapshotStore(IAccessSnapshots):
    def __init__(self):
        self.buckets: typing.Dict[str, typing.Dict[str, typing.List[Snapshot]]] = {}

    def get(self, bucket_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        if not self._ensure_stream(bucket_id=bucket_id, stream_id=stream_id):
            return None
        if len(self.buckets[bucket_id][stream_id]) == 0:
            return None
        snapshots = list(filter(lambda s: s.stream_revision <= max_revision, self.buckets[bucket_id][stream_id]))
        return snapshots[-1]

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] = None):
        self._ensure_stream(snapshot.bucket_id, snapshot.stream_id)
        stream = self.buckets[snapshot.bucket_id][snapshot.stream_id]
        if len(stream) == 0:
            stream.append(snapshot)
        else:
            latest = stream[-1]
            if latest.stream_revision <= snapshot.stream_revision:
                raise ValueError('Conflicting commit')
            stream.append(snapshot)
            stream.sort()

    def _ensure_stream(self, bucket_id: str, stream_id: str) -> bool:
        if bucket_id not in self.buckets:
            self.buckets[bucket_id] = {stream_id: []}
            return False
        if stream_id not in self.buckets[bucket_id]:
            self.buckets[bucket_id][stream_id] = list()
            return False
        return True
