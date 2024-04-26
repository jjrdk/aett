import datetime
import typing
from typing import Iterable

from aett.domain import ConflictingCommitException, NonConflictingCommitException, DuplicateCommitException, \
    ConflictDetector
from aett.eventstore import ICommitEvents, IAccessSnapshots, Snapshot, Commit, MAX_INT, EventMessage


class CommitStore(ICommitEvents):
    def __init__(self, conflict_detector: ConflictDetector = None):
        self._buckets: typing.Dict[str, typing.Dict[str, typing.List[Commit]]] = {}
        self._conflict_detector: ConflictDetector = \
            conflict_detector if conflict_detector is not None else ConflictDetector()

    def get(self, tenant_id: str, stream_id: str, min_revision: int = 0,
            max_revision: int = MAX_INT) -> typing.Iterable[Commit]:
        if not self._ensure_stream(tenant_id=tenant_id, stream_id=stream_id):
            return []
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision
        min_revision = 0 if min_revision < 0 else min_revision
        commits: typing.List[Commit] = self._buckets[tenant_id][stream_id]
        return (commit for commit in commits if min_revision <= commit.stream_revision <= max_revision)

    def get_to(self, tenant_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        if not self._ensure_stream(tenant_id=tenant_id, stream_id=stream_id):
            return []
        commits: typing.List[Commit] = self._buckets[tenant_id][stream_id]
        return (commit for commit in commits if commit.commit_stamp <= max_time)

    def get_all_to(self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        commits: typing.List[Commit] = []
        for bucket in self._buckets:
            for stream in self._buckets[bucket]:
                commits.extend(self.get_to(bucket, stream, max_time))
        commits.sort(key=lambda c: c.commit_stamp)
        return commits

    def commit(self, commit: Commit):
        self._ensure_stream(commit.tenant_id, commit.stream_id)
        existing = self._buckets[commit.tenant_id][commit.stream_id]
        if len(existing) > 0 and existing[-1].commit_sequence >= commit.commit_sequence:
            if existing[-1].commit_id == commit.commit_id:
                raise DuplicateCommitException('Duplicate commit')
            commits = [e for c in (c.events for c in existing if c.commit_sequence >= commit.commit_sequence) for e in
                       c]
            if self._conflict_detector.conflicts_with(list(map(self._get_body, commit.events)),
                                                      list(map(self._get_body, commits))):
                raise ConflictingCommitException('Conflicting commit')
            else:
                raise NonConflictingCommitException('Non-conflicting commit')
        existing.append(commit)

    @staticmethod
    def _get_body(em: EventMessage):
        return em.body

    def _ensure_stream(self, tenant_id: str, stream_id: str) -> bool:
        if tenant_id not in self._buckets:
            self._buckets[tenant_id] = {stream_id: list()}
            return False
        if stream_id not in self._buckets[tenant_id]:
            self._buckets[tenant_id] = {stream_id: list()}
            return False
        return True


class SnapshotStore(IAccessSnapshots):
    def __init__(self):
        self.buckets: typing.Dict[str, typing.Dict[str, typing.List[Snapshot]]] = {}

    def get(self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        if not self._ensure_stream(tenant_id=tenant_id, stream_id=stream_id):
            return None
        if len(self.buckets[tenant_id][stream_id]) == 0:
            return None
        snapshots = list(filter(lambda s: s.stream_revision <= max_revision, self.buckets[tenant_id][stream_id]))
        return snapshots[-1]

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] = None):
        self._ensure_stream(snapshot.tenant_id, snapshot.stream_id)
        stream = self.buckets[snapshot.tenant_id][snapshot.stream_id]
        if len(stream) == 0:
            stream.append(snapshot)
        else:
            latest = stream[-1]
            if latest.stream_revision <= snapshot.stream_revision:
                raise ValueError('Conflicting commit')
            stream.append(snapshot)
            stream.sort()

    def _ensure_stream(self, tenant_id: str, stream_id: str) -> bool:
        if tenant_id not in self.buckets:
            self.buckets[tenant_id] = {stream_id: []}
            return False
        if stream_id not in self.buckets[tenant_id]:
            self.buckets[tenant_id][stream_id] = list()
            return False
        return True
