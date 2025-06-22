import typing

from aett.eventstore import IAccessSnapshots, Snapshot, MAX_INT


class SnapshotStore(IAccessSnapshots):
    def __init__(self):
        self.buckets: typing.Dict[str, typing.Dict[str, typing.List[Snapshot]]] = {}

    def get(
            self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT
    ) -> Snapshot | None:
        if not self._ensure_stream(tenant_id=tenant_id, stream_id=stream_id):
            return None
        if len(self.buckets[tenant_id][stream_id]) == 0:
            return None
        snapshots = list(
            filter(
                lambda s: s.stream_revision <= max_revision,
                self.buckets[tenant_id][stream_id],
            )
        )
        return snapshots[-1]

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] | None = None):
        self._ensure_stream(snapshot.tenant_id, snapshot.stream_id)
        stream = self.buckets[snapshot.tenant_id][snapshot.stream_id]
        if len(stream) == 0:
            stream.append(snapshot)
        else:
            latest = stream[-1]
            if latest.stream_revision <= snapshot.stream_revision:
                raise ValueError("Conflicting commit")
            stream.append(snapshot)
            stream.sort(key=lambda s: s.stream_revision, reverse=False)

    def _ensure_stream(self, tenant_id: str, stream_id: str) -> bool:
        if tenant_id not in self.buckets:
            self.buckets[tenant_id] = {stream_id: []}
            return False
        if stream_id not in self.buckets[tenant_id]:
            self.buckets[tenant_id][stream_id] = list()
            return False
        return True
