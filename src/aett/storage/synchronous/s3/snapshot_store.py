from typing import Dict

from pydantic_core import from_json, to_json

from aett.eventstore import IAccessSnapshots, SNAPSHOTS, Snapshot, MAX_INT
from aett.storage.synchronous.s3 import S3Config


class SnapshotStore(IAccessSnapshots):
    def __init__(self, s3_config: S3Config, folder_name: str = SNAPSHOTS):
        self._s3_bucket = s3_config.bucket
        self._folder_name = folder_name
        self._resource = s3_config.to_client()

    def get(
            self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT
    ) -> Snapshot | None:
        files = self._resource.list_objects(
            Bucket=self._s3_bucket,
            Delimiter="/",
            Prefix=f"{self._folder_name}/{tenant_id}/{stream_id}/",
        )
        if "Contents" not in files:
            return None
        keys = list(
            int(key.split("/")[-1].replace(".json", ""))
            for key in map(lambda r: r.get("Key"), files.get("Contents"))
            if int(key.split("/")[-1].replace(".json", "")) <= max_revision
        )
        keys.sort(reverse=True)

        key = f"{self._folder_name}/{tenant_id}/{stream_id}/{keys[0]}.json"
        j = self._resource.get_object(Bucket=self._s3_bucket, Key=key)
        d = from_json(j["Body"].read())
        return Snapshot(
            tenant_id=d.get("tenant_id"),
            stream_id=d.get("stream_id"),
            stream_revision=int(d.get("stream_revision")),
            commit_sequence=int(d.get("commit_sequence")),
            payload=d.get("payload"),
            headers=d.get("headers"),
        )

    def add(self, snapshot: Snapshot, headers: Dict[str, str] | None = None):
        if headers is not None:
            snapshot.headers.update(headers)
        key = f"{self._folder_name}/{snapshot.tenant_id}/{snapshot.stream_id}/{snapshot.stream_revision}.json"
        self._resource.put_object(
            Bucket=self._s3_bucket, Key=key, Body=to_json(snapshot)
        )
