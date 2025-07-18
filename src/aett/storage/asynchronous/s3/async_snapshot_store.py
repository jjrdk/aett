from typing import Dict

from pydantic_core import from_json, to_json

from aett.eventstore import IAccessSnapshotsAsync, SNAPSHOTS, Snapshot, MAX_INT
from aett.storage.asynchronous.s3 import AsyncS3Config


class AsyncSnapshotStore(IAccessSnapshotsAsync):
    def __init__(self, s3_config: AsyncS3Config, folder_name: str = SNAPSHOTS):
        self.__s3_bucket = s3_config.bucket
        self.__folder_name = folder_name
        self.__config = s3_config

    async def get(
        self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT
    ) -> Snapshot | None:
        async with self.__config.to_client() as client:
            files = await client.list_objects(
                Bucket=self.__s3_bucket,
                Delimiter="/",
                Prefix=f"{self.__folder_name}/{tenant_id}/{stream_id}/",
            )
            if "Contents" not in files:
                return None
            keys = list(
                int(key.split("/")[-1].replace(".json", ""))
                for key in map(lambda r: r.get("Key"), files.get("Contents"))
                if int(key.split("/")[-1].replace(".json", "")) <= max_revision
            )
            keys.sort(reverse=True)

            key = f"{self.__folder_name}/{tenant_id}/{stream_id}/{keys[0]}.json"
            j = await client.get_object(Bucket=self.__s3_bucket, Key=key)
            body = await j["Body"].read()
            d = from_json(data=body)
            return Snapshot(
                tenant_id=d.get("tenant_id"),
                stream_id=d.get("stream_id"),
                stream_revision=int(d.get("stream_revision")),
                commit_sequence=int(d.get("commit_sequence")),
                payload=d.get("payload"),
                headers=d.get("headers"),
            )

    async def add(
        self, snapshot: Snapshot, headers: Dict[str, str] | None = None
    ) -> None:
        async with self.__config.to_client() as client:
            if headers is not None:
                snapshot.headers.update(headers)
            key = f"{self.__folder_name}/{snapshot.tenant_id}/{snapshot.stream_id}/{snapshot.stream_revision}.json"
            await client.put_object(
                Bucket=self.__s3_bucket, Key=key, Body=to_json(snapshot)
            )
