from typing import Iterable

from aett.eventstore import IManagePersistence, COMMITS, Commit, StreamHead
from aett.storage.synchronous.s3 import S3Config


class PersistenceManagement(IManagePersistence):
    def __init__(self, s3_config: S3Config, folder_name=COMMITS):
        self._folder_name = folder_name
        self._s3_bucket = s3_config.bucket
        self._resource = s3_config.to_client()

    def initialize(self):
        try:
            self._resource.create_bucket(Bucket=self._s3_bucket)
        except:
            pass

    def drop(self):
        self._resource.delete_bucket(Bucket=self._s3_bucket)

    def purge(self, tenant_id: str):
        response = self._resource.list_objects_v2(
            Bucket=self._s3_bucket, Prefix=f"{self._folder_name}/{tenant_id}/"
        )

        for o in response["Contents"]:
            self._resource.delete_object(Bucket=self._s3_bucket, Key=o["Key"])

    def get_from(self, checkpoint: int) -> Iterable[Commit]:
        return []

    def get_streams_to_snapshot(self, threshold: int) -> Iterable[StreamHead]:
        return []
