from typing import AsyncIterable

from aett.eventstore import IManagePersistenceAsync, COMMITS, Commit
from aett.storage.asynchronous.s3 import AsyncS3Config


class AsyncPersistenceManagement(IManagePersistenceAsync):
    def __init__(self, s3_config: AsyncS3Config, folder_name=COMMITS):
        self._folder_name = folder_name
        self._s3_bucket = s3_config.bucket
        self.__config = s3_config

    async def initialize(self) -> None:
        try:
            async with self.__config.to_client() as client:
                await client.create_bucket(Bucket=self._s3_bucket)
        except:
            pass

    async def drop(self) -> None:
        async with self.__config.to_client() as client:
            await client.delete_bucket(Bucket=self._s3_bucket)

    async def purge(self, tenant_id: str) -> None:
        async with self.__config.to_client() as client:
            response = await client.list_objects_v2(
                Bucket=self._s3_bucket, Prefix=f"{self._folder_name}/{tenant_id}/"
            )

            for o in response["Contents"]:
                await client.delete_object(Bucket=self._s3_bucket, Key=o["Key"])

    async def get_from(self, checkpoint: int) -> AsyncIterable[Commit]:
        raise NotImplementedError()
