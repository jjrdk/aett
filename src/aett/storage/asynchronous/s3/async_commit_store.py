import datetime
from typing import AsyncIterable

from pydantic_core import from_json, to_json

from aett.domain import (
    ConflictDetector,
    DuplicateCommitException,
    ConflictingCommitException,
    NonConflictingCommitException,
)
from aett.eventstore import (
    ICommitEventsAsync,
    TopicMap,
    COMMITS,
    MAX_INT,
    Commit,
    EventMessage,
    BaseEvent,
)
from aett.storage.asynchronous.s3 import AsyncS3Config


class AsyncCommitStore(ICommitEventsAsync):
    def __init__(
        self,
        s3_config: AsyncS3Config,
        topic_map: TopicMap,
        conflict_detector: ConflictDetector | None = None,
        folder_name=COMMITS,
    ):
        self.__s3_bucket: str = s3_config.bucket
        self.__topic_map: TopicMap = topic_map
        self.__config = s3_config
        self.__conflict_detector: ConflictDetector = (
            conflict_detector or ConflictDetector.empty()
        )
        self._folder_name = folder_name

    async def get(
        self,
        tenant_id: str,
        stream_id: str,
        min_revision: int = 0,
        max_revision: int = MAX_INT,
    ) -> AsyncIterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        async with self.__config.to_client() as client:
            response = await client.list_objects(
                Bucket=self.__s3_bucket,
                Delimiter="/",
                Prefix=f"{self._folder_name}/{tenant_id}/{stream_id}/",
            )
            if "Contents" not in response:
                return
            keys = [
                key
                for key in map(lambda r: r.get("Key"), response.get("Contents"))
                if min_revision
                <= int(key.split("_")[-1].replace(".json", ""))
                <= max_revision
            ]
            if not keys:
                return
            keys.sort()
            for key in keys:
                yield await self._file_to_commit(key)

    async def get_to(
        self,
        tenant_id: str,
        stream_id: str,
        max_time: datetime.datetime = datetime.datetime.max,
    ) -> AsyncIterable[Commit]:
        async with self.__config.to_client() as client:
            response = await client.list_objects(
                Bucket=self.__s3_bucket,
                Delimiter="/",
                Prefix=f"{self._folder_name}/{tenant_id}/{stream_id}/",
            )
            if "Contents" not in response:
                return
            timestamp = max_time.timestamp()
            keys = [
                key
                for key in map(lambda r: r.get("Key"), response.get("Contents"))
                if int(key.split("/")[-1].split("_")[0]) <= timestamp
            ]
            keys.sort()
            for key in keys:
                yield await self._file_to_commit(key)

    async def get_all_to(
        self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max
    ) -> AsyncIterable[Commit]:
        async with self.__config.to_client() as client:
            response = client.list_objects(
                Bucket=self.__s3_bucket,
                Delimiter="/",
                Prefix=f"{self._folder_name}/{tenant_id}/",
            )

            timestamp = max_time.timestamp()
            keys = [
                key
                for key in map(lambda r: r.get("Key"), response.get("Contents"))
                if int(key.split("/")[-1].split("_")[0]) <= timestamp
            ]
            keys.sort()
            for key in keys:
                yield await self._file_to_commit(key)

    async def _file_to_commit(self, key: str):
        async with self.__config.to_client() as client:
            file = await client.get_object(Bucket=self.__s3_bucket, Key=key)
            body = await file.get("Body").read()
            doc = from_json(body.decode())
            return Commit(
                tenant_id=doc.get("tenant_id"),
                stream_id=doc.get("stream_id"),
                stream_revision=doc.get("stream_revision"),
                commit_id=doc.get("commit_id"),
                commit_sequence=doc.get("commit_sequence"),
                commit_stamp=doc.get("commit_stamp"),
                headers=doc.get("headers"),
                events=[
                    EventMessage.from_dict(e, self.__topic_map)
                    for e in doc.get("events")
                ],
                checkpoint_token=0,
            )

    async def commit(self, commit: Commit):
        async with self.__config.to_client() as client:
            await self.check_exists(
                commit_sequence=commit.commit_sequence, commit=commit
            )
            commit_key = f"{self._folder_name}/{commit.tenant_id}/{commit.stream_id}/{int(commit.commit_stamp.timestamp())}_{commit.commit_id}_{commit.commit_sequence}_{commit.stream_revision}.json"
            d = commit.__dict__
            d["events"] = [e.to_json() for e in commit.events]
            d["headers"] = {k: to_json(v) for k, v in commit.headers.items()}
            body = to_json(d)
            await client.put_object(
                Bucket=self.__s3_bucket,
                Key=commit_key,
                Body=body,
                ContentLength=len(body),
                Metadata={k: to_json(v).decode() for k, v in commit.headers.items()},
            )

    async def check_exists(self, commit_sequence: int, commit: Commit) -> None:
        async with self.__config.to_client() as client:
            response = await client.list_objects(
                Delimiter="/",
                Bucket=self.__s3_bucket,
                Prefix=f"{self._folder_name}/{commit.tenant_id}/{commit.stream_id}/",
            )
            if "Contents" not in response:
                return
            keys = list(
                key for key in map(lambda r: r.get("Key"), response.get("Contents"))
            )
            keys.sort()
            for key in keys:
                split = key.split("_")
                if commit.commit_id == split[1]:
                    raise DuplicateCommitException(
                        f"Commit {commit.commit_id} already exists"
                    )
                if int(split[-2]) == commit_sequence or commit.stream_revision <= int(
                    split[-1].replace(".json", "")
                ):
                    overlapping = [
                        key
                        for key in keys
                        if int(key.split("_")[-1].replace(".json", ""))
                        >= commit.stream_revision
                    ]
                    if len(overlapping) == 0:
                        return
                    events = list(map(self._get_body, commit.events))
                    for o in overlapping:
                        c = await self._file_to_commit(o)
                        if self.__conflict_detector.conflicts_with(
                            events, list(map(self._get_body, c.events))
                        ):
                            raise ConflictingCommitException(
                                f"Commit {commit.commit_id} conflicts with {c.commit_id}"
                            )
                    raise NonConflictingCommitException(
                        f"Found non-conflicting commits at revision {commit.stream_revision}"
                    )

    @staticmethod
    def _get_body(em: EventMessage) -> BaseEvent:
        body: BaseEvent = em.body
        return body
