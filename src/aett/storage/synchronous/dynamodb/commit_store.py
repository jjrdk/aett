import datetime
import typing
from typing import Iterable
from uuid import UUID

from boto3.dynamodb.conditions import Key, Attr
from pydantic_core import from_json, to_json

from aett.domain import (
    ConflictDetector,
    DuplicateCommitException,
    ConflictingCommitException,
    NonConflictingCommitException,
)
from aett.storage.synchronous.dynamodb import _get_resource
from aett.eventstore import TopicMap, COMMITS, MAX_INT, Commit, EventMessage, BaseEvent
from aett.eventstore.i_commit_events import ICommitEvents


class CommitStore(ICommitEvents):
    def __init__(
            self,
            topic_map: TopicMap,
            conflict_detector: ConflictDetector | None = None,
            table_name: str = COMMITS,
            region: str = "eu-central-1",
            profile_name: str | None = None,
            aws_access_key_id: str | None = None,
            aws_secret_access_key: str | None = None,
            aws_session_token: str | None = None,
            port: int = 8000,
    ):
        self._topic_map = topic_map
        self._table_name = table_name
        self._region = region
        self._dynamodb = _get_resource(
            profile_name=profile_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region=region,
            port=port,
        )
        self._table = self._dynamodb.Table(table_name)
        self._conflict_detector: ConflictDetector = (
            conflict_detector if conflict_detector is not None else ConflictDetector()
        )

    def get(
            self,
            tenant_id: str,
            stream_id: str,
            min_revision: int = 0,
            max_revision: int = MAX_INT,
    ) -> typing.Iterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        query_response = self._table.query(
            TableName=self._table_name,
            IndexName="RevisionIndex",
            ConsistentRead=True,
            ProjectionExpression="TenantId,StreamId,StreamRevision,CommitId,CommitSequence,CommitStamp,Headers,Events",
            KeyConditionExpression=(
                    Key("TenantAndStream").eq(f"{tenant_id}{stream_id}")
                    & Key("StreamRevision").between(min_revision, max_revision)
            ),
            ScanIndexForward=True,
        )
        items = query_response["Items"]
        for item in items:
            yield self._item_to_commit(item)

    def get_to(
            self,
            tenant_id: str,
            stream_id: str,
            max_time: datetime.datetime = datetime.datetime.max,
    ) -> Iterable[Commit]:
        query_response = self._table.scan(
            IndexName="CommitStampIndex",
            ConsistentRead=True,
            Select="ALL_ATTRIBUTES",
            FilterExpression=(
                    Key("TenantAndStream").eq(f"{tenant_id}{stream_id}")
                    & Attr("CommitStamp").lte(int(max_time.timestamp()))
            ),
        )
        items = query_response["Items"]
        for item in items:
            if item["CommitStamp"] > max_time.timestamp():
                break
            yield self._item_to_commit(item)

    def get_all_to(
            self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max
    ) -> Iterable[Commit]:
        query_response = self._table.scan(
            IndexName="CommitStampIndex",
            ConsistentRead=True,
            Select="ALL_ATTRIBUTES",
            ProjectionExpression="CommitStamp",
            FilterExpression=(
                    Key("TenantAndStream").begins_with(f"{tenant_id}")
                    & Attr("CommitStamp").lte(int(max_time.timestamp()))
            ),
        )
        items = query_response["Items"]
        for item in items:
            if item["CommitStamp"] > max_time.timestamp():
                break
            yield self._item_to_commit(item)

    def _item_to_commit(self, item: dict) -> Commit:
        return Commit(
            tenant_id=item["TenantId"],
            stream_id=item["StreamId"],
            stream_revision=int(item["StreamRevision"]),
            commit_id=UUID(item["CommitId"]),
            commit_sequence=int(item["CommitSequence"]),
            commit_stamp=datetime.datetime.fromtimestamp(
                int(item["CommitStamp"]), datetime.timezone.utc
            ),
            headers=from_json(bytes(item["Headers"])),
            events=[
                EventMessage.from_dict(e, self._topic_map)
                for e in from_json(bytes(item["Events"]))
            ],
            checkpoint_token=0,
        )

    def commit(self, commit: Commit):
        try:
            item = {
                "TenantAndStream": f"{commit.tenant_id}{commit.stream_id}",
                "TenantId": commit.tenant_id,
                "StreamId": commit.stream_id,
                "StreamRevision": commit.stream_revision,
                "CommitId": str(commit.commit_id),
                "CommitSequence": commit.commit_sequence,
                "CommitStamp": int(commit.commit_stamp.timestamp()),
                "Headers": to_json(commit.headers),
                "Events": to_json([e.to_json() for e in commit.events]),
            }
            _ = self._table.put_item(
                TableName=self._table_name,
                Item=item,
                ReturnValues="NONE",
                ReturnValuesOnConditionCheckFailure="NONE",
                ConditionExpression="attribute_not_exists(TenantAndStream) AND attribute_not_exists(CommitSequence)",
            )
        except Exception as e:
            if e.__class__.__name__ == "ConditionalCheckFailedException":
                if self._detect_duplicate(
                        commit.commit_id,
                        commit.tenant_id,
                        commit.stream_id,
                        commit.commit_sequence,
                ):
                    raise DuplicateCommitException("Duplicate commit detected")
                else:
                    self._raise_conflict(commit)
            else:
                raise e

    def _raise_conflict(self, commit: Commit):
        if self._detect_conflicts(commit=commit):
            raise ConflictingCommitException(
                f"Conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}"
            )
        else:
            raise NonConflictingCommitException(
                f"Non-conflicting version conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}"
            )

    def _detect_duplicate(
            self, commit_id: UUID, tenant_id: str, stream_id: str, commit_sequence: int
    ) -> bool:
        duplicate_check = self._table.query(
            TableName=self._table_name,
            ConsistentRead=True,
            ScanIndexForward=False,
            Limit=1,
            Select="SPECIFIC_ATTRIBUTES",
            ProjectionExpression="CommitId",
            KeyConditionExpression=(
                    Key("TenantAndStream").eq(f"{tenant_id}{stream_id}")
                    & Key("CommitSequence").eq(commit_sequence)
            ),
        )
        items = duplicate_check["Items"]
        return items[0]["CommitId"] == str(commit_id)

    def _detect_conflicts(self, commit: Commit) -> bool:
        if commit.commit_sequence == 0:
            return False
        previous_commits = self.get(
            commit.tenant_id,
            commit.stream_id,
            commit.commit_sequence - 1,
            commit.commit_sequence,
        )
        for previous_commit in previous_commits:
            if self._conflict_detector.conflicts_with(
                    list(map(self._get_body, commit.events)),
                    list(map(self._get_body, previous_commit.events)),
            ):
                return True
        return False

    @staticmethod
    def _get_body(em: EventMessage) -> BaseEvent:
        body: BaseEvent = em.body
        return body
