import typing

from boto3.dynamodb.conditions import Key
from pydantic_core import from_json, to_json

from aett.storage.synchronous.dynamodb import _get_resource
from aett.eventstore import SNAPSHOTS, MAX_INT, Snapshot, IAccessSnapshots


class SnapshotStore(IAccessSnapshots):
    def __init__(
            self,
            table_name: str = SNAPSHOTS,
            region: str = "eu-central-1",
            profile_name: str | None = None,
            aws_access_key_id: str | None = None,
            aws_secret_access_key: str | None = None,
            aws_session_token: str | None = None,
            port: int = 8000,
    ):
        self.dynamodb = _get_resource(
            profile_name=profile_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region=region,
            port=port,
        )
        self.table = self.dynamodb.Table(table_name)
        self.table_name = table_name

    def get(
            self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT
    ) -> Snapshot | None:
        try:
            query_response = self.table.query(
                TableName=self.table_name,
                ConsistentRead=True,
                Limit=1,
                KeyConditionExpression=(
                        Key("TenantAndStream").eq(f"{tenant_id}{stream_id}")
                        & Key("StreamRevision").lte(max_revision)
                ),
                ScanIndexForward=False,
            )
            if len(query_response["Items"]) == 0:
                return None
            item = query_response["Items"][0]
            return Snapshot(
                tenant_id=item["TenantId"],
                stream_id=item["StreamId"],
                stream_revision=int(item["StreamRevision"]),
                payload=item["Payload"],
                commit_sequence=item["CommitSequence"],
                headers=dict(from_json(item["Headers"])),
            )
        except Exception as e:
            raise Exception(
                f"Failed to get snapshot for stream {stream_id} with status code {e}"
            )

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] | None = None):
        if headers is None:
            headers = {}
        try:
            item = {
                "TenantAndStream": f"{snapshot.tenant_id}{snapshot.stream_id}",
                "TenantId": snapshot.tenant_id,
                "StreamId": snapshot.stream_id,
                "StreamRevision": snapshot.stream_revision,
                "Payload": snapshot.payload,
                "CommitSequence": snapshot.commit_sequence,
                "Headers": to_json(headers).decode("utf-8"),
            }
            _ = self.table.put_item(
                TableName=self.table_name,
                Item=item,
                ReturnValues="NONE",
                ReturnValuesOnConditionCheckFailure="NONE",
                ConditionExpression="attribute_not_exists(TenantAndStream) AND attribute_not_exists(StreamRevision)",
            )
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with status code {e}"
            )
