from typing import Iterable

from boto3.dynamodb.conditions import Key

from aett.storage.synchronous.dynamodb import _get_resource
from aett.eventstore import Commit
from aett.eventstore.i_manage_persistence import IManagePersistence

from aett.eventstore.constants import COMMITS, SNAPSHOTS


class PersistenceManagement(IManagePersistence):
    def __init__(
            self,
            commits_table_name: str = COMMITS,
            snapshots_table_name: str = SNAPSHOTS,
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
        self.commits_table_name = commits_table_name
        self.snapshots_table_name = snapshots_table_name

    def initialize(self):
        tables = self.dynamodb.tables.all()
        table_names = [table.name for table in tables]
        if self.commits_table_name not in table_names:
            response = self.dynamodb.create_table(
                TableName=self.commits_table_name,
                KeySchema=[
                    {"AttributeName": "TenantAndStream", "KeyType": "HASH"},
                    {"AttributeName": "CommitSequence", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "TenantAndStream", "AttributeType": "S"},
                    {"AttributeName": "CommitSequence", "AttributeType": "N"},
                    {"AttributeName": "StreamRevision", "AttributeType": "N"},
                    {"AttributeName": "CommitStamp", "AttributeType": "N"},
                ],
                LocalSecondaryIndexes=[
                    {
                        "IndexName": "RevisionIndex",
                        "KeySchema": [
                            {"AttributeName": "TenantAndStream", "KeyType": "HASH"},
                            {"AttributeName": "StreamRevision", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                    {
                        "IndexName": "CommitStampIndex",
                        "KeySchema": [
                            {"AttributeName": "TenantAndStream", "KeyType": "HASH"},
                            {"AttributeName": "CommitStamp", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                ],
                TableClass="STANDARD",
                StreamSpecification={
                    "StreamEnabled": True,
                    "StreamViewType": "NEW_IMAGE",
                },
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10,
                },
            )

        if self.snapshots_table_name not in table_names:
            _ = self.dynamodb.create_table(
                TableName=self.snapshots_table_name,
                KeySchema=[
                    {"AttributeName": "TenantAndStream", "KeyType": "HASH"},
                    {"AttributeName": "StreamRevision", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "TenantAndStream", "AttributeType": "S"},
                    {"AttributeName": "StreamRevision", "AttributeType": "N"},
                ],
                TableClass="STANDARD",
                ProvisionedThroughput={
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10,
                },
            )

    def drop(self):
        tables = self.dynamodb.tables.all()
        for table in tables:
            if table.name in [self.commits_table_name, self.snapshots_table_name]:
                table.delete()

    def purge(self, tenant_id: str):
        table = self.dynamodb.Table(self.commits_table_name)
        query_response = table.scan(
            IndexName="CommitStampIndex",
            ConsistentRead=True,
            Select="ALL_ATTRIBUTES",
            ProjectionExpression="Tenant,CommitSequence",
            FilterExpression=(Key("Tenant").eq(f"{tenant_id}")),
        )
        with table.batch_writer() as batch:
            for each in query_response["Items"]:
                batch.delete_item(
                    Key={
                        "Tenant": each["Tenant"],
                        "CommitSequence": each["CommitSequence"],
                    }
                )

    def get_from(self, checkpoint: int) -> Iterable[Commit]:
        raise NotImplementedError()
