from typing import Iterable

from aiobotocore.client import AioBaseClient
from boto3.dynamodb.conditions import Key

from aett.storage.asynchronous.dynamodb import _get_client
from aett.eventstore import Commit
from aett.eventstore.i_manage_persistence_async import IManagePersistenceAsync

from aett.eventstore.constants import COMMITS, SNAPSHOTS


class AsyncPersistenceManagement(IManagePersistenceAsync):
    def __init__(
            self,
            client: AioBaseClient,
            commits_table_name: str = COMMITS,
            snapshots_table_name: str = SNAPSHOTS,
    ):
        self.__dynamodb = client
        self.commits_table_name = commits_table_name
        self.snapshots_table_name = snapshots_table_name

    async def initialize(self):
        tables = await self.__dynamodb.Table.all()
        table_names = [await table.name for table in tables]
        if self.commits_table_name not in table_names:
            _ = await self.__dynamodb.create_table(
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
            _ = await self.__dynamodb.create_table(
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

    async def drop(self):
        tables = await self.__dynamodb.tables.all()
        for table in tables:
            if table.name in [self.commits_table_name, self.snapshots_table_name]:
                table.delete()

    async def purge(self, tenant_id: str):
        table = self.__dynamodb.Table(self.commits_table_name)
        query_response = await table.scan(
            IndexName="CommitStampIndex",
            ConsistentRead=True,
            Select="ALL_ATTRIBUTES",
            ProjectionExpression="Tenant,CommitSequence",
            FilterExpression=(Key("Tenant").eq(f"{tenant_id}")),
        )
        with table.batch_writer() as batch:
            for each in query_response["Items"]:
                await batch.delete_item(
                    Key={
                        "Tenant": each["Tenant"],
                        "CommitSequence": each["CommitSequence"],
                    }
                )

    async def get_from(self, checkpoint: int) -> Iterable[Commit]:
        raise NotImplementedError()
