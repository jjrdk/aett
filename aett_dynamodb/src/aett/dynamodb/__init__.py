import datetime
import typing
from typing import Iterable
from uuid import UUID

import boto3
import jsonpickle
from boto3.dynamodb.conditions import Key, Attr

from aett.eventstore import ICommitEvents, EventStream, IAccessSnapshots, Snapshot, Commit, MAX_INT, EventMessage, \
    TopicMap


def _get_resource(region: str):
    return boto3.resource('dynamodb',
                          region_name=region,
                          endpoint_url='http://localhost:8000' if region == 'localhost' else None)


class CommitStore(ICommitEvents):
    def __init__(self, topic_map: TopicMap, table_name: str = 'commits', region: str = 'eu-central-1'):
        self.topic_map = topic_map
        self.table_name = table_name
        self.region = region
        self.dynamodb = _get_resource(region)
        self.table = self.dynamodb.Table(table_name)

    def get(self, bucket_id: str, stream_id: str, min_revision: int = 0,
            max_revision: int = MAX_INT) -> typing.Iterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        query_response = self.table.query(
            TableName=self.table_name,
            IndexName="RevisionIndex",
            ConsistentRead=True,
            ProjectionExpression='BucketId,StreamId,StreamRevision,CommitId,CommitSequence,CommitStamp,Headers,Events',
            KeyConditionExpression=(Key("BucketAndStream").eq(f'{bucket_id}{stream_id}')
                                    & Key("StreamRevision").between(min_revision, max_revision)),
            ScanIndexForward=True)
        items = query_response['Items']
        for item in items:
            yield self._item_to_commit(item)

    def get_to(self, bucket_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        query_response = self.table.scan(IndexName="CommitStampIndex",
                                         ConsistentRead=True,
                                         Select='ALL_ATTRIBUTES',
                                         FilterExpression=(
                                                 Key("BucketAndStream").eq(f'{bucket_id}{stream_id}')
                                                 & Attr('CommitStamp').lte(int(max_time.timestamp()))))
        items = query_response['Items']
        for item in items:
            if item['CommitStamp'] > max_time.timestamp():
                break
            yield self._item_to_commit(item)

    def _item_to_commit(self, item: dict) -> Commit:
        return Commit(
            bucket_id=item['BucketId'],
            stream_id=item['StreamId'],
            stream_revision=int(item['StreamRevision']),
            commit_id=UUID(item['CommitId']),
            commit_sequence=int(item['CommitSequence']),
            commit_stamp=datetime.datetime.fromtimestamp(int(item['CommitStamp']), datetime.UTC),
            headers=jsonpickle.decode(item['Headers']),
            events=[EventMessage.from_json(e, self.topic_map) for e in jsonpickle.decode(item['Events'])],
            checkpoint_token=0)

    def commit(self, event_stream: EventStream, commit_id: UUID):
        try:
            commit = event_stream.to_commit(commit_id)
            item = {
                'BucketAndStream': f'{event_stream.bucket_id}{event_stream.stream_id}',
                'BucketId': event_stream.bucket_id,
                'StreamId': event_stream.stream_id,
                'StreamRevision': commit.stream_revision,
                'CommitId': str(commit_id),
                'CommitSequence': commit.commit_sequence,
                'CommitStamp': int(commit.commit_stamp.timestamp()),
                'Headers': jsonpickle.encode(commit.headers, unpicklable=False),
                'Events': jsonpickle.encode([e.to_json() for e in commit.events], unpicklable=False)
            }
            response = self.table.put_item(
                TableName=self.table_name,
                Item=item,
                ReturnValues='NONE',
                ReturnValuesOnConditionCheckFailure='NONE',
                ConditionExpression='attribute_not_exists(BucketAndStream) AND attribute_not_exists(CommitSequence)')
            print(response)
        except Exception as e:
            if e.__class__.__name__ == 'ConditionalCheckFailedException':
                if self.detect_duplicate(commit_id, event_stream.bucket_id, event_stream.stream_id,
                                         event_stream.commit_sequence):
                    raise Exception(
                        f"Commit {commit_id} already exists in stream {event_stream.stream_id}")
                else:
                    event_stream.__update__(self)
            else:
                raise Exception(
                    f"Failed to commit event to stream {event_stream.stream_id} with status code {e.response['ResponseMetadata']['HTTPStatusCode']}")

    def detect_duplicate(self, commit_id: UUID, bucket_id: str, stream_id: str, commit_sequence: int) -> bool:
        duplicate_check = self.dynamodb.query(
            TableName=self.table_name,
            ConsistentRead=True,
            ScanIndexForward=False,
            Limit=1,
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression='CommitId',
            KeyConditionExpression="BucketAndStream = :v_BucketAndStream AND CommitSequence = :v_CommitSequence",
            ExpressionAttributeValues={
                {":v_BucketAndStream", {'S': f"{bucket_id}{stream_id}"}},
                {":v_CommitSequence", {'N': str(commit_sequence)}}
            })
        return duplicate_check.Items[0]['CommitId'].S == str(commit_id)


class SnapshotStore(IAccessSnapshots):
    def __init__(self, table_name: str = 'snapshots', region: str = 'eu-central-1'):
        self.dynamodb = _get_resource(region)
        self.table = self.dynamodb.Table(table_name)
        self.table_name = table_name

    def get(self, bucket_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        try:
            query_response = self.table.query(
                TableName=self.table_name,
                ConsistentRead=True,
                Limit=1,
                KeyConditionExpression=(
                        Key("BucketAndStream").eq(f'{bucket_id}{stream_id}') & Key("StreamRevision").lte(max_revision)),
                ScanIndexForward=False
            )
            if len(query_response['Items']) == 0:
                return None
            item = query_response['Items'][0]
            return Snapshot(bucket_id=item['BucketId'],
                            stream_id=item['StreamId'],
                            stream_revision=int(item['StreamRevision']),
                            payload=item['Payload'],
                            headers=dict(jsonpickle.decode(item['Headers'])))
        except Exception as e:
            raise Exception(
                f"Failed to get snapshot for stream {stream_id} with status code {e.response['ResponseMetadata']['HTTPStatusCode']}")

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] = None):
        if headers is None:
            headers = {}
        try:
            item = {
                'BucketAndStream': f"{snapshot.bucket_id}{snapshot.stream_id}",
                'BucketId': snapshot.bucket_id,
                'StreamId': snapshot.stream_id,
                'StreamRevision': snapshot.stream_revision,
                'Payload': snapshot.payload,
                'Headers': jsonpickle.encode(headers, unpicklable=False)
            }
            _ = self.table.put_item(
                TableName=self.table_name,
                Item=item,
                ReturnValues='NONE',
                ReturnValuesOnConditionCheckFailure='NONE',
                ConditionExpression='attribute_not_exists(BucketAndStream) AND attribute_not_exists(StreamRevision)'
            )
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with status code {e.response['ResponseMetadata']['HTTPStatusCode']}")


class PersistenceManagement:
    def __init__(self,
                 commits_table_name: str = 'commits',
                 snapshots_table_name: str = 'snapshots',
                 region: str = 'eu-central-1'):
        self.dynamodb = _get_resource(region)
        self.commits_table_name = commits_table_name
        self.snapshots_table_name = snapshots_table_name

    def initialize(self):
        tables = self.dynamodb.tables.all()
        table_names = [table.name for table in tables]
        if self.commits_table_name not in table_names:
            _ = self.dynamodb.create_table(
                TableName=self.commits_table_name,
                KeySchema=[
                    {'AttributeName': 'BucketAndStream', 'KeyType': 'HASH'},
                    {'AttributeName': 'CommitSequence', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'BucketAndStream', 'AttributeType': 'S'},
                    {'AttributeName': 'CommitSequence', 'AttributeType': 'N'},
                    {'AttributeName': 'StreamRevision', 'AttributeType': 'N'},
                    {'AttributeName': 'CommitStamp', 'AttributeType': 'N'}
                ],
                LocalSecondaryIndexes=[
                    {
                        'IndexName': "RevisionIndex",
                        'KeySchema': [
                            {'AttributeName': 'BucketAndStream', 'KeyType': 'HASH'},
                            {'AttributeName': 'StreamRevision', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'}
                    },
                    {
                        'IndexName': "CommitStampIndex",
                        'KeySchema': [
                            {'AttributeName': 'BucketAndStream', 'KeyType': 'HASH'},
                            {'AttributeName': 'CommitStamp', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'}
                    }],
                TableClass='STANDARD',
                StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'NEW_IMAGE'},
                ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10, })

        if self.snapshots_table_name not in table_names:
            _ = self.dynamodb.create_table(
                TableName=self.snapshots_table_name,
                KeySchema=[
                    {'AttributeName': 'BucketAndStream', 'KeyType': 'HASH'},
                    {'AttributeName': 'StreamRevision', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'BucketAndStream', 'AttributeType': 'S'},
                    {'AttributeName': 'StreamRevision', 'AttributeType': 'N'}
                ],
                TableClass='STANDARD',
                ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10, })

    def drop(self):
        tables = self.dynamodb.tables.all()
        for table in tables:
            if table.name in [self.commits_table_name, self.snapshots_table_name]:
                table.delete()
