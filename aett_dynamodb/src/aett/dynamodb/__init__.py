import datetime
import typing
from typing import Iterable
from uuid import UUID

import boto3
import jsonpickle
from boto3.dynamodb.conditions import Key, Attr

from aett.domain import ConflictDetector, ConflictingCommitException, NonConflictingCommitException, \
    DuplicateCommitException
from aett.eventstore import ICommitEvents, IAccessSnapshots, Snapshot, Commit, MAX_INT, EventMessage, \
    TopicMap, COMMITS, SNAPSHOTS


def _get_resource(profile_name: str, region: str):
    session = boto3.Session(profile_name=profile_name)
    return session.resource('dynamodb',
                            region_name=region,
                            endpoint_url='http://localhost:8000' if region == 'localhost' else None)


class CommitStore(ICommitEvents):
    def __init__(self, topic_map: TopicMap, conflict_detector: ConflictDetector = None, table_name: str = COMMITS,
                 region: str = 'eu-central-1', profile_name: str = 'default'):
        self._topic_map = topic_map
        self._table_name = table_name
        self._region = region
        self._dynamodb = _get_resource(profile_name=profile_name, region=region)
        self.table = self._dynamodb.Table(table_name)
        self._conflict_detector: ConflictDetector = conflict_detector if conflict_detector is not None \
            else ConflictDetector()

    def get(self, tenant_id: str, stream_id: str, min_revision: int = 0,
            max_revision: int = MAX_INT) -> typing.Iterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        query_response = self.table.query(
            TableName=self._table_name,
            IndexName="RevisionIndex",
            ConsistentRead=True,
            ProjectionExpression='TenantId,StreamId,StreamRevision,CommitId,CommitSequence,CommitStamp,Headers,Events',
            KeyConditionExpression=(Key("TenantAndStream").eq(f'{tenant_id}{stream_id}')
                                    & Key("StreamRevision").between(min_revision, max_revision)),
            ScanIndexForward=True)
        items = query_response['Items']
        for item in items:
            yield self._item_to_commit(item)

    def get_to(self, tenant_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        query_response = self.table.scan(IndexName="CommitStampIndex",
                                         ConsistentRead=True,
                                         Select='ALL_ATTRIBUTES',
                                         FilterExpression=(
                                                 Key("TenantAndStream").eq(f'{tenant_id}{stream_id}')
                                                 & Attr('CommitStamp').lte(int(max_time.timestamp()))))
        items = query_response['Items']
        for item in items:
            if item['CommitStamp'] > max_time.timestamp():
                break
            yield self._item_to_commit(item)

    def get_all_to(self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        query_response = self.table.scan(IndexName="CommitStampIndex",
                                         ConsistentRead=True,
                                         Select='ALL_ATTRIBUTES',
                                         ProjectionExpression='CommitStamp',
                                         FilterExpression=(
                                                 Key("TenantAndStream").begins_with(f'{tenant_id}')
                                                 & Attr('CommitStamp').lte(int(max_time.timestamp()))))
        items = query_response['Items']
        for item in items:
            if item['CommitStamp'] > max_time.timestamp():
                break
            yield self._item_to_commit(item)

    def _item_to_commit(self, item: dict) -> Commit:
        return Commit(
            tenant_id=item['TenantId'],
            stream_id=item['StreamId'],
            stream_revision=int(item['StreamRevision']),
            commit_id=UUID(item['CommitId']),
            commit_sequence=int(item['CommitSequence']),
            commit_stamp=datetime.datetime.fromtimestamp(int(item['CommitStamp']), datetime.UTC),
            headers=jsonpickle.decode(item['Headers']),
            events=[EventMessage.from_json(e, self._topic_map) for e in jsonpickle.decode(item['Events'])],
            checkpoint_token=0)

    def commit(self, commit: Commit):
        try:
            item = {
                'TenantAndStream': f'{commit.tenant_id}{commit.stream_id}',
                'TenantId': commit.tenant_id,
                'StreamId': commit.stream_id,
                'StreamRevision': commit.stream_revision,
                'CommitId': str(commit.commit_id),
                'CommitSequence': commit.commit_sequence,
                'CommitStamp': int(commit.commit_stamp.timestamp()),
                'Headers': jsonpickle.encode(commit.headers, unpicklable=False),
                'Events': jsonpickle.encode([e.to_json() for e in commit.events], unpicklable=False)
            }
            response = self.table.put_item(
                TableName=self._table_name,
                Item=item,
                ReturnValues='NONE',
                ReturnValuesOnConditionCheckFailure='NONE',
                ConditionExpression='attribute_not_exists(TenantAndStream) AND attribute_not_exists(CommitSequence)')
            print(response)
        except Exception as e:
            if e.__class__.__name__ == 'ConditionalCheckFailedException':
                if self._detect_duplicate(commit.commit_id, commit.tenant_id, commit.stream_id,
                                          commit.commit_sequence):
                    raise DuplicateCommitException('Duplicate commit detected')
                else:
                    self._raise_conflict(commit)
            else:
                raise e

    def _raise_conflict(self, commit: Commit):
        if self._detect_conflicts(commit=commit):
            raise ConflictingCommitException(
                f"Conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}")
        else:
            raise NonConflictingCommitException(
                f'Non-conflicting version conflict detected in stream {commit.stream_id} with revision {commit.stream_revision}')

    def _detect_duplicate(self, commit_id: UUID, tenant_id: str, stream_id: str, commit_sequence: int) -> bool:
        duplicate_check = self.table.query(
            TableName=self._table_name,
            ConsistentRead=True,
            ScanIndexForward=False,
            Limit=1,
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression='CommitId',
            KeyConditionExpression=(Key("TenantAndStream").eq(f'{tenant_id}{stream_id}')
                                    & Key("CommitSequence").eq(commit_sequence)), )
        items = duplicate_check['Items']
        return items[0]['CommitId'] == str(commit_id)

    def _detect_conflicts(self, commit: Commit) -> bool:
        if commit.commit_sequence == 0:
            return False
        previous_commits = self.get(commit.tenant_id, commit.stream_id, commit.commit_sequence - 1,
                                    commit.commit_sequence)
        for previous_commit in previous_commits:
            if self._conflict_detector.conflicts_with(list(map(self._get_body, commit.events)),
                                                      list(map(self._get_body, previous_commit.events))):
                return True
        return False

    @staticmethod
    def _get_body(em: EventMessage):
        return em.body


class SnapshotStore(IAccessSnapshots):
    def __init__(self, table_name: str = SNAPSHOTS, region: str = 'eu-central-1', profile_name: str = 'default'):
        self.dynamodb = _get_resource(profile_name=profile_name, region=region)
        self.table = self.dynamodb.Table(table_name)
        self.table_name = table_name

    def get(self, tenant_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        try:
            query_response = self.table.query(
                TableName=self.table_name,
                ConsistentRead=True,
                Limit=1,
                KeyConditionExpression=(
                        Key("TenantAndStream").eq(f'{tenant_id}{stream_id}') & Key("StreamRevision").lte(max_revision)),
                ScanIndexForward=False
            )
            if len(query_response['Items']) == 0:
                return None
            item = query_response['Items'][0]
            return Snapshot(tenant_id=item['TenantId'],
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
                'TenantAndStream': f"{snapshot.tenant_id}{snapshot.stream_id}",
                'TenantId': snapshot.tenant_id,
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
                ConditionExpression='attribute_not_exists(TenantAndStream) AND attribute_not_exists(StreamRevision)'
            )
        except Exception as e:
            raise Exception(
                f"Failed to add snapshot for stream {snapshot.stream_id} with status code {e.response['ResponseMetadata']['HTTPStatusCode']}")


class PersistenceManagement:
    def __init__(self,
                 commits_table_name: str = COMMITS,
                 snapshots_table_name: str = SNAPSHOTS,
                 region: str = 'eu-central-1',
                 profile_name='default'):
        self.dynamodb = _get_resource(profile_name, region)
        self.commits_table_name = commits_table_name
        self.snapshots_table_name = snapshots_table_name

    def initialize(self):
        tables = self.dynamodb.tables.all()
        table_names = [table.name for table in tables]
        if self.commits_table_name not in table_names:
            _ = self.dynamodb.create_table(
                TableName=self.commits_table_name,
                KeySchema=[
                    {'AttributeName': 'TenantAndStream', 'KeyType': 'HASH'},
                    {'AttributeName': 'CommitSequence', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'TenantAndStream', 'AttributeType': 'S'},
                    {'AttributeName': 'CommitSequence', 'AttributeType': 'N'},
                    {'AttributeName': 'StreamRevision', 'AttributeType': 'N'},
                    {'AttributeName': 'CommitStamp', 'AttributeType': 'N'}
                ],
                LocalSecondaryIndexes=[
                    {
                        'IndexName': "RevisionIndex",
                        'KeySchema': [
                            {'AttributeName': 'TenantAndStream', 'KeyType': 'HASH'},
                            {'AttributeName': 'StreamRevision', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'}
                    },
                    {
                        'IndexName': "CommitStampIndex",
                        'KeySchema': [
                            {'AttributeName': 'TenantAndStream', 'KeyType': 'HASH'},
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
                    {'AttributeName': 'TenantAndStream', 'KeyType': 'HASH'},
                    {'AttributeName': 'StreamRevision', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'TenantAndStream', 'AttributeType': 'S'},
                    {'AttributeName': 'StreamRevision', 'AttributeType': 'N'}
                ],
                TableClass='STANDARD',
                ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10, })

    def drop(self):
        tables = self.dynamodb.tables.all()
        for table in tables:
            if table.name in [self.commits_table_name, self.snapshots_table_name]:
                table.delete()
