import datetime
import typing
from typing import Iterable
from uuid import UUID

import boto3
import jsonpickle
from boto3 import client

from aett.eventstore import ICommitEvents, EventStream, IAccessSnapshots, Snapshot, Commit, MAX_INT, TopicMap, \
    EventMessage


class S3Config:
    def __init__(self, bucket: str, aws_access_key_id: str = None, aws_secret_access_key: str = None,
                 aws_session_token: str = None,
                 region: str = 'us-east-1', endpoint_url: str = None, use_tls: bool = True):
        self.aws_session_token = aws_session_token
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_access_key_id: str = aws_access_key_id
        self.use_tls = use_tls
        self.bucket = bucket
        self.region = region
        self.endpoint_url = endpoint_url

    def to_client(self):
        return boto3.client('s3',
                            aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key,
                            aws_session_token=self.aws_session_token,
                            region_name=self.region,
                            endpoint_url=self.endpoint_url,
                            verify=self.use_tls)


class CommitStore(ICommitEvents):
    def __init__(self, s3_config: S3Config, topic_map: TopicMap, folder_name='commits'):
        self._s3_bucket = s3_config.bucket
        self._topic_map = topic_map
        self._resource: client = s3_config.to_client()
        self._folder_name = folder_name

    def get(self, bucket_id: str, stream_id: str, min_revision: int = 0,
            max_revision: int = MAX_INT) -> typing.Iterable[Commit]:
        max_revision = MAX_INT if max_revision >= MAX_INT else max_revision + 1
        min_revision = 0 if min_revision < 0 else min_revision
        response = self._resource.list_objects(Bucket=self._s3_bucket,
                                               Delimiter='/',
                                               Prefix=f'{self._folder_name}/{bucket_id}/{stream_id}/')
        if 'Contents' not in response:
            return []
        keys = [key for key in map(lambda r: r.get('Key'), response.get('Contents')) if
                min_revision <= int(key.split('_')[-1].replace('.json', '')) <= max_revision]
        keys.sort()
        for key in keys:
            yield self._file_to_commit(key)

    def get_to(self, bucket_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        response = self._resource.list_objects(Bucket=self._s3_bucket,
                                               Delimiter='/',
                                               Prefix=f'{self._folder_name}/{bucket_id}/{stream_id}/')
        if 'Contents' not in response:
            return []
        timestamp = max_time.timestamp()
        keys = [key for key in map(lambda r: r.get('Key'), response.get('Contents')) if
                int(key.split('/')[-1].split('_')[0]) <= timestamp]
        keys.sort()
        for key in keys:
            yield self._file_to_commit(key)

    def _file_to_commit(self, key):
        file = self._resource.get_object(Bucket=self._s3_bucket, Key=key)
        doc = jsonpickle.decode(file.get('Body').read().decode('utf-8'))
        return Commit(bucket_id=doc.get('bucket_id'),
                      stream_id=doc.get('stream_id'),
                      stream_revision=doc.get('stream_revision'),
                      commit_id=doc.get('commit_id'),
                      commit_sequence=doc.get('commit_sequence'),
                      commit_stamp=doc.get('commit_stamp'),
                      headers=doc.get('headers'),
                      events=[EventMessage.from_json(e, self._topic_map) for e in doc.get('events')],
                      checkpoint_token=0)

    def commit(self, event_stream: EventStream, commit_id: UUID):
        commit = event_stream.to_commit(commit_id)
        commit_key = f'{self._folder_name}/{commit.bucket_id}/{commit.stream_id}/{int(commit.commit_stamp.timestamp())}_{commit.commit_sequence}_{commit.stream_revision}.json'
        if not self.check_exists(commit_key):
            d = commit.__dict__
            d['events'] = [e.to_json() for e in commit.events]
            d['headers'] = {k: jsonpickle.encode(v, unpicklable=False) for k, v in commit.headers.items()}
            body = jsonpickle.encode(d, unpicklable=False).encode('utf-8')
            self._resource.put_object(Bucket=self._s3_bucket,
                                      Key=commit_key,
                                      Body=body,
                                      ContentLength=len(body),
                                      Metadata={k: jsonpickle.encode(v, unpicklable=False) for k, v in
                                                commit.headers.items()})

    def check_exists(self, key: str):
        try:
            self._resource.get_object(
                Bucket=self._s3_bucket,
                Key=key)
            return True
        except self._resource.exceptions.NoSuchKey:
            return False


class SnapshotStore(IAccessSnapshots):
    def __init__(self, s3_config: S3Config, folder_name: str = 'snapshots'):
        self._s3_bucket = s3_config.bucket
        self._folder_name = folder_name
        self._resource: client = s3_config.to_client()

    def get(self, bucket_id: str, stream_id: str, max_revision: int = MAX_INT) -> Snapshot | None:
        files = self._resource.list_objects(Bucket=self._s3_bucket,
                                            Delimiter='/',
                                            Prefix=f'{self._folder_name}/{bucket_id}/{stream_id}/')
        if 'Contents' not in files:
            return None
        keys = list(
            int(key.split('/')[-1].replace('.json', '')) for key in map(lambda r: r.get('Key'), files.get('Contents'))
            if
            int(key.split('/')[-1].replace('.json', '')) <= max_revision)
        keys.sort(reverse=True)

        key = f'{self._folder_name}/{bucket_id}/{stream_id}/{keys[0]}.json'
        j = self._resource.get_object(Bucket=self._s3_bucket, Key=key)
        d = jsonpickle.decode(j['Body'].read())
        return Snapshot(bucket_id=d.get('bucket_id'),
                        stream_id=d.get('stream_id'),
                        stream_revision=int(d.get('stream_revision')),
                        payload=d.get('payload'),
                        headers=d.get('headers'))

    def add(self, snapshot: Snapshot, headers: typing.Dict[str, str] = None):
        if headers is None:
            headers = {}
        key = f'{self._folder_name}/{snapshot.bucket_id}/{snapshot.stream_id}/{snapshot.stream_revision}.json'
        self._resource.put_object(Bucket=self._s3_bucket, Key=key,
                                  Body=jsonpickle.encode(snapshot, unpicklable=False).encode('utf-8'))


class PersistenceManagement:
    def __init__(self, s3_config: S3Config):
        self._s3_bucket = s3_config.bucket
        self._resource: client = s3_config.to_client()

    def initialize(self):
        try:
            bucket = self._resource.create_bucket(Bucket=self._s3_bucket)
        except:
            pass

    def drop(self):
        self._resource.delete_bucket(Bucket=self._s3_bucket)
