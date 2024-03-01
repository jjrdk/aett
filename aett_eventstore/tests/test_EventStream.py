import datetime
import uuid
from typing import Iterable
from unittest import TestCase
from uuid import UUID

from aett.eventstore.EventStream import DomainEvent, EventStream, Commit, ICommitEvents, EventMessage


class TestEvent(DomainEvent):
    def __init__(self):
        super().__init__(id="a", timestamp=datetime.datetime.now(datetime.UTC), version=0)


class TestEventStore(ICommitEvents):
    def commit(self, event_stream: 'EventStream', commit_id: UUID):
        pass

    def get(self, bucket_id: str, stream_id: str, min_revision: int, max_revision: int) -> Iterable[Commit]:
        return [
            Commit(bucket_id=bucket_id, stream_id=stream_id, stream_revision=1, commit_id=uuid.uuid4(),
                   commit_sequence=1, commit_stamp=datetime.datetime.now(), headers={},
                   events=[EventMessage(body=TestEvent())], checkpoint_token=1)]


class TestEventStream(TestCase):
    def test_create_empty(self):
        stream = EventStream.create('bucket', 'stream')
        self.assertEqual(stream.bucket_id, 'bucket')
        self.assertEqual(stream.stream_id, 'stream')
        self.assertEqual(stream.version, 0)

    def test_create_from_store(self):
        store = TestEventStore()
        stream = EventStream.load('bucket', 'stream', store, 0)
        self.assertEqual(stream.bucket_id, 'bucket')
        self.assertEqual(stream.stream_id, 'stream')
        self.assertEqual(1, stream.version)

    def test_add_event(self):
        stream = EventStream.create('bucket', 'stream')
        stream.add(EventMessage(body=TestEvent()))
        self.assertEqual(stream.version, 1)

    def test_add_header(self):
        stream = EventStream.create('bucket', 'stream')
        stream.set_header('key', 'value')
        self.assertEqual(stream.uncommitted_headers['key'], 'value')
