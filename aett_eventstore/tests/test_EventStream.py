import datetime
import uuid
from typing import Iterable
from unittest import TestCase

from aett.eventstore import DomainEvent, EventStream, Commit, ICommitEvents, EventMessage, Topic, MAX_INT


@Topic('MyTestTopic')
class TestEvent(DomainEvent):
    pass


class TestEventStore(ICommitEvents):
    def get_to(self, tenant_id: str, stream_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        pass

    def get_all_to(self, tenant_id: str, max_time: datetime.datetime = datetime.datetime.max) -> \
            Iterable[Commit]:
        pass

    def commit(self, commit: Commit):
        pass

    def get(self, tenant_id: str, stream_id: str, min_revision: int = 0, max_revision: int = MAX_INT) -> Iterable[Commit]:
        return [
            Commit(tenant_id=tenant_id, stream_id=stream_id, stream_revision=1, commit_id=uuid.uuid4(),
                   commit_sequence=1, commit_stamp=datetime.datetime.now(), headers={},
                   events=[EventMessage(
                       body=TestEvent(source='test', timestamp=datetime.datetime.now(datetime.UTC), version=1))],
                   checkpoint_token=1)]


class TestEventStream(TestCase):
    def test_create_empty(self):
        stream = EventStream.create('bucket', 'stream')
        self.assertEqual(stream.tenant_id, 'bucket')
        self.assertEqual(stream.stream_id, 'stream')
        self.assertEqual(stream.version, 0)

    def test_create_from_store(self):
        store = TestEventStore()
        stream = EventStream.load('bucket', 'stream', store, 0)
        self.assertEqual(stream.tenant_id, 'bucket')
        self.assertEqual(stream.stream_id, 'stream')
        self.assertEqual(1, stream.version)

    def test_add_event(self):
        stream = EventStream.create('bucket', 'stream')
        stream.add(EventMessage(body=TestEvent(source='test', timestamp=datetime.datetime.now(), version=1)))
        self.assertEqual(stream.version, 1)

    def test_add_header(self):
        stream = EventStream.create('bucket', 'stream')
        stream.set_header('key', 'value')
        self.assertEqual(stream.uncommitted_headers['key'], 'value')
