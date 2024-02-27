# import datetime
# from unittest import TestCase
# from unittest.mock import MagicMock
#
# from src.grpcserver.EventStore import EventStore
# from src.eventstore.EventStream import EventStream, DomainEvent
# from src.eventstore.eventstore_pb2 import CommitInfo
# from src.grpcserver.eventstore_pb2_grpc import EventStoreStub
#
#
# class TestEvent(DomainEvent):
#     def __init__(self, entity_id, version, timestamp: datetime, value: str):
#         super().__init__(entity_id, version, timestamp)
#         self.value = value
#
#
# class TestEventStore(TestCase):
#     def setUp(self):
#         self.eventstore = EventStore('http://localhost:8000')
#         grpc_store = MagicMock(spec=EventStoreStub)
#         mock_commit_info = MagicMock(spec=CommitInfo)
#         grpc_store.Commit.return_value = mock_commit_info
#         self.eventstore.__stub__ = grpc_store
#
#     async def test_commit(self):
#         stream = await EventStream.create('bucket', 'stream', self.eventstore)
#         stream.add_event(TestEvent('stream', 0, datetime.datetime.utcnow(), 'value'))
#         commit_info = self.eventstore.commit(stream)
#
#     def test_get(self):
#         self.fail()
#
#     def test_get_snapshot(self):
#         self.fail()
#
#     def test_add_snapshot(self):
#         self.fail()
