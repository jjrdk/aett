from unittest import TestCase

from aett.eventstore.EventStream import Memento


class TestMemento(TestCase):
    def setUp(self):
        self.memento = Memento("test", 1, "payload")


class TestCreateMemento(TestMemento):
    def test_read_id(self):
        self.assertEqual("test", self.memento.id)

    def test_read_version(self):
        self.assertEqual(1, self.memento.version)

    def test_read_payload(self):
        self.assertEqual("payload", self.memento.payload)

    def test_create_snapshot(self):
        snapshot = self.memento.__to_snapshot__("bucket")
        self.assertEqual("bucket", snapshot.bucket_id)
        self.assertEqual("InBheWxvYWQi", snapshot.payload)
