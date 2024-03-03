from unittest import TestCase

from aett.eventstore import Memento


class TestMemento(TestCase):
    def setUp(self):
        self.memento = Memento(id="test", version=1, payload="payload")


class TestCreateMemento(TestMemento):
    def test_read_id(self):
        self.assertEqual("test", self.memento.id)

    def test_read_version(self):
        self.assertEqual(1, self.memento.version)

    def test_read_payload(self):
        self.assertEqual("payload", self.memento.payload)
