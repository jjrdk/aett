import datetime
from unittest import TestCase

import jsonpickle.handlers

from aett.eventstore import EventMessage, TopicMap, Topic, DomainEvent


@Topic('MyTestTopic')
class TestEvent(DomainEvent):
    pass


class TestSerialization(TestCase):
    def test_serialize(self):
        msg = EventMessage(body=TestEvent(source='test', timestamp=datetime.datetime.now(), version=1))
        j = jsonpickle.encode(msg.to_json())
        print(j)
        self.assertTrue('$type' in j)

    def test_deserialize(self):
        tm = TopicMap()
        tm.register(TestEvent)
        j = '{"headers": "{\\"topic\\": \\"MyTestTopic\\"}", "body": "{\\"$type\\": \\"MyTestTopic\\", \\"source\\": \\"test\\", \\"timestamp\\": \\"2024-03-24T12:33:22.041039\\", \\"version\\": 1}"}'
        msg = EventMessage.from_json(jsonpickle.decode(j), tm)
        self.assertEqual(msg.body.source, 'test')
