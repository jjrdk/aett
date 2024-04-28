import inspect
from unittest import TestCase

import aett.eventstore
from aett.eventstore import Topic, TopicMap


@Topic('test')
class TestClass:
    pass


class TestsTopics(TestCase):
    def test_assign_topic(self):
        c = TestClass()
        self.assertEqual(c.__topic__, 'test')


class TestsTopicMap(TestCase):
    def test_topic_map(self):
        x = TopicMap()
        x.register(TestClass)
        m = list(inspect.getmembers(aett.eventstore, inspect.isclass))
        for c in m:
            x.register(c[1])

