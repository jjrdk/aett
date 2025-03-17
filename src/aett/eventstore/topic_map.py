import inspect
from typing import Any, List, Self

from aett.eventstore.topic import Topic


class TopicMap:
    """
    Represents a map of topics to event classes.
    """

    def __init__(self):
        self.__topics = {}

    def add(self, topic: str, cls: type) -> Self:
        """
        Adds the topic and class to the map.
        :param topic: The topic of the event.
        :param cls: The class of the event.
        """
        self.__topics[topic] = cls
        return self

    def register(self, instance: Any) -> Self:
        t = instance if isinstance(instance, type) else type(instance)
        topic = Topic.get(t)
        if topic not in self.__topics:
            self.add(topic, t)

        return self

    def register_module(self, module: object) -> Self:
        """
        Registers all the classes in the module.
        """
        for c in inspect.getmembers(module, inspect.isclass):
            self.register(c[1])
        return self

    def get(self, topic: str) -> type | None:
        """
        Gets the class of the event given the topic.
        :param topic: The topic of the event.
        :return: The class of the event.
        """
        return self.__topics.get(topic, None)

    def get_all_types(self) -> List[type]:
        """
        Gets all the types in the map.
        :return: A list of all the types in the map.
        """
        return list(self.__topics.values())
