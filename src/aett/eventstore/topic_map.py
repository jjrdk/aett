import inspect
from typing import Any, Dict, List, Self

from aett.eventstore.topic import Topic


class TopicMap:
    """
    Represents a map of topics to event classes.
    """

    def __init__(self):
        self._topics: Dict[str, type] = {}

    def add(self, topic: str, cls: type) -> Self:
        """
        Adds the topic and class to the map.
        :param topic: The topic of the event.
        :param cls: The class of the event.
        """
        self._topics[topic] = cls
        return self

    def register(self, instance: Any) -> Self:
        t = instance if isinstance(instance, type) else type(instance)
        topic = Topic.get(t)
        if topic not in self._topics:
            self.add(topic, t)

        return self

    def register_module(self, module: object) -> Self:
        """
        Registers all the classes in the module.
        """
        for _, o in inspect.getmembers(module, inspect.isclass):
            if inspect.isclass(o):
                self.register(o)
            if inspect.ismodule(o):
                self.register_module(o)
        return self

    def get(self, topic: str) -> type | None:
        """
        Gets the class of the event given the topic.
        :param topic: The topic of the event.
        :return: The class of the event.
        """
        return self._topics.get(topic, None)

    def get_from_type(self, t: type) -> str | None:
        """
        Gets the topic of the event given the class.
        :param t: The class of the event.
        :return: The topic of the event.
        """
        for topic, cls in self._topics.items():
            if cls is t:
                return topic
        return None

    def get_all(self) -> List[str]:
        """
        Gets all the topics and their corresponding classes in the map.
        :return: A dictionary of all the topics and their classes.
        """
        return list(self._topics.keys())

    def get_all_types(self) -> List[type]:
        """
        Gets all the types in the map.
        :return: A list of all the types in the map.
        """
        return list(self._topics.values())
