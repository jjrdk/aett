import inspect
from typing import Any, List, Self, Dict

from aett.eventstore.topic import Topic


class TopicMap:
    """
    Represents a map of topics to event classes.
    """

    def __init__(self):
        self.__topics: Dict[str, type] = {}

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

    def get_all(self) -> List[str]:
        """
        Gets all the topics and their corresponding classes in the map.
        :return: A dictionary of all the topics and their classes.
        """
        return list(self.__topics.keys())

    def get_all_types(self) -> List[type]:
        """
        Gets all the types in the map.
        :return: A list of all the types in the map.
        """
        return list(self.__topics.values())


class HierarchicalTopicMap:
    """
    Represents a map of topics to event classes.
    """

    def __init__(self):
        self.__topics: Dict[str, type] = {}
        self.__excepted_bases__: List[type] = [object]

    def add(self, topic: str, cls: type) -> Self:
        """
        Adds the topic and class to the map.
        :param topic: The topic of the event.
        :param cls: The class of the event.
        """
        self.__topics[topic] = cls
        return self

    def except_base(self, t: type) -> None:
        """
        Exclude the base class from the topic hierarchy.
        :param t: The class to exclude.
        """
        if not isinstance(t, type):
            raise TypeError("Expected a class type")
        if t not in self.__excepted_bases__:
            self.__excepted_bases__.append(t)

    def register(self, instance: Any) -> Self:
        t = instance if isinstance(instance, type) else type(instance)
        topic = self._resolve_topic(t)
        if topic not in self.__topics:
            self.add(topic, t)

        return self

    def _resolve_topic(self, t: type) -> str:
        topic = t.__topic__ if hasattr(t, "__topic__") else t.__name__
        if t.__base__ and t.__base__ not in self.__excepted_bases__:
            topic = f"{self._resolve_topic(t.__base__)}.{topic}"
        return topic

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

    def get(self, instance: type) -> str | None:
        """
        Gets the topic of the event given the class.
        :param instance: The class of the event.
        :return: The topic of the event.
        """
        t = instance if isinstance(instance, type) else type(instance)
        return self._resolve_topic(t) if t in self.__topics.values() else None

    def get_all(self) -> List[str]:
        """
        Gets all the topics and their corresponding classes in the map.
        :return: A dictionary of all the topics and their classes.
        """
        return list(self.__topics.keys())
