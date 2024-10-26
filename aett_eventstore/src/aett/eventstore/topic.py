class Topic(object):
    """
    Represents the topic of an event message.
    Should be used as a decorator on a class to indicate the topic of the event which will help with type deserialization.
    """

    def __init__(self, topic: str):
        self.topic = topic

    def __call__(self, cls):
        cls.__topic__ = self.topic
        return cls

    @staticmethod
    def get(cls: type) -> str:
        return cls.__topic__ if hasattr(cls, '__topic__') else cls.__name__
