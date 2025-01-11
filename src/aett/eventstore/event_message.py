from typing import Dict, Any

from pydantic import BaseModel
from pydantic_core import to_json, from_json

from aett.eventstore.topic import Topic
from aett.eventstore.topic_map import TopicMap
from aett.eventstore.base_event import BaseEvent

class EventMessage(BaseModel):
    """
    Represents a single event message within a commit.
    """

    body: object
    """
    Gets the body of the event message.
    """

    headers: Dict[str, Any] | None = None
    """
    Gets the metadata which provides additional, unstructured information about this event message.
    """

    def to_json(self) -> bytes:
        """
        Converts the event message to a dictionary which can be serialized to JSON.
        """
        if self.headers is None:
            self.headers = {}
        if 'topic' not in self.headers:
            self.headers['topic'] = Topic.get(type(self.body))
        return to_json(self)

    @staticmethod
    def from_json(j: bytes | str, topic_map: TopicMap) -> 'EventMessage':
        json_dict = from_json(j)
        headers = json_dict['headers'] if 'headers' in json_dict and json_dict['headers'] is not None else None
        decoded_body = json_dict['body']
        topic = decoded_body.pop('$type', None)
        if topic is None and headers is not None and 'topic' in headers:
            topic = headers['topic']
        if headers is not None and topic is None and 'topic' in headers:
            topic = headers['topic']
        if topic is None:
            return EventMessage(body=BaseEvent(**decoded_body), headers=headers)
        else:
            t = topic_map.get(topic=topic)
            body = t(**decoded_body)
            return EventMessage(body=body, headers=headers)
