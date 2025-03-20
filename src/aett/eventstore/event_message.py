from typing import Dict, Any

from pydantic import BaseModel, Field

from aett.eventstore.topic import Topic
from aett.eventstore.topic_map import TopicMap


TOPIC_HEADER = "topic"


class EventMessage(BaseModel):
    """
    Represents a single event message within a commit.
    """

    body: Any = Field(description="Gets the body of the event message.")

    headers: Dict[str, Any] | None = Field(
        default_factory=dict,
        description="Gets the metadata which provides additional, "
        "unstructured information about this event message.",
    )

    def to_json(self) -> dict:
        """
        Converts the event message to a dictionary which can be serialized to JSON.
        """
        if self.headers is None:
            self.headers = {}
        if "topic" not in self.headers:
            self.headers[TOPIC_HEADER] = Topic.get(type(self.body))
        return self.model_dump(serialize_as_any=True)

    @staticmethod
    def from_dict(json_dict: dict, topic_map: TopicMap) -> "EventMessage":
        headers = (
            json_dict["headers"]
            if "headers" in json_dict and json_dict["headers"] is not None
            else {}
        )
        decoded_body = json_dict["body"]
        topic = (
            decoded_body.pop("$type", None) if isinstance(decoded_body, dict) else None
        )
        if topic is None and TOPIC_HEADER in headers:
            topic = headers[TOPIC_HEADER]
        if topic is None:
            return EventMessage(body=decoded_body, headers=headers)
        else:
            t = topic_map.get(topic=topic)
            body = (
                t.model_validate(decoded_body)
                if t is not None and issubclass(t, BaseModel)
                else decoded_body
            )
            return EventMessage(body=body, headers=headers)
