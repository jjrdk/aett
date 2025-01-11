import datetime
from abc import ABC

from pydantic import BaseModel


class BaseEvent(ABC, BaseModel):
    """
    Represents a single event which has occurred.
    """

    source: str
    """
    Gets the value which uniquely identifies the source of the event.
    """

    timestamp: datetime.datetime
    """
    Gets the point in time at which the event was generated.
    """
