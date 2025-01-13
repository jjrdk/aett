import datetime
from abc import ABC

from pydantic import BaseModel, Field


class BaseEvent(ABC, BaseModel):
    """
    Represents a single event which has occurred.
    """

    source: str = Field(description="Gets the value which uniquely identifies the source of the event.")

    timestamp: datetime.datetime = Field(description="Gets the point in time at which the event was generated.")
