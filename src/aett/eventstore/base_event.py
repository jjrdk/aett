import datetime
from abc import ABC
from typing import Optional

from pydantic import BaseModel, Field


class BaseEvent(ABC, BaseModel):
    """
    Represents a single event which has occurred outside the application domain.
    """

    source: str = Field(
        description="Gets the value which uniquely identifies the source of the event."
    )

    timestamp: datetime.datetime = Field(
        description="Gets the point in time at which the event was generated."
    )

    correlation_id: Optional[str] = Field(default=None, description="Gets the correlation id of the event.")