import datetime
from abc import ABC
from typing import Optional

from pydantic import BaseModel, Field


class BaseCommand(ABC, BaseModel):
    aggregate_id: str = Field(
        description="The id of the aggregate to apply the command to."
    )
    version: int = Field(description="The version of the aggregate to target")
    timestamp: datetime.datetime = Field(
        description="Gets the point in time at which the command was generated"
    )
    correlation_id: Optional[str] = Field(default=None, description="Gets the correlation id of the command.")
