import datetime
from typing import Dict, List, Annotated
from uuid import UUID

from pydantic import BaseModel, Field, PlainSerializer, BeforeValidator

from aett.eventstore.event_message import EventMessage


def _convert_event_list(event_list: List[EventMessage]) -> List[dict]:
    return [x.to_json() for x in event_list]


class Commit(BaseModel):
    """
    Represents a series of events which have been fully committed as a single unit
    and which apply to the stream indicated.
    """

    tenant_id: str
    """
    Gets or sets the value which identifies tenant to which the stream and the commit belongs.
    """

    stream_id: str
    """
    Gets the value which uniquely identifies the stream to which the commit belongs.
    """

    stream_revision: int
    """
    Gets the value which indicates the revision of the most recent event in the stream to which this commit applies.
    """

    commit_id: UUID
    """
    Gets the value which uniquely identifies the commit within the stream.
    """

    commit_sequence: int
    """
    Gets the value which indicates the sequence (or position) in the stream to which this commit applies.
    """

    commit_stamp: datetime.datetime
    """
    Gets the point in time at which the commit was persisted.
    """

    headers: Dict[str, object]
    """
    Gets the metadata which provides additional, unstructured information about this commit.
    """

    events: Annotated[
        List[EventMessage],
        PlainSerializer(
            func=_convert_event_list, return_type=List[dict], when_used="json"
        ),
    ] = Field(default_factory=list)
    """
    Gets the collection of event messages to be committed as a single unit.
    """

    checkpoint_token: int
    """
    The checkpoint that represents the storage level order.
    """
