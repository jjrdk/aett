import typing
from abc import ABC

from pydantic import BaseModel

from aett.eventstore.constants import T


class Memento(ABC, BaseModel, typing.Generic[T]):
    id: str
    """
    Gets the id of the aggregate which generated the memento.
    """

    version: int
    """
    Gets the version of the aggregate which generated the memento.
    """

    payload: T
    """
    Gets the state of the aggregate at the time the memento was taken.
    """
