from typing import TypeVar

from aett.eventstore import (BaseEvent, Memento)

T = TypeVar('T', bound=Memento)

TUncommitted = TypeVar('TUncommitted', bound=BaseEvent)
TCommitted = TypeVar('TCommitted', bound=BaseEvent)
