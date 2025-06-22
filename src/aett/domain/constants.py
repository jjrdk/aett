from typing import TypeVar

from aett.eventstore import BaseEvent, Memento
from aett.eventstore.base_command import BaseCommand

TMemento = TypeVar("TMemento", bound=Memento)
TCommand = TypeVar("TCommand", bound=BaseCommand)

TUncommitted = TypeVar("TUncommitted", bound=BaseEvent)
TCommitted = TypeVar("TCommitted", bound=BaseEvent)

UNDISPATCHEDMESSAGES = "UndispatchedMessage"