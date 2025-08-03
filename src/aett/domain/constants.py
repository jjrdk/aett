from typing import TypeVar

from aett.eventstore import BaseEvent, Memento
from aett.eventstore.base_command import BaseCommand

TMemento = TypeVar("TMemento", bound=Memento, contravariant=True)
TCommand = TypeVar("TCommand", bound=BaseCommand, contravariant=True)
TEvent = TypeVar("TEvent", bound=BaseEvent, contravariant=True)

TUncommitted = TypeVar("TUncommitted", bound=BaseEvent)
TCommitted = TypeVar("TCommitted", bound=BaseEvent)

UNDISPATCHEDMESSAGES = "UndispatchedMessage"
