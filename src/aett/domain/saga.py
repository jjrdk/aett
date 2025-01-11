from abc import ABC
from typing import List, Dict, Any

from aett.domain.constants import T
from aett.eventstore import EventMessage, BaseEvent


class Saga(ABC):
    """
    A saga is a long-running process that coordinates multiple services to achieve a goal.
    The saga base class requires implementors to provide a method to apply events during state transition.

    In addition to this, the aggregate base class provides a method to raise events, but the concrete application
    of the event relies on multiple dispatch to call the correct apply method in the subclass.
    """

    def __init__(self, saga_id: str, commit_sequence: int):
        """
        Initialize the saga

        :param saga_id: The id of the saga
        :param commit_sequence: The commit sequence number which the saga was built from
        """
        self._id = saga_id
        self._commit_sequence = commit_sequence
        self._version = 0
        self.uncommitted: List[EventMessage] = []
        self._headers: Dict[str, Any] = {}

    @property
    def id(self) -> str:
        """
        Gets the id of the saga
        """
        return self._id

    @property
    def version(self) -> int:
        """
        Gets the version of the saga
        """
        return self._version

    @property
    def commit_sequence(self):
        """
        Gets the commit sequence number of the saga
        """
        return self._commit_sequence

    @property
    def headers(self):
        """
        Gets the metadata headers of the saga
        """
        return self._headers

    def transition(self, event: BaseEvent) -> None:
        """
        Transitions the saga to the next state based on the event
        :param event: The trigger event
        :return: None
        """
        # Use multiple dispatch to call the correct apply method
        self._apply(event)
        self.uncommitted.append(EventMessage(body=event, headers=self._headers))
        self._version += 1

    def dispatch(self, command: T) -> None:
        """
        Adds a command to the stream to be dispatched when the saga is committed
        :param command: The command to dispatch
        :return: None
        """
        index = len(self._headers)
        self._headers[f'UndispatchedMessage.{index}'] = command
