import inspect
import logging
from typing import List, Dict, Type, Callable, Iterable

from aett.domain.conflict_delegate import ConflictDelegate
from aett.eventstore import BaseEvent, DomainEvent


class ConflictDetector:
    @staticmethod
    def empty() -> 'ConflictDetector':
        return ConflictDetector()

    def __init__(self, delegates: List[ConflictDelegate] = None, logger: logging.Logger = None):
        """
        Initialize the conflict detector with the specified delegates.

        :param delegates: The delegates to use for conflict detection
        :param logger: The optional logger to use for logging.
        """
        self.delegates: Dict[
            Type, Dict[Type, Callable[[BaseEvent, BaseEvent], bool]]] = {}
        self._logger = logger if logger is not None and delegates is not None else logging.getLogger(
            ConflictDetector.__name__)
        if delegates is not None:
            for delegate in delegates:
                args = inspect.getfullargspec(delegate.detect)
                uncommitted_type = args.annotations[args.args[1]]
                committed_type = args.annotations[args.args[2]]
                if uncommitted_type not in self.delegates:
                    self.delegates[uncommitted_type] = {}
                self.delegates[uncommitted_type][committed_type] = delegate.detect

    def conflicts_with(self,
                       uncommitted_events: Iterable[BaseEvent],
                       committed_events: Iterable[BaseEvent]) -> bool:
        """
        Detects if the uncommitted events conflict with the committed events.

        :param uncommitted_events: The uncommitted events to analyze
        :param committed_events: The committed events to compare against.
        """
        if len(self.delegates) == 0:
            return False
        for uncommitted in uncommitted_events:
            for committed in committed_events:
                uncommitted_type = type(uncommitted)
                delegates_keys = self.delegates.keys()
                committed_type = type(committed)
                if uncommitted_type in delegates_keys:
                    committed_keys = self.delegates[uncommitted_type].keys()
                    if committed_type in committed_keys:
                        if self.delegates[uncommitted_type][committed_type](uncommitted, committed):
                            if isinstance(uncommitted, DomainEvent):
                                self._logger.warning(
                                    f'Detected conflict between uncommitted event {uncommitted_type.__name__} from {uncommitted.source} with version {uncommitted.version}')
                            else:
                                self._logger.warning(
                                    f'Detected conflict between uncommitted event {uncommitted_type.__name__} from {uncommitted.source} with timestamp {uncommitted.timestamp:%Y%m%d-%H%M%S%z}')
                            return True
        return False
