from abc import ABC, abstractmethod
from typing import TypeVar, Type

from aett.domain.saga import Saga


class SagaRepository(ABC):
    """
    Defines the abstract interface for an saga repository.
    The repository is responsible for loading and saving sagas to the event store,
    typically using the ICommitEvents interface.
    """
    TSaga = TypeVar('TSaga', bound=Saga)

    @abstractmethod
    def get(self, cls: Type[TSaga], stream_id: str) -> TSaga:
        """
        Gets the saga with the specified stream id and type at the latest version.
        """
        pass

    @abstractmethod
    def save(self, saga: Saga) -> None:
        """
        Save the saga to the repository.
        """
        pass
