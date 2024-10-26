from abc import ABC, abstractmethod
from typing import Iterable

from aett.eventstore import Commit


class IManagePersistence(ABC):
    @abstractmethod
    def initialize(self):
        """
        Initializes the persistence mechanism.
        """
        pass

    @abstractmethod
    def drop(self):
        """
        Drops the persistence mechanism.
        """
        pass

    @abstractmethod
    def purge(self, tenant_id: str):
        """
        Purges the persistence mechanism.

        :param tenant_id: The value which uniquely identifies the tenant to be purged.
        """
        pass

    @abstractmethod
    def get_from(self, checkpoint: int) -> Iterable[Commit]:
        """
        Gets the commits from the checkpoint.
        :param checkpoint: The checkpoint to start from.
        :return: The commits from the checkpoint.
        """
        pass
