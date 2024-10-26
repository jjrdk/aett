from abc import ABC, abstractmethod
from typing import Optional, Dict

from aett.eventstore.snapshot import Snapshot


class IAccessSnapshotsAsync(ABC):
    """
    Indicates the ability to get and add snapshots.
    """

    @abstractmethod
    async def get(self, tenant_id: str, stream_id: str, max_revision: int) -> Optional[Snapshot]:
        """
        Gets the snapshot at the revision indicated or the most recent snapshot below that revision.

        :param tenant_id: The value which uniquely identifies the bucket to which the stream and the snapshot belong.
        :param stream_id: The stream for which the snapshot should be returned.
        :param max_revision: The maximum revision possible for the desired snapshot.
        :return: If found, returns the snapshot for the stream indicated; otherwise null.
        :raises StorageException:
        :raises StorageUnavailableException:
        """
        pass

    @abstractmethod
    async def add(self, snapshot: Snapshot, headers: Dict[str, str] = None):
        """
        Adds the snapshot provided to the stream indicated. Using a snapshotId of Guid.Empty will always persist the
        snapshot.

        :param snapshot: The snapshot to save.
        :param headers: The metadata to assign to the snapshot.
        :raises StorageException:
        :raises StorageUnavailableException:
        """
        pass
