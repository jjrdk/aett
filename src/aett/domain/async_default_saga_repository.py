import datetime
import logging
import typing
import uuid

from aett.domain import Saga, AsyncSagaRepository
from aett.domain.default_saga_repository import DefaultSagaRepository
from aett.eventstore import ICommitEventsAsync, Commit


class AsyncDefaultSagaRepository(AsyncSagaRepository):

    def __init__(self, tenant_id: str, store: ICommitEventsAsync, logger: logging.Logger = None):
        """
        Initialize the default saga repository.

        :param tenant_id: The tenant id of the repository instance
        :param store: The event store to use
        """
        self._tenant_id = tenant_id
        self._store = store
        self._logger = logger if logger is not None else logging.getLogger(DefaultSagaRepository.__name__)

    async def get(self, cls: typing.Type[AsyncSagaRepository.TSaga], stream_id: str) -> AsyncSagaRepository.TSaga:
        self._logger.debug(f'Getting saga {cls.__name__} with id {stream_id}')
        ait = self._store.get(self._tenant_id, stream_id)
        commits = [x async for x in ait]
        commit_sequence = commits[-1].commit_sequence if len(commits) > 0 else 0
        saga = cls(stream_id, commit_sequence)
        for commit in commits:
            for event in commit.events:
                saga.transition(event.body)
        saga.uncommitted.clear()
        return saga

    async def save(self, saga: Saga) -> None:
        self._logger.debug(f'Saving saga {saga.id} at version {saga.version}')
        commit = Commit(tenant_id=self._tenant_id,
                        stream_id=saga.id,
                        stream_revision=saga.version,
                        commit_id=uuid.uuid4(),
                        commit_sequence=saga.commit_sequence + 1,
                        commit_stamp=datetime.datetime.now(datetime.timezone.utc),
                        headers=dict(saga.headers),
                        events=list(saga.uncommitted),
                        checkpoint_token=0)
        await self._store.commit(commit=commit)
        self._logger.debug(f'Saved saga {saga.id}')
        saga.uncommitted.clear()
        saga.headers.clear()
