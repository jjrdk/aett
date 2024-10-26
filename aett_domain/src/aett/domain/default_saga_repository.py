import datetime
import logging
import uuid
import typing

from aett.domain.saga_repository import SagaRepository, Saga
from aett.eventstore import ICommitEvents, Commit


class DefaultSagaRepository(SagaRepository):

    def __init__(self, tenant_id: str, store: ICommitEvents, logger: logging.Logger = None):
        """
        Initialize the default saga repository.

        :param tenant_id: The tenant id of the repository instance
        :param store: The event store to use
        """
        self._tenant_id = tenant_id
        self._store = store
        self._logger = logger if logger is not None else logging.getLogger(DefaultSagaRepository.__name__)

    def get(self, cls: typing.Type[SagaRepository.TSaga], stream_id: str) -> SagaRepository.TSaga:
        self._logger.debug(f'Getting saga {cls.__name__} with id {stream_id}')
        ait = self._store.get(self._tenant_id, stream_id)
        commits = [x for x in ait]
        commit_sequence = commits[-1].commit_sequence if len(commits) > 0 else 0
        saga = cls(stream_id, commit_sequence)
        for commit in commits:
            for event in commit.events:
                saga.transition(event.body)
        saga.uncommitted.clear()
        return saga

    def save(self, saga: Saga) -> None:
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
        self._store.commit(commit=commit)
        self._logger.debug(f'Saved saga {saga.id}')
        saga.uncommitted.clear()
        saga.headers.clear()
