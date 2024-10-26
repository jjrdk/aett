from typing import Dict

from pydantic import BaseModel

from aett.eventstore.memento import Memento


class Snapshot(BaseModel):
    """
    Represents a materialized view of a stream at specific revision.
    """

    tenant_id: str
    """
    Gets the value which uniquely identifies the tenant to which the stream belongs.
    """

    stream_id: str
    """
    Gets the value which uniquely identifies the stream to which the snapshot applies.
    """

    stream_revision: int
    """
    Gets the position at which the snapshot applies.
    """

    commit_sequence: int
    """
    Gets the commit sequence at which the snapshot applies.
    """

    payload: str
    """
    Gets the snapshot or materialized view of the stream at the revision indicated.
    """

    headers: Dict[str, str]

    @staticmethod
    def from_memento(tenant_id: str, memento: Memento, commit_sequence: int, headers: Dict[str, str]) -> 'Snapshot':
        """
        Converts the memento to a snapshot which can be persisted.
        :param tenant_id: The value which uniquely identifies the bucket to which the stream belongs.
        :param memento:  The memento to be converted.
        :param commit_sequence: The commit sequence at which the snapshot applies.
        :param headers: The headers to assign to the snapshot
        :return:
        """
        return Snapshot(tenant_id=tenant_id, stream_id=memento.id, stream_revision=memento.version,
                        payload=memento.payload.model_dump_json(serialize_as_any=True), headers=headers,
                        commit_sequence=commit_sequence)
