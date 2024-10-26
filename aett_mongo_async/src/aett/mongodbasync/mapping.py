import datetime
from uuid import UUID

from pydantic_core import from_json

from aett.eventstore import TopicMap, Commit, EventMessage


def _doc_to_commit(doc: dict, topic_map: TopicMap) -> Commit:
    loads = from_json(doc['Events'])
    events_ = [EventMessage.from_json(e, topic_map) for e in loads]
    return Commit(tenant_id=doc['TenantId'],
                  stream_id=doc['StreamId'],
                  stream_revision=int(doc['StreamRevision']),
                  commit_id=UUID(doc['CommitId']),
                  commit_sequence=int(doc['CommitSequence']),
                  commit_stamp=datetime.datetime.fromtimestamp(int(doc['CommitStamp']), datetime.UTC),
                  headers=from_json(doc['Headers']),
                  events=events_,
                  checkpoint_token=doc['CheckpointToken'])
