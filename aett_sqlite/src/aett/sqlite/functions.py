from pydantic_core import from_json

from aett.eventstore import TopicMap, Commit, EventMessage


def _item_to_commit(item, topic_map: TopicMap):
    return Commit(tenant_id=item[0],
                  stream_id=item[1],
                  stream_revision=item[3],
                  commit_id=item[4],
                  commit_sequence=item[5],
                  commit_stamp=item[6],
                  headers=from_json(item[8]),
                  events=[EventMessage.from_json(e, topic_map) for e in from_json(item[9])],
                  checkpoint_token=item[7])
