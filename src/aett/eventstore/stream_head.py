from pydantic import BaseModel


class StreamHead(BaseModel):
    tenant_id: str
    stream_id: str
    head_revision: int
    snapshot_revision: int
