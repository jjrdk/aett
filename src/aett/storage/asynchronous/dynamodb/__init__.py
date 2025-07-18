from aioboto3 import Session
from botocore.client import BaseClient


def _get_client(
    session: Session,
    port: int = 8000,
) -> BaseClient:
    return session.resource(
        "dynamodb",
        use_ssl=False if session.region_name == "localhost" else True,
        endpoint_url=f"http://localhost:{port}"
        if session.region_name == "localhost"
        else None,
    )
