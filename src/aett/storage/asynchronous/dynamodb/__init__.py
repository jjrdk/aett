from aiobotocore.client import AioBaseClient
from aiobotocore.session import get_session


def _get_client(
        profile_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        region: str = "eu-central-1",
        port: int = 8000,
) -> AioBaseClient:
    session = get_session()
    session.set_credentials(
        access_key=aws_access_key_id,
        secret_key=aws_secret_access_key,
        token=aws_session_token,
        account_id=profile_name)
    return session.create_client(
        "dynamodb",
        region_name=region,
        endpoint_url=f"http://localhost:{port}" if region == "localhost" else None,
    )
