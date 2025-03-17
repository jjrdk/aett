import boto3


def _get_resource(
    profile_name: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
    region: str = "eu-central-1",
    port: int = 8000,
):
    session = boto3.Session(
        profile_name=profile_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
    )
    return session.resource(
        "dynamodb",
        region_name=region,
        endpoint_url=f"http://localhost:{port}" if region == "localhost" else None,
    )
