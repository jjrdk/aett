import boto3
import botocore.client
from botocore.client import BaseClient
import inspect


class S3Config:
    def __init__(
            self,
            bucket: str,
            aws_access_key_id: str | None = None,
            aws_secret_access_key: str | None = None,
            aws_session_token: str | None = None,
            profile_name: str | None = None,
            region: str = "us-east-1",
            endpoint_url: str | None = None,
            use_tls: bool = True,
    ):
        """
        Defines the configuration for the S3 client.
        If a profile name is provided, the access key id and secret access are disregarded and the profile credentials
        are used.

        :param bucket: The name of the bucket
        :param aws_access_key_id: The AWS access key id
        :param aws_secret_access_key: The AWS secret access key
        :param aws_session_token: The AWS session token
        :param region: The AWS region
        :param endpoint_url: The endpoint URL
        :param use_tls: Whether to use TLS
        :param profile_name: The profile name
        """
        self._aws_session_token = aws_session_token
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_access_key_id = aws_access_key_id
        self._use_tls = use_tls
        self.bucket = bucket
        self._region = region
        self._endpoint_url = endpoint_url
        self._profile_name = profile_name

    def to_client(self) -> BaseClient:
        session = boto3.Session(
            profile_name=self._profile_name,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            aws_session_token=self._aws_session_token,
        )
        return session.client(
            service_name="s3",
            region_name=self._region,
            endpoint_url=self._endpoint_url,
            verify=self._use_tls,
        )
