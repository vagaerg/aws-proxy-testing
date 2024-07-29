from dataclasses import dataclass
from functools import cached_property
import datetime as dt
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from .constants import AWS_TIMESTAMP_FORMAT
import boto3


@dataclass
class RuntimeConfig:
    s3_endpoint: str
    access_key: str
    secret_access_key: str

    @cached_property
    def s3_client(self) -> boto3.client:
        return boto3.client(
            "s3",
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_access_key,
            verify=False,
        )


@dataclass
class BaseSignedAwsRequest:
    access_key: str
    secret_access_key: str
    request_timestamp: dt.datetime
    region: str
    service: str
    signature: str
    request: AWSRequest
    signer: SigV4Auth

    @property
    def key_path(self) -> str:
        return f"{self.request_timestamp.strftime('%Y%m%d')}/{self.region}/{self.service}/aws4_request"

    @property
    def formatted_request_timestamp(self) -> str:
        return self.request_timestamp.strftime(AWS_TIMESTAMP_FORMAT)


@dataclass
class TestResult:
    request_content: str
    content_length_header: bool
    decoded_content_length_header: bool
    sha256_header: str
    transfer_encoding_header: str | None
    content_encoding_header: str | None
    result: str | int
