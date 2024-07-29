import re
import datetime as dt
import hashlib
from .datamodel import BaseSignedAwsRequest, RuntimeConfig
from .constants import TEST_CONTENT_TYPE, DEFAULT_REGION, AWS_TIMESTAMP_FORMAT
from typing import Callable
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials
from urllib.parse import urljoin, urlparse


def sha256(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def build_request(
    runtime_config: RuntimeConfig,
    bucket: str,
    key: str,
    method: str,
    header_modifier: Callable[[dict[str, str]], None] | None = None,
) -> BaseSignedAwsRequest:
    if header_modifier is None:
        header_modifier = lambda _: None  # noqa: E731
    base_url = runtime_config.s3_endpoint
    base_url = base_url + "/" if not base_url.endswith("/") else base_url
    final_url = urljoin(base_url, f"{bucket}/{key}")
    parsed_url = urlparse(final_url)

    headers = {
        "Host": parsed_url.netloc,
        "Content-Type": TEST_CONTENT_TYPE,
    }
    header_modifier(headers)
    aws_request = AWSRequest(method=method, url=final_url, headers=headers)
    sigv4_signer = SigV4Auth(
        Credentials(runtime_config.access_key, runtime_config.secret_access_key),
        "s3",
        DEFAULT_REGION,
    )
    sigv4_signer.add_auth(aws_request)
    signature = re.match(
        r"AWS4-HMAC-SHA256.*Signature=(\w+)$", aws_request.headers["Authorization"]
    ).group(1)
    return BaseSignedAwsRequest(
        access_key=runtime_config.access_key,
        secret_access_key=runtime_config.secret_access_key,
        request_timestamp=dt.datetime.strptime(
            aws_request.headers["x-amz-date"], AWS_TIMESTAMP_FORMAT
        ),
        region=DEFAULT_REGION,
        service="s3",
        signature=signature,
        request=aws_request,
        signer=sigv4_signer,
    )
