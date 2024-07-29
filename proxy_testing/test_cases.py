from .raw_http import send_raw_http_request
import requests
from functools import wraps
from typing import ParamSpec, TypeVar, Callable
from .datamodel import RuntimeConfig, TestResult, BaseSignedAwsRequest
from .request_helpers import build_request, sha256
from .s3_helpers import ensure_bucket_exists, ensure_content_matches
from .aws_chunked import get_aws_chunked_content_length, get_aws_chunked_content_string
from .http_chunked import get_http_encoded_chunks_raw

ParamT = ParamSpec("ParamT")
ReturnT = TypeVar("ReturnT")


def _get_response_or_exc_info(
    wrapped: Callable[ParamT, ReturnT],
) -> Callable[ParamT, ReturnT | str]:
    @wraps(wrapped)
    def _(*args: ParamT.args, **kwargs: ParamT.kwargs) -> ReturnT | str:
        try:
            return wrapped(*args, **kwargs)
        except Exception as e:
            return f"FAILURE: {e}"

    return _


@_get_response_or_exc_info
def _standard_upload(
    runtime_config: RuntimeConfig,
    bucket: str,
    key: str,
    data: str,
    hash_data: bool = True,
) -> int:
    def _prepare_headers(headers: dict[str, str]) -> None:
        headers["Content-Length"] = str(len(data.encode("utf-8")))
        if hash_data:
            headers["X-Amz-Content-Sha256"] = sha256(data)
        else:
            headers["X-Amz-Content-Sha256"] = "UNSIGNED-PAYLOAD"

    ensure_bucket_exists(runtime_config, bucket)
    built_request = build_request(runtime_config, bucket, key, "PUT", _prepare_headers)
    response = requests.put(
        built_request.request.url,
        data=data,
        headers=dict(built_request.request.headers.items()),
        verify=False,
    )
    if response.ok:
        ensure_content_matches(runtime_config, bucket, key, data)
    return response.status_code


def standard_upload(
    runtime_config: RuntimeConfig,
    bucket: str,
    key: str,
    data: str,
    hash_data: bool = True,
) -> TestResult:
    return TestResult(
        "unchunked content",
        True,
        False,
        "actual hash" if hash_data else "UNSIGNED-PAYLOAD",
        "not present",
        "not present",
        _standard_upload(runtime_config, bucket, key, data, hash_data),
    )


@_get_response_or_exc_info
def _aws_chunked_upload(
    runtime_config: RuntimeConfig,
    bucket: str,
    key: str,
    data: str,
    chunk_count: int,
    content_encoding: str | None,
    sha256_header: str,
    add_decoded_content_length: bool,
) -> int:
    total_data_length = len(data.encode("utf-8"))
    total_chunked_content_size = get_aws_chunked_content_length(
        total_data_length, chunk_count
    )

    def _prepare_headers(headers: dict[str, str]) -> None:
        if content_encoding:
            headers["Content-Encoding"] = content_encoding
        headers["X-Amz-Content-Sha256"] = sha256_header
        if add_decoded_content_length:
            headers["X-Amz-Decoded-Content-Length"] = str(total_data_length)
        headers["Content-Length"] = str(total_chunked_content_size)

    ensure_bucket_exists(runtime_config, bucket)
    built_request = build_request(runtime_config, bucket, key, "PUT", _prepare_headers)
    all_chunks = get_aws_chunked_content_string(data, chunk_count, built_request)
    headers_to_send = dict(built_request.request.headers.items())
    response = requests.put(
        built_request.request.url,
        data=all_chunks.encode("utf-8"),
        headers=headers_to_send,
        verify=False,
    )
    if response.ok:
        ensure_content_matches(runtime_config, bucket, key, data)
    return response.status_code


def aws_chunked_upload(
    runtime_config: RuntimeConfig,
    bucket: str,
    key: str,
    data: str,
    chunk_count: int,
    content_encoding: str = "aws-chunked",
    sha256_header: str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
    add_decoded_content_length: bool = True,
) -> TestResult:
    return TestResult(
        f"aws-chunked-{chunk_count}-chunks",
        True,
        add_decoded_content_length,
        sha256_header,
        None,
        content_encoding,
        _aws_chunked_upload(
            runtime_config=runtime_config,
            bucket=bucket,
            key=key,
            data=data,
            chunk_count=chunk_count,
            content_encoding=content_encoding,
            sha256_header=sha256_header,
            add_decoded_content_length=add_decoded_content_length,
        ),
    )


def aws_chunked_upload_with_chunked_transfer_encoding(
    runtime_config: RuntimeConfig,
    bucket: str,
    key: str,
    data: str,
    aws_chunk_count: int,
    http_chunk_count: int,
    content_encoding: str | None,
    sha256_header: str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
    add_decoded_content_length: bool = True,
    trailer_header: str | None = None,
    trailer_header_value: str | None = None,
) -> TestResult:
    test_name = (
        f"aws-and-http-chunked-with-trailer-{trailer_header}"
        if trailer_header
        else "aws-and-http-chunked"
    )
    return TestResult(
        f"{test_name}-{aws_chunk_count}-aws-chunks-{http_chunk_count}-http-chunks",
        False,
        add_decoded_content_length,
        sha256_header,
        "chunked",
        content_encoding,
        _http_chunked_upload_with_trailer(
            runtime_config=runtime_config,
            bucket=bucket,
            key=key,
            data=data,
            chunk_count=http_chunk_count,
            content_encoding=content_encoding,
            sha256_header=sha256_header,
            add_decoded_content_length=add_decoded_content_length,
            trailer_header=trailer_header,
            trailer_header_value=trailer_header_value,
            data_generator=lambda raw_content, request: get_aws_chunked_content_string(
                raw_content, aws_chunk_count, request
            ),
        ),
    )


@_get_response_or_exc_info
def _http_chunked_upload_with_trailer(
    runtime_config: RuntimeConfig,
    bucket: str,
    key: str,
    data: str,
    chunk_count: int,
    content_encoding: str | None,
    sha256_header: str,
    add_decoded_content_length: bool,
    trailer_header: str | None,
    trailer_header_value: str | None,
    data_generator: Callable[[str, BaseSignedAwsRequest], str] | None = None,
) -> int:
    total_data_length = len(data.encode("utf-8"))

    def _prepare_headers(headers: dict[str, str]) -> None:
        if content_encoding:
            headers["Content-Encoding"] = content_encoding
        headers["X-Amz-Content-Sha256"] = sha256_header
        if add_decoded_content_length:
            headers["X-Amz-Decoded-Content-Length"] = str(total_data_length)
        headers["Transfer-Encoding"] = "chunked"
        if trailer_header is not None:
            headers["Trailer"] = trailer_header
            headers["x-amz-trailer"] = trailer_header

    ensure_bucket_exists(runtime_config, bucket)
    built_request = build_request(runtime_config, bucket, key, "PUT", _prepare_headers)
    headers_to_send = dict(built_request.request.headers.items())

    trailer_headers = None
    if trailer_header is not None and trailer_header_value is not None:
        trailer_headers = {trailer_header: trailer_header_value}

    if data_generator:
        generated_data = data_generator(data, built_request)
    else:
        generated_data = data
    data_to_send = get_http_encoded_chunks_raw(
        generated_data, chunk_count, "", trailer_headers
    )
    response_code = send_raw_http_request(
        built_request.request.url, headers_to_send, data_to_send
    )
    if 400 > response_code >= 200:
        ensure_content_matches(runtime_config, bucket, key, data)
    return response_code


def http_chunked_upload(
    runtime_config: RuntimeConfig,
    bucket: str,
    key: str,
    data: str,
    chunk_count: int,
    content_encoding: str | None,
    sha256_header: str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
    add_decoded_content_length: bool = True,
    trailer_header: str | None = None,
    trailer_header_value: str | None = None,
) -> TestResult:
    test_name = (
        f"http-chunked-with-trailer-{trailer_header}"
        if trailer_header
        else "http-chunked"
    )
    return TestResult(
        f"{test_name}-{chunk_count}-chunks",
        False,
        add_decoded_content_length,
        sha256_header,
        "chunked",
        content_encoding,
        _http_chunked_upload_with_trailer(
            runtime_config=runtime_config,
            bucket=bucket,
            key=key,
            data=data,
            chunk_count=chunk_count,
            content_encoding=content_encoding,
            sha256_header=sha256_header,
            add_decoded_content_length=add_decoded_content_length,
            trailer_header=trailer_header,
            trailer_header_value=trailer_header_value,
        ),
    )
