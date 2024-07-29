from uuid import uuid4
from collections import namedtuple
from tabulate import tabulate
from typing import Iterable
from proxy_testing.constants import (
    LONG_TEXT,
)
from proxy_testing.datamodel import RuntimeConfig, TestResult
from proxy_testing.test_cases import (
    standard_upload,
    aws_chunked_upload,
    http_chunked_upload,
    aws_chunked_upload_with_chunked_transfer_encoding,
)
from proxy_testing.request_helpers import sha256


CONFIG = RuntimeConfig("https://localhost:8443", "testidentity", "testsecret")

TestRunner = namedtuple("TestRunner", ("callable", "args"))
STANDARD_UPLOAD_TESTS = tuple(
    TestRunner(standard_upload, args)
    for args in (
        {"data": "some small string", "hash_data": True},
        {"data": "some small string", "hash_data": False},
    )
)
AWS_CHUNKED_UPLOAD_TESTS = tuple(
    TestRunner(aws_chunked_upload, args)
    for args in (
        # Cases with the STREAMING sha256 header
        {
            "data": LONG_TEXT,
            "chunk_count": 3,
            "content_encoding": "aws-chunked",
            "add_decoded_content_length": True,
        },
        {
            "data": LONG_TEXT,
            "chunk_count": 3,
            "content_encoding": "aws-chunked",
            "add_decoded_content_length": False,
        },
    )
)
HTTP_CHUNKED_TEST_CASES = tuple(
    TestRunner(http_chunked_upload, args)
    for args in (
        {
            "data": LONG_TEXT,
            "chunk_count": 3,
            "content_encoding": None,
            "sha256_header": "UNSIGNED-PAYLOAD",
            "add_decoded_content_length": True,
            "trailer_header": None,
            "trailer_header_value": None,
        },
        {
            "data": LONG_TEXT,
            "chunk_count": 3,
            "content_encoding": None,
            "sha256_header": "UNSIGNED-PAYLOAD",
            "add_decoded_content_length": True,
            "trailer_header": "x-amz-checksum-sha256",
            "trailer_header_value": sha256(LONG_TEXT),
        },
        {
            "data": LONG_TEXT,
            "chunk_count": 3,
            "content_encoding": None,
            "sha256_header": "UNSIGNED-PAYLOAD",
            "add_decoded_content_length": False,
            "trailer_header": None,
            "trailer_header_value": None,
        },
        {
            "data": LONG_TEXT,
            "chunk_count": 3,
            "content_encoding": None,
            "sha256_header": "UNSIGNED-PAYLOAD",
            "add_decoded_content_length": False,
            "trailer_header": "x-amz-checksum-sha256",
            "trailer_header_value": sha256(LONG_TEXT),
        },
        {
            "data": LONG_TEXT,
            "chunk_count": 3,
            "content_encoding": None,
            "sha256_header": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
            "add_decoded_content_length": True,
            "trailer_header": None,
            "trailer_header_value": None,
        },
        {
            "data": LONG_TEXT,
            "chunk_count": 3,
            "content_encoding": None,
            "sha256_header": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
            "add_decoded_content_length": True,
            "trailer_header": "x-amz-checksum-sha256",
            "trailer_header_value": sha256(LONG_TEXT),
        },
        {
            "data": LONG_TEXT,
            "chunk_count": 3,
            "content_encoding": None,
            "sha256_header": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
            "add_decoded_content_length": False,
            "trailer_header": None,
            "trailer_header_value": None,
        },
        {
            "data": LONG_TEXT,
            "chunk_count": 3,
            "content_encoding": None,
            "sha256_header": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
            "add_decoded_content_length": False,
            "trailer_header": "x-amz-checksum-sha256",
            "trailer_header_value": sha256(LONG_TEXT),
        },
    )
)

AWS_CHUNKED_HTTP_CHUNKED_UPLOADS = tuple(
    TestRunner(aws_chunked_upload_with_chunked_transfer_encoding, args)
    for args in (
        {
            "data": LONG_TEXT,
            "aws_chunk_count": 3,
            "http_chunk_count": 3,
            "content_encoding": "aws-chunked",
            "sha256_header": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "add_decoded_content_length": True,
            "trailer_header": None,
            "trailer_header_value": None,
        },
        {
            "data": LONG_TEXT,
            "aws_chunk_count": 3,
            "http_chunk_count": 3,
            "content_encoding": "aws-chunked",
            "sha256_header": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "add_decoded_content_length": True,
            "trailer_header": "x-amz-checksum-sha256",
            "trailer_header_value": sha256(LONG_TEXT),
        },
        {
            "data": LONG_TEXT,
            "aws_chunk_count": 3,
            "http_chunk_count": 3,
            "content_encoding": "aws-chunked",
            "sha256_header": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "add_decoded_content_length": False,
            "trailer_header": None,
            "trailer_header_value": None,
        },
        {
            "data": LONG_TEXT,
            "aws_chunk_count": 3,
            "http_chunk_count": 3,
            "content_encoding": "aws-chunked",
            "sha256_header": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
            "add_decoded_content_length": False,
            "trailer_header": "x-amz-checksum-sha256",
            "trailer_header_value": sha256(LONG_TEXT),
        },
    )
)


def run_tests(
    config: RuntimeConfig,
    tests_and_buckets: list[tuple[str, Iterable[TestRunner]]],
):
    header_text = (
        "Request Content",
        "Content-Length header",
        "X-Amz-Decoded-Content-Length header",
        "X-Amz-Content-SHA256 header",
        "Transfer-Encoding header",
        "Content-Encoding header",
        "Result",
    )
    results: list[TestResult] = []
    for bucket_name, test_cases in tests_and_buckets:
        results.extend(
            [
                test_callable(config, bucket_name, uuid4().hex, **args)
                for test_callable, args in test_cases
            ]
        )

    print(tabulate(results, headers=header_text))


run_tests(
    CONFIG,
    [
        ("standard-upload-proxy-tests", STANDARD_UPLOAD_TESTS),
        ("aws-chunked-proxy-tests", AWS_CHUNKED_UPLOAD_TESTS),
        ("aws-chunked-http-chunked-proxy-tests", AWS_CHUNKED_HTTP_CHUNKED_UPLOADS),
        ("raw-http-chunked-proxy-tests", HTTP_CHUNKED_TEST_CASES),
    ],
)
