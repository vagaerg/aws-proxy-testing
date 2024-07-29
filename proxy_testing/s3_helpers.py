import io
from .datamodel import RuntimeConfig
from botocore.exceptions import ClientError
from dataclasses import dataclass
from typing import Any


@dataclass
class S3File:
    contents: str
    metadata: dict[str, Any] | None = None


def get_file_from_s3(
    runtime_config: RuntimeConfig, bucket: str, key: str
) -> S3File | None:
    def _do_get():
        s3client = runtime_config.s3_client
        out = io.BytesIO()
        s3client.download_fileobj(Bucket=bucket, Key=key, Fileobj=out)
        out.seek(0)
        object_metadata = s3client.head_object(Bucket=bucket, Key=key)
        if "ResponseMetadata" in object_metadata:
            object_metadata.pop("ResponseMetadata")

        return S3File(contents=out.read().decode("utf-8"), metadata=object_metadata)

    try:
        return _do_get()
    except Exception:
        return None


def ensure_content_matches(
    runtime_config: RuntimeConfig, bucket: str, key: str, expected_content: str
) -> None:
    assert (
        get_file_from_s3(runtime_config, bucket, key).contents == expected_content
    ), "Unexpected contents"


def ensure_bucket_exists(runtime_config: RuntimeConfig, bucket_name: str) -> None:
    def _try_extract_nested_error(
        parent_key: str, child_key: str, resp: dict
    ) -> int | None:
        if (extracted := resp.get(parent_key)) and isinstance(extracted, dict):
            if (child_value := extracted.get(child_key)) and isinstance(
                child_value,
                (str, int),
            ):
                try:
                    return int(child_value)
                except Exception:
                    pass
        return None

    def _does_bucket_exist() -> bool:
        try:
            runtime_config.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            resp = e.response
            if not isinstance(resp, dict):
                raise
            error_code = _try_extract_nested_error("Error", "Code", resp)
            if error_code == 404:
                return False
            error_code = _try_extract_nested_error(
                "ResponseMetadata", "HTTPStatusCode", resp
            )
            if error_code == 404:
                return False
            raise

    if not _does_bucket_exist():
        runtime_config.s3_client.create_bucket(Bucket=bucket_name)
