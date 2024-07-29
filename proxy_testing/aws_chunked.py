from math import ceil
from textwrap import dedent
from .constants import HEX_SIGNATURE_SIZE
from .datamodel import BaseSignedAwsRequest
from .request_helpers import sha256


def get_aws_chunked_content_length(
    unchunked_data_length: int,
    chunk_count: int,
) -> int:
    chunk_size = ceil(unchunked_data_length / chunk_count)
    penultimate_chunk_size = unchunked_data_length - (chunk_size * (chunk_count - 1))
    chunk_header_size = len(";chunk-signature=\r\n") + HEX_SIGNATURE_SIZE
    return (
        # Data length
        unchunked_data_length
        +
        # Number of chunks * chunk header size, plus 1 for the final chunk
        (chunk_header_size * (chunk_count + 1))
        +
        # Main data chunks - 1 * the size of the chunk as a hex string
        ((chunk_count - 1) * len(f"{chunk_size:x}"))
        +
        # Size of the penultimate chunk as a hex string
        len(f"{penultimate_chunk_size:x}")
        +
        # Size of the last chunk ("0", a 1 character hex string)
        1
        +
        # \r\n for each chunk, including the final one
        2 * (chunk_count + 1)
    )


def get_aws_chunked_content_string(
    data_to_encode: str, chunk_count: int, built_request: BaseSignedAwsRequest
) -> str:
    def _get_string_to_sign(previous_signature: str, data_hash: str) -> str:
        return dedent(
            f"""
        AWS4-HMAC-SHA256-PAYLOAD
        {built_request.formatted_request_timestamp}
        {built_request.key_path}
        {previous_signature}
        e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        {data_hash}"""
        ).strip()

    def _get_chunk(previous_signature: str, data_in_chunk: str) -> tuple[str, str]:
        signature = built_request.signer.signature(
            _get_string_to_sign(previous_signature, sha256(data_in_chunk)),
            built_request.request,
        )

        return (
            signature,
            f"{len(data_in_chunk):x};chunk-signature={signature}\r\n{data_in_chunk}\r\n",
        )

    result = ""
    chunk_size = ceil(len(data_to_encode.encode("utf-8")) / chunk_count)
    last_seen_signature = built_request.signature
    for chunk_pos in range(chunk_count + 1):
        last_seen_signature, this_chunk_data = _get_chunk(
            last_seen_signature,
            data_to_encode[chunk_pos * chunk_size : (chunk_pos + 1) * chunk_size],
        )
        result += this_chunk_data
    return result
