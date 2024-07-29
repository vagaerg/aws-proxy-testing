from math import ceil
from typing import Iterator


def get_http_chunked_content_length(
    unchunked_data_length: int,
    chunk_count: int,
) -> int:
    chunk_size = ceil(unchunked_data_length / chunk_count)
    penultimate_chunk_size = unchunked_data_length - (chunk_size * (chunk_count - 1))
    return (
        # Data length
        unchunked_data_length
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
        # \r\n for each chunk's header, including the final one
        # another \r\n for each chunk after its data, including the final one
        4 * (chunk_count + 1)
    )


def get_http_encoded_chunks_iter(
    data_to_encode: str, chunk_count: int
) -> Iterator[bytes]:
    data_bytes = data_to_encode.encode("utf-8")
    chunk_size = ceil(len(data_bytes) / chunk_count)
    for chunk_pos in range(chunk_count):
        yield data_bytes[chunk_pos * chunk_size : (chunk_pos + 1) * chunk_size]


def get_http_encoded_chunks_raw(
    data_to_encode: str,
    chunk_count: int,
    extra_chunk_header_content: str = "",
    trailer_headers: dict[str, str] | None = None,
) -> bytes:
    def _get_header_for_chunk(chunk: bytes) -> bytes:
        return f"{len(chunk):x}{extra_chunk_header_content}\r\n".encode("utf-8")

    result = b""
    for data_chunk in get_http_encoded_chunks_iter(data_to_encode, chunk_count):
        result += _get_header_for_chunk(data_chunk)
        result += data_chunk + b"\r\n"

    # Final 0-sized chunk
    result += _get_header_for_chunk(b"")
    if trailer_headers:
        for header_name, header_value in trailer_headers.items():
            result += f"{header_name}: {header_value}\r\n".encode("utf-8")
    result += b"\r\n"

    return result
