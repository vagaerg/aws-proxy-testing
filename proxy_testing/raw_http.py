from contextlib import contextmanager
import re
import socket
import ssl
from typing import Iterator
from urllib.parse import urlparse


@contextmanager
def _get_socket(scheme: str, url_host: str) -> Iterator[socket.socket]:
    @contextmanager
    def _obtain_sock(hostname: str):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as open_sock:
            open_sock.settimeout(5.0)
            if "https" in scheme.lower():
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                with context.wrap_socket(
                    open_sock, server_hostname=hostname, do_handshake_on_connect=False
                ) as open_ssl_sock:
                    yield open_ssl_sock
            else:
                yield open_sock

    host_parts = url_host.split(":")
    if len(host_parts) < 1 or len(host_parts) > 2:
        raise ValueError(f"Invalid host {host_parts}")
    try:
        hostname = host_parts[0].strip()
        port = int(host_parts[1]) if len(host_parts) > 1 else 80
        with _obtain_sock(hostname) as sock:
            sock.connect((hostname, port))
            yield sock
    except ValueError:
        raise ValueError(f"Invalid host {host_parts}")


def send_raw_http_request(final_url: str, headers: dict[str, str], data: bytes) -> int:
    """
    Horrible (but useful) helper to send HTTP requests by hand since most libraries don't support
    HTTP trailer headers
    """
    parsed_url = urlparse(final_url)
    raw_data_to_send = "\r\n".join(
        [
            f"PUT {parsed_url.path} HTTP/1.1",
            *[
                f"{header_name}: {header_value}"
                for header_name, header_value in headers.items()
            ],
        ]
    ).encode("utf-8")
    raw_data_to_send += b"\r\n\r\n"
    raw_data_to_send += data
    with _get_socket(parsed_url.scheme, parsed_url.netloc) as open_sock:
        open_sock.sendall(raw_data_to_send)
        raw_response_bytes = open_sock.recv(1024)
        print(raw_response_bytes)
        return int(
            re.match(r"HTTP/[\d\.]+ (\d+).*", raw_response_bytes.decode("utf-8"))
            .group(1)
            .strip()
        )
