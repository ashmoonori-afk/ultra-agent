"""Wire-level tests for ultra_agent.http against a real loopback HTTP server.

Every test drives the production ``HttpClient`` against a ``ThreadingHTTPServer``
bound to 127.0.0.1, so the real urllib path, redirect policy, size cap and
timeout are exercised. Server handlers are gated by ``threading.Event`` objects
and inspectable records, so synchronization never depends on sleeps.
"""

from __future__ import annotations

import json
import socket
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from ultra_agent.http import (
    DEFAULT_TIMEOUT_SECONDS,
    MAX_RESPONSE_BYTES,
    MAX_TIMEOUT_SECONDS,
    HttpClient,
    TransportError,
)

Responder = Callable[[BaseHTTPRequestHandler, bytes], None]
HeaderPairs = Iterable[tuple[str, str]]

_TOK = "fixture-token"


@dataclass
class _Recorded:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


class _Recorder:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.records: list[_Recorded] = []

    def add(self, record: _Recorded) -> None:
        with self._lock:
            self.records.append(record)

    def paths(self) -> list[str]:
        with self._lock:
            return [record.path for record in self.records]


def _make_handler(recorder: _Recorder, responder: Responder) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._dispatch()

        def do_POST(self) -> None:
            self._dispatch()

        def _dispatch(self) -> None:
            length = int(self.headers.get("Content-Length") or "0")
            body = self.rfile.read(length) if length else b""
            recorder.add(
                _Recorded(
                    method=self.command,
                    path=self.path,
                    headers=dict(self.headers.items()),
                    body=body,
                )
            )
            try:
                responder(self, body)
            except OSError:
                pass

        def log_message(self, format: str, *args: object) -> None:
            pass

    return _Handler


class _TestServer:
    def __init__(self, responder: Responder) -> None:
        self.recorder = _Recorder()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(self.recorder, responder))
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(
            target=self.httpd.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )

    def __enter__(self) -> _TestServer:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    @property
    def base_url(self) -> str:
        host, port = self.httpd.server_address[0], self.httpd.server_address[1]
        return f"http://{host}:{port}"


def _raw_responder(
    status: int,
    data: bytes,
    content_type: str = "application/json",
    extra_headers: HeaderPairs = (),
) -> Responder:
    def responder(handler: BaseHTTPRequestHandler, body: bytes) -> None:
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(data)))
        for name, value in extra_headers:
            handler.send_header(name, value)
        handler.end_headers()
        handler.wfile.write(data)

    return responder


def _unused_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def test_timeout_defaults_and_bounds() -> None:
    assert HttpClient().timeout == DEFAULT_TIMEOUT_SECONDS
    assert HttpClient(timeout=MAX_TIMEOUT_SECONDS).timeout == MAX_TIMEOUT_SECONDS
    for bad in (0.0, -1.0, MAX_TIMEOUT_SECONDS + 0.5, float("inf"), float("nan")):
        with pytest.raises(ValueError):
            HttpClient(timeout=bad)


def test_post_success_returns_parsed_json() -> None:
    payload = {"ok": True, "items": [1, "two", None]}
    with _TestServer(_raw_responder(200, json.dumps(payload).encode())) as server:
        client = HttpClient()
        result = client.request(
            "POST",
            f"{server.base_url}/v1/send",
            {"Content-Type": "application/json", "X-Custom": "abc"},
            {"a": 1},
        )
    assert result == payload
    record = server.recorder.records[0]
    assert record.method == "POST"
    assert record.headers["Content-Type"] == "application/json"
    assert record.headers["X-Custom"] == "abc"
    assert json.loads(record.body) == {"a": 1}


def test_redirect_is_rejected_without_following() -> None:
    def responder(handler: BaseHTTPRequestHandler, body: bytes) -> None:
        handler.send_response(302)
        handler.send_header("Location", "/next")
        handler.send_header("Content-Length", "0")
        handler.end_headers()

    with _TestServer(responder) as server:
        client = HttpClient()
        with pytest.raises(TransportError) as info:
            client.request("GET", f"{server.base_url}/start", {})
        assert server.recorder.paths() == ["/start"]
    assert info.value.status == 302
    assert info.value.ambiguous is False
    assert "redirect" in str(info.value)


@pytest.mark.parametrize(
    ("header_value", "expected"),
    [("7", 7.0), ("0", 0.0), ("3600", 3600.0), ("2.5", 2.5)],
)
def test_rate_limit_reports_validated_retry_after(header_value: str, expected: float) -> None:
    headers = (("Retry-After", header_value),)
    responder = _raw_responder(429, b'{"error":"slow"}', extra_headers=headers)
    with _TestServer(responder) as server:
        client = HttpClient()
        with pytest.raises(TransportError) as info:
            client.request(
                "POST", f"{server.base_url}/send", {"Content-Type": "application/json"}, {"x": 1}
            )
    assert info.value.status == 429
    assert info.value.retry_after == expected
    assert info.value.ambiguous is False


@pytest.mark.parametrize(
    "header_value",
    ["-1", "3601", "nan", "inf", "soon", "Wed, 21 Oct 2015 07:28:00 GMT"],
)
def test_invalid_retry_after_is_none(header_value: str) -> None:
    responder = _raw_responder(429, b"{}", extra_headers=(("Retry-After", header_value),))
    with _TestServer(responder) as server:
        client = HttpClient()
        with pytest.raises(TransportError) as info:
            client.request("GET", f"{server.base_url}/send", {})
    assert info.value.status == 429
    assert info.value.retry_after is None


def test_post_server_error_is_ambiguous_and_not_retried() -> None:
    with _TestServer(_raw_responder(500, b'{"error":"boom"}')) as server:
        client = HttpClient()
        with pytest.raises(TransportError) as info:
            client.request(
                "POST", f"{server.base_url}/send", {"Content-Type": "application/json"}, {"x": 1}
            )
        assert len(server.recorder.records) == 1
    assert info.value.status == 500
    assert info.value.ambiguous is True


def test_get_server_error_is_not_ambiguous() -> None:
    with _TestServer(_raw_responder(500, b'{"error":"boom"}')) as server:
        client = HttpClient()
        with pytest.raises(TransportError) as info:
            client.request("GET", f"{server.base_url}/read", {})
    assert info.value.status == 500
    assert info.value.ambiguous is False


def test_non_json_body_raises_safe_error() -> None:
    responder = _raw_responder(200, b"<html>not json</html>", content_type="text/html")
    with _TestServer(responder) as server:
        client = HttpClient()
        with pytest.raises(TransportError) as info:
            client.request("GET", f"{server.base_url}/page", {})
    assert info.value.status == 200
    assert "<html>" not in str(info.value)


def test_oversized_body_raises_safe_error() -> None:
    oversized = b"[" + b"0," * (MAX_RESPONSE_BYTES // 2) + b"0]"
    assert len(oversized) > MAX_RESPONSE_BYTES
    with _TestServer(_raw_responder(200, oversized)) as server:
        client = HttpClient()
        with pytest.raises(TransportError) as info:
            client.request("GET", f"{server.base_url}/big", {})
    assert "size" in str(info.value)


def test_token_in_url_never_appears_in_error() -> None:
    with _TestServer(_raw_responder(500, b'{"ok":false}')) as server:
        client = HttpClient()
        with pytest.raises(TransportError) as info:
            client.request(
                "POST",
                f"{server.base_url}/bot{_TOK}/sendMessage",
                {"Content-Type": "application/json"},
                {"text": "hi"},
            )
    rendered = f"{info.value!s} {info.value!r} {info.value.args!r}"
    assert _TOK not in rendered
    assert "sendMessage" not in rendered


def test_token_not_leaked_on_transport_failure() -> None:
    client = HttpClient(timeout=2.0)
    with pytest.raises(TransportError) as info:
        client.request("GET", f"{_TOK}/getUpdates", {})
    assert _TOK not in f"{info.value!s} {info.value!r}"


def test_timeout_fires_while_server_gate_is_held() -> None:
    received = threading.Event()
    release = threading.Event()

    def responder(handler: BaseHTTPRequestHandler, body: bytes) -> None:
        received.set()
        release.wait(timeout=10.0)
        handler.send_response(200)
        handler.send_header("Content-Length", "2")
        handler.end_headers()
        handler.wfile.write(b"{}")

    with _TestServer(responder) as server:
        client = HttpClient(timeout=0.5)
        with pytest.raises(TransportError) as info:
            client.request("GET", f"{server.base_url}/slow", {})
        assert received.is_set()
        release.set()
    assert info.value.status is None
    assert info.value.ambiguous is False


def test_server_resources_closed_after_use() -> None:
    server = _TestServer(_raw_responder(200, b'{"ok":true}'))
    with server:
        client = HttpClient()
        assert client.request("GET", f"{server.base_url}/x", {}) == {"ok": True}
    assert server.thread.is_alive() is False
    assert server.httpd.socket.fileno() == -1


def test_empty_no_content_success_is_not_a_json_failure() -> None:
    with _TestServer(_raw_responder(204, b"")) as server:
        assert HttpClient().request("POST", f"{server.base_url}/typing", {}) is None


def test_accepted_post_with_unreadable_response_is_uncertain() -> None:
    with _TestServer(_raw_responder(200, b"not-json")) as server:
        with pytest.raises(TransportError) as info:
            HttpClient().request("POST", f"{server.base_url}/send", {})
        assert len(server.recorder.records) == 1
    assert info.value.ambiguous is True


def test_json_rate_limit_body_supplies_telegram_delay() -> None:
    body = b'{"ok":false,"parameters":{"retry_after":3}}'
    with _TestServer(_raw_responder(429, body)) as server:
        with pytest.raises(TransportError) as info:
            HttpClient().request("POST", f"{server.base_url}/send", {})
    assert info.value.retry_after == 3


def test_nonfinite_response_is_not_valid_json() -> None:
    with _TestServer(_raw_responder(200, b'{"value":NaN}')) as server:
        with pytest.raises(TransportError):
            HttpClient().request("GET", f"{server.base_url}/data", {})
