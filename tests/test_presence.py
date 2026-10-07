"""Wire-level tests for ultra_agent.presence against real loopback servers.

Every test drives the production :class:`PresenceSession` against a real
``websockets.sync`` server (and a real ``ThreadingHTTPServer`` for Slack's Web
API calls) bound to 127.0.0.1. Server handlers synchronize with the test through
``threading.Event`` objects and recorded frames, so no test depends on a sleep.
"""

from __future__ import annotations

import json
import socket
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest
from websockets.sync.server import ServerConnection, serve

from ultra_agent import presence
from ultra_agent.http import HttpClient
from ultra_agent.models import MessengerConfig
from ultra_agent.presence import (
    PresenceAuthError,
    PresenceError,
    PresenceSession,
    PresenceUnsupportedError,
)

_TOKEN_ENV = "ULTRA_PRESENCE_TEST_TOKEN"
_COOKIE_ENV = "ULTRA_PRESENCE_TEST_COOKIE"
_TOKEN = "fixture-token-value"
_COOKIE = "fixture-cookie-value"
_READY_TIMEOUT = 10.0

Responder = Callable[[str, str], tuple[int, dict[str, object]]]


def _presence_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "ultra-agent-presence" and t.is_alive()]


class _WsServer:
    """A real loopback websocket server running ``handler`` for each connection."""

    def __init__(self, handler: Callable[[ServerConnection], None]) -> None:
        self._sock = socket.create_server(("127.0.0.1", 0), family=socket.AF_INET)
        self.port = int(self._sock.getsockname()[1])
        self._server = serve(handler, sock=self._sock)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> _WsServer:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"


class _DiscordHandler:
    """Sends Hello, records Identify, acks READY and logs heartbeats."""

    def __init__(self, heartbeat_interval_ms: int = 100) -> None:
        self._interval = heartbeat_interval_ms
        self.identify: dict[str, object] | None = None
        self.heartbeats: list[object] = []
        self.identify_seen = threading.Event()
        self.heartbeat_seen = threading.Event()
        self.closed = threading.Event()

    def __call__(self, connection: ServerConnection) -> None:
        try:
            connection.send(json.dumps({"op": 10, "d": {"heartbeat_interval": self._interval}}))
            self.identify = json.loads(connection.recv())
            self.identify_seen.set()
            connection.send(
                json.dumps({"op": 0, "s": 7, "t": "READY", "d": {"user": {"id": "1"}}})
            )
            while True:
                message = json.loads(connection.recv())
                if message.get("op") == 1:
                    self.heartbeats.append(message.get("d"))
                    self.heartbeat_seen.set()
                    connection.send(json.dumps({"op": 11, "d": None}))
        except Exception:
            pass
        finally:
            self.closed.set()


class _SlackHandler:
    """Sends ``hello`` and answers every ``tickle`` frame."""

    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.path = ""
        self.tickle_seen = threading.Event()
        self.closed = threading.Event()

    def __call__(self, connection: ServerConnection) -> None:
        self.headers = dict(connection.request.headers)
        self.path = connection.request.path
        try:
            connection.send(json.dumps({"type": "hello"}))
            while True:
                message = json.loads(connection.recv())
                if message.get("type") == "tickle":
                    self.tickle_seen.set()
                    connection.send(json.dumps({"type": "tickle", "id": message.get("id")}))
        except Exception:
            pass
        finally:
            self.closed.set()


class _HttpServer:
    """A real loopback HTTP server delegating each request to ``responder``."""

    def __init__(self, responder: Responder) -> None:
        self._responder = responder
        self._lock = threading.Lock()
        self._records: list[dict[str, object]] = []
        outer = self

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                outer._handle(self)

            def do_POST(self) -> None:
                outer._handle(self)

            def log_message(self, format: str, *args: object) -> None:
                pass

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._httpd.daemon_threads = True
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )

    def _handle(self, handler: BaseHTTPRequestHandler) -> None:
        length = int(handler.headers.get("Content-Length") or "0")
        body = handler.rfile.read(length) if length else b""
        with self._lock:
            self._records.append(
                {
                    "method": handler.command,
                    "path": handler.path,
                    "headers": dict(handler.headers.items()),
                    "body": body,
                }
            )
        status, payload = self._responder(handler.command, handler.path)
        data = json.dumps(payload).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)

    def records(self) -> list[dict[str, object]]:
        with self._lock:
            return list(self._records)

    def __enter__(self) -> _HttpServer:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{int(self._httpd.server_address[1])}"


def _header(record: dict[str, object], name: str) -> str | None:
    headers = record["headers"]
    assert isinstance(headers, dict)
    lowered = {str(k).lower(): str(v) for k, v in headers.items()}
    return lowered.get(name.lower())


def _slack_responder(ws_url: str) -> Responder:
    def responder(method: str, path: str) -> tuple[int, dict[str, object]]:
        if path == "/client.getWebSocketURL":
            return 200, {
                "ok": True, "primary_websocket_url": ws_url,
                "routing_context": "fixture-route",
            }
        if path == "/users.setPresence":
            return 200, {"ok": True}
        return 404, {"ok": False, "error": "unknown_method"}

    return responder


def test_discord_identify_ready_heartbeat_and_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    handler = _DiscordHandler(heartbeat_interval_ms=100)
    config = MessengerConfig(platform="discord", token_env=_TOKEN_ENV)

    with _WsServer(handler) as server:
        session = PresenceSession(config, gateway_url=server.url)
        with session:
            assert session.wait_ready(_READY_TIMEOUT) is True
            assert session.connected is True
            assert session.available is False
            assert session.phase == "connected"
            assert session.error is None
            assert handler.identify_seen.wait(_READY_TIMEOUT)
            assert handler.heartbeat_seen.wait(_READY_TIMEOUT)
        assert session.phase == "stopped"
        assert session.connected is False
        assert session.available is False
        assert handler.closed.wait(_READY_TIMEOUT)

    identify = handler.identify
    assert isinstance(identify, dict)
    assert identify["op"] == 2
    data = identify["d"]
    assert isinstance(data, dict)
    assert data["token"] == _TOKEN
    assert data["intents"] == 0
    properties = data["properties"]
    assert isinstance(properties, dict)
    assert set(properties) == {"$os", "$browser", "$device"}
    presence_payload = data["presence"]
    assert isinstance(presence_payload, dict)
    assert presence_payload["status"] == "online"
    assert handler.heartbeats[0] == 7
    assert _presence_threads() == []


def test_discord_missing_token_is_auth_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_TOKEN_ENV, raising=False)
    config = MessengerConfig(platform="discord", token_env=_TOKEN_ENV)
    session = PresenceSession(config)
    with pytest.raises(PresenceAuthError):
        session.start()
    assert session.phase == "idle"
    assert _presence_threads() == []


def test_discord_connection_failure_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    closed_port = int(probe.getsockname()[1])
    probe.close()
    config = MessengerConfig(platform="discord", token_env=_TOKEN_ENV)
    session = PresenceSession(config, gateway_url=f"ws://127.0.0.1:{closed_port}")

    session.start()
    assert session.wait_ready(_READY_TIMEOUT) is False
    session.stop()

    assert session.phase == "failed"
    assert session.connected is False
    assert session.error
    assert _TOKEN not in session.error
    assert _presence_threads() == []
    session.stop()


def test_telegram_presence_is_unsupported() -> None:
    config = MessengerConfig(platform="telegram", token_env=_TOKEN_ENV)
    session = PresenceSession(config)
    with pytest.raises(PresenceUnsupportedError):
        session.start()
    with pytest.raises(PresenceUnsupportedError):
        session.__enter__()
    assert _presence_threads() == []


def test_slack_handshake_tickle_and_away_on_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    monkeypatch.setenv(_COOKIE_ENV, _COOKIE)
    monkeypatch.setattr(presence, "_SLACK_TICKLE_INTERVAL_SECONDS", 0.05)
    ws_handler = _SlackHandler()
    config = MessengerConfig(platform="slack", token_env=_TOKEN_ENV, cookie_env=_COOKIE_ENV)

    with _WsServer(ws_handler) as ws_server:
        with _HttpServer(_slack_responder(ws_server.url)) as http_server:
            session = PresenceSession(
                config, HttpClient(), slack_base_url=http_server.base_url
            )
            with session:
                assert session.wait_ready(_READY_TIMEOUT) is True
                assert session.connected is True
                assert session.available is False
                assert ws_handler.tickle_seen.wait(_READY_TIMEOUT)
            assert session.phase == "stopped"
            assert ws_handler.closed.wait(_READY_TIMEOUT)
            api_records = http_server.records()

    handshake_headers = {k.lower(): v for k, v in ws_handler.headers.items()}
    assert handshake_headers.get("cookie") == f"d={_COOKIE}"
    assert handshake_headers.get("origin") == "https://app.slack.com"
    query = parse_qs(urlsplit(ws_handler.path).query)
    assert query["token"] == [_TOKEN]
    assert query["gateway_server"] == ["fixture-route"]
    assert query["slack_client"] == ["desktop"]

    ws_request = next(r for r in api_records if r["path"] == "/client.getWebSocketURL")
    assert _header(ws_request, "Authorization") == f"Bearer {_TOKEN}"
    assert _header(ws_request, "Cookie") == f"d={_COOKIE}"
    assert ws_request["method"] == "POST"
    presence_calls = [r for r in api_records if r["path"] == "/users.setPresence"]
    assert [json.loads(bytes(r["body"])) for r in presence_calls] == [
        {"presence": "auto"}, {"presence": "away"},
    ]
    away = presence_calls[-1]
    assert away["method"] == "POST"
    assert _header(away, "Authorization") == f"Bearer {_TOKEN}"
    assert json.loads(bytes(away["body"])) == {"presence": "away"}
    assert _presence_threads() == []


def test_presence_endpoints_cannot_export_credentials() -> None:
    config = MessengerConfig(platform="discord", token_env=_TOKEN_ENV)
    with pytest.raises(ValueError):
        PresenceSession(config, gateway_url="wss://example.invalid/socket")
    slack = MessengerConfig(platform="slack", token_env=_TOKEN_ENV, cookie_env=_COOKIE_ENV)
    with pytest.raises(ValueError):
        PresenceSession(slack, slack_base_url="https://example.invalid/api")


def test_disabled_presence_refuses_before_start(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    config = MessengerConfig(platform="discord", token_env=_TOKEN_ENV, enabled=False)
    session = PresenceSession(config, gateway_url="ws://127.0.0.1:1")
    try:
        with pytest.raises(PresenceError):
            session.start()
    finally:
        session.stop()


def test_slack_missing_cookie_is_auth_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    monkeypatch.delenv(_COOKIE_ENV, raising=False)
    no_cookie_env = MessengerConfig(platform="slack", token_env=_TOKEN_ENV)
    with pytest.raises(PresenceAuthError):
        PresenceSession(no_cookie_env).start()
    empty_cookie = MessengerConfig(
        platform="slack", token_env=_TOKEN_ENV, cookie_env=_COOKIE_ENV
    )
    with pytest.raises(PresenceAuthError):
        PresenceSession(empty_cookie).start()
    assert _presence_threads() == []


def test_slack_rejects_non_slack_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    monkeypatch.setenv(_COOKIE_ENV, _COOKIE)
    config = MessengerConfig(platform="slack", token_env=_TOKEN_ENV, cookie_env=_COOKIE_ENV)
    with _HttpServer(_slack_responder("wss://evil.example/websocket")) as http_server:
        session = PresenceSession(config, HttpClient(), slack_base_url=http_server.base_url)
        session.start()
        assert session.wait_ready(_READY_TIMEOUT) is False
        session.stop()
    assert session.phase == "failed"
    assert session.error is not None
    assert "Slack" in session.error
    assert _presence_threads() == []


def test_slack_rejected_websocket_url_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_TOKEN_ENV, _TOKEN)
    monkeypatch.setenv(_COOKIE_ENV, _COOKIE)

    def responder(method: str, path: str) -> tuple[int, dict[str, object]]:
        return 200, {"ok": False, "error": "invalid_auth"}

    config = MessengerConfig(platform="slack", token_env=_TOKEN_ENV, cookie_env=_COOKIE_ENV)
    with _HttpServer(responder) as http_server:
        session = PresenceSession(config, HttpClient(), slack_base_url=http_server.base_url)
        session.start()
        assert session.wait_ready(_READY_TIMEOUT) is False
        session.stop()
    assert session.phase == "failed"
    assert session.error is not None
    assert _TOKEN not in session.error
    assert _presence_threads() == []
