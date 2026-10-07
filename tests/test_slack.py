"""Wire-level tests for ultra_agent.slack against a real loopback HTTP server.

Every test drives the production :class:`~ultra_agent.slack.SlackTransport`
against a ``ThreadingHTTPServer`` bound to 127.0.0.1 with a real
:class:`~ultra_agent.http.HttpClient`, so authorization headers, JSON bodies,
strict ``ok`` parsing and pagination follow the true urllib path. Handlers are
plain closures over recorded requests, so synchronization never depends on
sleeps.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from ultra_agent.http import HttpClient
from ultra_agent.models import Delivery, MessengerConfig, ThreadRef
from ultra_agent.slack import SlackError, SlackTransport, UnsupportedCapabilityError
from ultra_agent.store import Store

_TOKEN = "fixture-token"
_COOKIE = "fixture-cookie"

_DEFAULTS: dict[str, Any] = {
    "auth.test": {"ok": True, "user_id": "USELF"},
    "conversations.history": {"ok": True, "messages": []},
    "conversations.replies": {"ok": True, "messages": []},
    "subscriptions.thread.getView": {"ok": True, "threads": []},
}

Route = Callable[[str, dict[str, Any]], Any]


def _make_router(overrides: dict[str, Any]) -> Route:
    def route(method: str, body: dict[str, Any]) -> Any:
        handler = overrides.get(method)
        if handler is None:
            return dict(_DEFAULTS.get(method, {"ok": True, "messages": []}))
        return handler(body) if callable(handler) else handler

    return route


class _SlackServer:
    def __init__(self, route: Route) -> None:
        self._route = route
        self.records: list[dict[str, Any]] = []
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(
            target=self.httpd.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        server = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                server._dispatch(self)

            def do_GET(self) -> None:
                server._dispatch(self)

            def log_message(self, format: str, *args: object) -> None:
                pass

        return _Handler

    def _dispatch(self, handler: BaseHTTPRequestHandler) -> None:
        length = int(handler.headers.get("Content-Length") or "0")
        raw = handler.rfile.read(length) if length else b""
        parsed = json.loads(raw) if raw else {}
        self.records.append(
            {
                "method": handler.command,
                "path": handler.path,
                "headers": {key.lower(): value for key, value in handler.headers.items()},
                "body": parsed,
            }
        )
        method = handler.path.rsplit("/", 1)[-1]
        try:
            result = self._route(method, parsed)
            data = json.dumps(result).encode()
            status = 200
        except Exception:
            data = b"{}"
            status = 500
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)

    @property
    def base_url(self) -> str:
        host, port = self.httpd.server_address[0], self.httpd.server_address[1]
        return f"http://{host}:{port}"

    def calls(self, method: str) -> list[dict[str, Any]]:
        suffix = f"/{method}"
        return [record for record in self.records if record["path"].endswith(suffix)]

    def __enter__(self) -> _SlackServer:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


def _config(**overrides: Any) -> MessengerConfig:
    values: dict[str, Any] = {
        "platform": "slack",
        "token_env": "SLACK_TOKEN",
        "users": ("U1",),
        "channels": ("C1",),
        "cookie_env": None,
        "threads": (),
    }
    values.update(overrides)
    return MessengerConfig(**values)


def _transport(server: _SlackServer, store: Store, config: MessengerConfig) -> SlackTransport:
    return SlackTransport(config, store, http=HttpClient(), base_url=server.base_url)


def _store(tmp_path: Path) -> Store:
    return Store(tmp_path / "runtime.db")


def test_send_threads_reply_and_tracks_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    route = _make_router({"chat.postMessage": {"ok": True, "channel": "C1", "ts": "2.1"}})
    store = _store(tmp_path)
    with _SlackServer(route) as server:
        transport = _transport(server, store, _config())
        ts = transport.send(Delivery("fixture", "C1", "hello", reply_to="1.1"))
        record = server.calls("chat.postMessage")[0]
    assert ts == "2.1"
    assert record["body"] == {"channel": "C1", "text": "hello", "thread_ts": "1.1"}
    assert record["headers"]["authorization"] == f"Bearer {_TOKEN}"
    assert record["headers"]["content-type"].startswith("application/json")
    assert "cookie" not in record["headers"]
    assert store.get_checkpoint("slack:threads:C1") == json.dumps(["1.1"])


def test_send_without_thread_tracks_own_timestamp(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    route = _make_router({"chat.postMessage": {"ok": True, "channel": "C1", "ts": "2.2"}})
    store = _store(tmp_path)
    with _SlackServer(route) as server:
        transport = _transport(server, store, _config())
        ts = transport.send(Delivery("fixture", "C1", "hi"))
        record = server.calls("chat.postMessage")[0]
    assert ts == "2.2"
    assert record["body"] == {"channel": "C1", "text": "hi"}
    assert store.get_checkpoint("slack:threads:C1") == json.dumps(["2.2"])


def test_edit_calls_chat_update(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    route = _make_router({"chat.update": {"ok": True, "ts": "2.1", "channel": "C1"}})
    store = _store(tmp_path)
    with _SlackServer(route) as server:
        transport = _transport(server, store, _config())
        assert transport.edit("C1", "2.1", "edited") == "2.1"
        record = server.calls("chat.update")[0]
    assert record["body"] == {"channel": "C1", "ts": "2.1", "text": "edited"}


@pytest.mark.parametrize("method", [
    "conversations.history", "conversations.replies",
    "subscriptions.thread.getView", "chat.update",
])
def test_missing_required_slack_payload_is_an_error(tmp_path, monkeypatch, method):
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    monkeypatch.setenv("SLACK_COOKIE", "fixture-cookie")
    route = _make_router({method: {"ok": True}})
    store = _store(tmp_path)
    config = _config(
        cookie_env="SLACK_COOKIE",
        threads=(ThreadRef(channel_id="C1", thread_id="1.0"),),
    )
    with _SlackServer(route) as server:
        transport = _transport(server, store, config)
        with pytest.raises(SlackError):
            if method == "chat.update":
                transport.edit("C1", "2.1", "edited")
            else:
                transport.poll()
    if method == "conversations.history":
        assert store.get_checkpoint("slack:history:C1") is None
    assert store.list_jobs() == []


def test_typing_is_explicitly_unsupported(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    store = _store(tmp_path)
    transport = SlackTransport(_config(), store, http=HttpClient(), base_url="http://127.0.0.1:1")
    with pytest.raises(UnsupportedCapabilityError):
        transport.typing("C1")
    with pytest.raises(UnsupportedCapabilityError):
        transport.typing("C1", thread_id="1.1")


def test_disabled_transport_does_not_poll_network(tmp_path: Path) -> None:
    with _SlackServer(_make_router({})) as server:
        transport = _transport(server, _store(tmp_path), _config(enabled=False))
        assert transport.poll() == []
        assert server.records == []


def test_explicit_thread_cannot_bypass_channel_allowlist(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    config = _config(threads=(ThreadRef(channel_id="C2", thread_id="1.0"),))
    with _SlackServer(_make_router({})) as server:
        _transport(server, _store(tmp_path), config).poll()
        assert server.calls("conversations.replies") == []


def test_remote_endpoint_cannot_receive_credentials(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        SlackTransport(_config(), _store(tmp_path), base_url="https://example.invalid/api")


def test_untagged_thread_reply_is_enqueued(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    replies = {
        "ok": True,
        "messages": [
            {"ts": "1.0", "user": "U1", "text": "root"},
            {"ts": "1.1", "user": "U1", "text": "ping without a mention", "thread_ts": "1.0"},
        ],
    }
    route = _make_router({"conversations.replies": replies})
    store = _store(tmp_path)
    config = _config(threads=(ThreadRef(channel_id="C1", thread_id="1.0"),))
    with _SlackServer(route) as server:
        transport = _transport(server, store, config)
        job_ids = transport.poll()
        record = server.calls("conversations.replies")[0]
    assert len(job_ids) == 1
    jobs = store.list_jobs()
    assert len(jobs) == 1
    assert jobs[0].source_key == "slack:C1:1.1"
    assert jobs[0].channel_id == "C1"
    assert jobs[0].thread_id == "1.0"
    assert jobs[0].reply_to == "1.1"
    assert jobs[0].platform == "slack"
    assert jobs[0].approved is False
    assert record["body"]["channel"] == "C1"
    assert record["body"]["ts"] == "1.0"
    assert record["body"]["oldest"] == "1.0"


def test_self_unauthorized_and_bot_messages_ignored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    replies = {
        "ok": True,
        "messages": [
            {"ts": "1.1", "user": "U1", "text": "allowed", "thread_ts": "1.0"},
            {"ts": "1.2", "user": "USELF", "text": "self", "thread_ts": "1.0"},
            {"ts": "1.3", "user": "U2", "text": "stranger", "thread_ts": "1.0"},
            {"ts": "1.4", "user": "U1", "text": "bot", "thread_ts": "1.0", "bot_id": "B1"},
        ],
    }
    route = _make_router({"conversations.replies": replies})
    store = _store(tmp_path)
    config = _config(threads=(ThreadRef(channel_id="C1", thread_id="1.0"),))
    with _SlackServer(route) as server:
        transport = _transport(server, store, config)
        transport.poll()
    jobs = store.list_jobs()
    assert [job.source_key for job in jobs] == ["slack:C1:1.1"]


def test_cursor_restart_does_not_replay(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    pages = iter(
        [
            {
                "ok": True,
                "messages": [{"ts": "1.1", "user": "U1", "text": "a", "thread_ts": "1.0"}],
            },
            {
                "ok": True,
                "messages": [
                    {"ts": "1.1", "user": "U1", "text": "a", "thread_ts": "1.0"},
                    {"ts": "1.2", "user": "U1", "text": "b", "thread_ts": "1.0"},
                ],
            },
        ]
    )
    route = _make_router({"conversations.replies": lambda body: next(pages)})
    store = _store(tmp_path)
    config = _config(threads=(ThreadRef(channel_id="C1", thread_id="1.0"),))
    with _SlackServer(route) as server:
        transport = _transport(server, store, config)
        first = transport.poll()
        second = transport.poll()
    assert len(first) == 1
    assert len(second) == 1
    jobs = store.list_jobs()
    assert [job.source_key for job in jobs] == ["slack:C1:1.1", "slack:C1:1.2"]
    assert store.get_checkpoint("slack:thread:C1:1.0") == "1.2"


def test_history_baseline_then_incremental(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    pages = iter(
        [
            {"ok": True, "messages": [{"ts": "1.5", "user": "U1", "text": "old"}]},
            {
                "ok": True,
                "messages": [
                    {"ts": "1.4", "user": "U1", "text": "older"},
                    {"ts": "1.5", "user": "U1", "text": "old"},
                    {"ts": "1.6", "user": "U1", "text": "new"},
                ],
            },
        ]
    )
    route = _make_router({"conversations.history": lambda body: next(pages)})
    store = _store(tmp_path)
    with _SlackServer(route) as server:
        transport = _transport(server, store, _config())
        assert transport.poll() == []
        assert store.get_checkpoint("slack:history:C1") == "1.5"
        enqueued = transport.poll()
    assert len(enqueued) == 1
    jobs = store.list_jobs()
    assert [job.source_key for job in jobs] == ["slack:C1:1.6"]
    assert store.get_checkpoint("slack:history:C1") == "1.6"


def test_rejected_api_raises_without_leaking_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    route = _make_router({"chat.postMessage": {"ok": False, "error": "channel_not_found"}})
    store = _store(tmp_path)
    with _SlackServer(route) as server:
        transport = _transport(server, store, _config())
        with pytest.raises(SlackError) as info:
            transport.send(Delivery("fixture", "C1", "hi"))
    assert "channel_not_found" in str(info.value)

    leaky = _make_router({"chat.postMessage": {"ok": False, "error": f"xoxb-{_TOKEN}"}})
    with _SlackServer(leaky) as server:
        transport = _transport(server, store, _config())
        with pytest.raises(SlackError) as info:
            transport.send(Delivery("fixture", "C1", "hi"))
    rendered = f"{info.value!s} {info.value!r} {info.value.args!r}"
    assert _TOKEN not in rendered


def test_malformed_response_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    store = _store(tmp_path)
    with _SlackServer(_make_router({"chat.postMessage": ["not", "an", "object"]})) as server:
        transport = _transport(server, store, _config())
        with pytest.raises(SlackError):
            transport.send(Delivery("fixture", "C1", "hi"))
    with _SlackServer(_make_router({"chat.postMessage": {"ts": "2.1"}})) as server:
        transport = _transport(server, store, _config())
        with pytest.raises(SlackError):
            transport.send(Delivery("fixture", "C1", "hi"))


def test_pagination_follows_next_cursor(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)

    def history(body: dict[str, Any]) -> dict[str, Any]:
        if body.get("cursor") == "c2":
            return {"ok": True, "messages": [{"ts": "1.2", "user": "U1", "text": "b"}]}
        return {
            "ok": True,
            "messages": [{"ts": "1.1", "user": "U1", "text": "a"}],
            "response_metadata": {"next_cursor": "c2"},
        }

    route = _make_router({"conversations.history": history})
    store = _store(tmp_path)
    store.set_checkpoint("slack:history:C1", "1.0")
    with _SlackServer(route) as server:
        transport = _transport(server, store, _config())
        enqueued = transport.poll()
        history_calls = server.calls("conversations.history")
        followups = [call for call in history_calls if call["body"].get("cursor")]
    assert len(enqueued) == 2
    jobs = store.list_jobs()
    assert [job.source_key for job in jobs] == ["slack:C1:1.1", "slack:C1:1.2"]
    assert followups[0]["body"]["cursor"] == "c2"


def test_repeated_pagination_cursor_fails_visibly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)

    def history(body: dict[str, Any]) -> dict[str, Any]:
        return {
            "ok": True,
            "messages": [{"ts": "1.1", "user": "U1", "text": "a"}],
            "response_metadata": {"next_cursor": "c2"},
        }

    route = _make_router({"conversations.history": history})
    store = _store(tmp_path)
    store.set_checkpoint("slack:history:C1", "1.0")
    with _SlackServer(route) as server:
        transport = _transport(server, store, _config())
        with pytest.raises(SlackError):
            transport.poll()
    assert store.list_jobs() == []


def test_cookie_enables_thread_discovery(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    monkeypatch.setenv("SLACK_COOKIE", _COOKIE)
    view = {
        "ok": True,
        "threads": [{"root_msg": {"channel": "C1", "ts": "3.0"}}],
    }
    replies = {
        "ok": True,
        "messages": [{"ts": "3.1", "user": "U1", "text": "hey", "thread_ts": "3.0"}],
    }
    route = _make_router(
        {"subscriptions.thread.getView": view, "conversations.replies": replies}
    )
    store = _store(tmp_path)
    config = _config(cookie_env="SLACK_COOKIE")
    with _SlackServer(route) as server:
        transport = _transport(server, store, config)
        enqueued = transport.poll()
        view_call = server.calls("subscriptions.thread.getView")[0]
    assert len(enqueued) == 1
    assert store.list_jobs()[0].source_key == "slack:C1:3.1"
    assert view_call["headers"]["cookie"] == f"d={_COOKIE}"
    assert json.loads(store.get_checkpoint("slack:threads:C1") or "[]") == ["3.0"]


def test_discovery_failure_is_not_faked(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    monkeypatch.setenv("SLACK_COOKIE", _COOKIE)
    route = _make_router(
        {"subscriptions.thread.getView": {"ok": False, "error": "not_allowed"}}
    )
    store = _store(tmp_path)
    config = _config(cookie_env="SLACK_COOKIE")
    with _SlackServer(route) as server:
        transport = _transport(server, store, config)
        with pytest.raises(SlackError):
            transport.poll()
    assert store.list_jobs() == []


def test_decimal_timestamps_stay_exact(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SLACK_TOKEN", _TOKEN)
    assert float("10000000000.000001") == float("10000000000.000002")
    route = _make_router(
        {
            "conversations.history": {
                "ok": True,
                "messages": [{"ts": "10000000000.000002", "user": "U1", "text": "precise"}],
            }
        }
    )
    store = _store(tmp_path)
    store.set_checkpoint("slack:history:C1", "10000000000.000001")
    with _SlackServer(route) as server:
        transport = _transport(server, store, _config())
        enqueued = transport.poll()
    assert len(enqueued) == 1
    assert store.list_jobs()[0].source_key == "slack:C1:10000000000.000002"


def test_missing_token_environment_is_a_safe_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    route = _make_router({})
    store = _store(tmp_path)
    with _SlackServer(route) as server:
        transport = _transport(server, store, _config())
        with pytest.raises(SlackError):
            transport.identity()
    assert server.records == []
