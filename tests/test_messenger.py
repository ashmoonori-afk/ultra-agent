"""Wire-level tests for ultra_agent.messenger.

Dispatch is exercised two ways: against real loopback ``ThreadingHTTPServer``
fixtures with the production adapters and the real ``urllib`` path, and with an
injected ``Transport`` map for aggregation, ordering and capability rules.
Delivery state is read back from a real SQLite ``Store`` so an ambiguous or
rate-limited send is asserted from durable state, not from an in-memory guess.
No test sleeps or polls; a fake clock advances injected time explicitly.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

from ultra_agent.messenger import CapabilityError, Messenger, MessengerError, PlatformStatus
from ultra_agent.models import Config, Delivery, MessengerConfig, Platform, ThreadRef
from ultra_agent.presence import PresenceSession
from ultra_agent.store import Store

_DISCORD_TOKEN_ENV = "ULTRA_MESSENGER_DISCORD_TOKEN"
_SLACK_TOKEN_ENV = "ULTRA_MESSENGER_SLACK_TOKEN"
_TELEGRAM_TOKEN_ENV = "ULTRA_MESSENGER_TELEGRAM_TOKEN"
_TOKEN = "fixture-token-value"

Response = tuple[int, Any, Sequence[tuple[str, str]]]
Route = Callable[[str, str, Any], Response]


class _Server:
    """A loopback HTTP server backed by a route callable, recording requests."""

    def __init__(self, route: Route) -> None:
        self._route = route
        self.records: list[dict[str, Any]] = []
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(
            target=self.httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        server = self

        class _Handler(BaseHTTPRequestHandler):
            def _dispatch(self) -> None:
                length = int(self.headers.get("Content-Length") or "0")
                raw = self.rfile.read(length) if length else b""
                body = json.loads(raw.decode("utf-8")) if raw else None
                server.records.append(
                    {
                        "method": self.command,
                        "path": self.path,
                        "body": body,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                    }
                )
                status, payload, extra = server._route(self.command, self.path, body)
                data = b"" if payload is None else json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for name, value in extra:
                    self.send_header(name, value)
                self.end_headers()
                if data:
                    self.wfile.write(data)

            do_GET = _dispatch
            do_POST = _dispatch
            do_PATCH = _dispatch

            def log_message(self, format: str, *args: object) -> None:
                pass

        return _Handler

    @property
    def base_url(self) -> str:
        host, port = self.httpd.server_address[0], self.httpd.server_address[1]
        return f"http://{host}:{port}"

    def calls(self, method: str) -> list[dict[str, Any]]:
        return [record for record in self.records if record["method"] == method]

    def path_calls(self, suffix: str) -> list[dict[str, Any]]:
        return [record for record in self.records if record["path"].endswith(suffix)]

    def __enter__(self) -> _Server:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


class _FakeTransport:
    """A recording transport used where the real wire path is not under test."""

    def __init__(self, poll_ids: Sequence[str] = ()) -> None:
        self._poll_ids = list(poll_ids)
        self.sent: list[Delivery] = []
        self.edits: list[tuple[str, str, str]] = []
        self.typings: list[tuple[str, str | None]] = []
        self.identity_calls = 0

    def identity(self) -> str:
        self.identity_calls += 1
        return "self"

    def poll(self) -> list[str]:
        return list(self._poll_ids)

    def send(self, delivery: Delivery, /) -> str:
        self.sent.append(delivery)
        return f"remote-{len(self.sent)}"

    def edit(self, channel: str, message_id: str, text: str, /) -> str:
        self.edits.append((channel, message_id, text))
        return message_id

    def typing(self, channel: str, /, thread_id: str | None = None) -> None:
        self.typings.append((channel, thread_id))


def _messenger_config(platform: Platform, **overrides: Any) -> MessengerConfig:
    values: dict[str, Any] = {
        "platform": platform,
        "token_env": {
            "discord": _DISCORD_TOKEN_ENV,
            "slack": _SLACK_TOKEN_ENV,
            "telegram": _TELEGRAM_TOKEN_ENV,
        }[platform],
        "users": ("U1",),
        "channels": (),
    }
    values.update(overrides)
    return MessengerConfig(**values)


def _config(workspace: Path, *messengers: MessengerConfig) -> Config:
    return Config(workspace=workspace, messengers=tuple(messengers))


def _store(tmp_path: Path) -> Store:
    return Store(tmp_path / "runtime.db")


def _discord_router(method: str, path: str, body: Any) -> Response:
    parts = urlsplit(path)
    if parts.path == "/users/@me":
        return 200, {"bot": True, "id": "7"}, ()
    if method == "GET" and parts.path.endswith("/messages"):
        return 200, [], ()
    if method == "POST" and parts.path.endswith("/messages"):
        return 200, {"id": "1000"}, ()
    if method == "POST" and parts.path.endswith("/typing"):
        return 204, None, ()
    if method == "PATCH" and "/messages/" in parts.path:
        return 200, {"id": parts.path.rsplit("/", 1)[-1]}, ()
    return 404, {"error": "no route"}, ()


def _slack_router(method: str, path: str, body: Any) -> Response:
    name = urlsplit(path).path.rsplit("/", 1)[-1]
    if name == "auth.test":
        return 200, {"ok": True, "user_id": "USELF"}, ()
    if name == "chat.postMessage":
        channel = body.get("channel", "C1") if isinstance(body, dict) else "C1"
        return 200, {"ok": True, "channel": channel, "ts": "1.5"}, ()
    if name == "chat.update":
        ts = body.get("ts", "1.5") if isinstance(body, dict) else "1.5"
        return 200, {"ok": True, "ts": ts}, ()
    if name in ("conversations.history", "conversations.replies"):
        return 200, {"ok": True, "messages": []}, ()
    if name == "subscriptions.thread.getView":
        return 200, {"ok": True, "threads": []}, ()
    return 200, {"ok": True}, ()


def _telegram_router(method: str, path: str, body: Any) -> Response:
    name = urlsplit(path).path.rsplit("/", 1)[-1]
    if name == "getMe":
        return 200, {"ok": True, "result": {"is_bot": True, "id": 123}}, ()
    if name == "sendMessage":
        return 200, {"ok": True, "result": {"message_id": 55}}, ()
    if name == "editMessageText":
        message_id = body.get("message_id") if isinstance(body, dict) else None
        return 200, {"ok": True, "result": {"message_id": message_id}}, ()
    if name == "sendChatAction":
        return 200, {"ok": True, "result": True}, ()
    if name == "getUpdates":
        return 200, {"ok": True, "result": []}, ()
    return 200, {"ok": True, "result": True}, ()


def test_discord_send_and_drain_over_wire(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_DISCORD_TOKEN_ENV, _TOKEN)
    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("discord", channels=("10",)))
    with _Server(_discord_router) as server:
        messenger = Messenger(config, store, base_urls={"discord": server.base_url})
        ids = messenger.send("discord", "10", "hello", reply_to="9")
        assert len(ids) == 1
        assert messenger.drain() == ids
        post = server.path_calls("/messages")[0]
    assert post["headers"]["authorization"] == f"Bot {_TOKEN}"
    assert post["body"]["content"] == "hello"
    assert post["body"]["message_reference"] == {"message_id": "9"}
    delivery = store.deliveries()[0]
    assert delivery.status == "sent"
    assert delivery.remote_id == "1000"


def test_slack_send_and_drain_over_wire(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SLACK_TOKEN_ENV, _TOKEN)
    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("slack", channels=("C1",)))
    with _Server(_slack_router) as server:
        messenger = Messenger(config, store, base_urls={"slack": server.base_url})
        ids = messenger.send("slack", "C1", "hello", thread_id="1.0")
        assert messenger.drain() == ids
        post = server.path_calls("chat.postMessage")[0]
    assert post["headers"]["authorization"] == f"Bearer {_TOKEN}"
    assert post["body"] == {"channel": "C1", "text": "hello", "thread_ts": "1.0"}
    delivery = store.deliveries()[0]
    assert delivery.status == "sent"
    assert delivery.remote_id == "1.5"


def test_telegram_send_and_drain_over_wire(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_TELEGRAM_TOKEN_ENV, _TOKEN)
    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("telegram", channels=("123",)))
    with _Server(_telegram_router) as server:
        messenger = Messenger(config, store, base_urls={"telegram": server.base_url})
        ids = messenger.send("telegram", "123", "hello", thread_id="5", reply_to="4")
        assert messenger.drain() == ids
        post = server.path_calls("/sendMessage")[0]
    assert post["body"]["chat_id"] == 123
    assert post["body"]["message_thread_id"] == 5
    assert post["body"]["reply_parameters"] == {"message_id": 4}
    assert f"bot{_TOKEN}" in post["path"]
    delivery = store.deliveries()[0]
    assert delivery.status == "sent"
    assert delivery.remote_id == "55"


def test_poll_dispatches_each_enabled_platform_over_wire(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_DISCORD_TOKEN_ENV, _TOKEN)
    monkeypatch.setenv(_SLACK_TOKEN_ENV, _TOKEN)
    monkeypatch.setenv(_TELEGRAM_TOKEN_ENV, _TOKEN)
    store = _store(tmp_path)
    config = _config(
        tmp_path,
        _messenger_config("discord", channels=("10",)),
        _messenger_config("slack", channels=("C1",)),
        _messenger_config("telegram", channels=("123",)),
    )
    with _Server(_discord_router) as discord, _Server(_slack_router) as slack, _Server(
        _telegram_router
    ) as telegram:
        messenger = Messenger(
            config,
            store,
            base_urls={
                "discord": discord.base_url,
                "slack": slack.base_url,
                "telegram": telegram.base_url,
            },
        )
        assert messenger.poll() == []
    assert discord.path_calls("/users/@me")
    assert slack.path_calls("auth.test")
    assert telegram.path_calls("/getMe")


def test_send_chunks_into_one_ordered_group(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("discord", channels=("10",)))
    messenger = Messenger(config, store)
    ids = messenger.send("discord", "10", "x" * 4000)
    assert len(ids) == 3
    deliveries = store.deliveries()
    assert [d.ordinal for d in deliveries] == [0, 1, 2]
    assert len({d.job_id for d in deliveries}) == 1
    assert all(d.status == "pending" for d in deliveries)


def test_adapter_never_falls_back_to_another_platform(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("discord", channels=("10",)))
    messenger = Messenger(config, store)
    with pytest.raises(MessengerError):
        messenger.adapter("telegram")
    disabled = _config(
        tmp_path, _messenger_config("telegram", channels=("123",), enabled=False)
    )
    with pytest.raises(MessengerError):
        Messenger(disabled, store).adapter("telegram")


def test_send_rejects_disallowed_channel(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("discord", channels=("10",)))
    messenger = Messenger(config, store)
    with pytest.raises(MessengerError):
        messenger.send("discord", "99", "hello")
    assert store.deliveries() == []


def test_ambiguous_send_becomes_uncertain_and_is_not_resent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_DISCORD_TOKEN_ENV, _TOKEN)

    def router(method: str, path: str, body: Any) -> Response:
        return 500, {"error": "server"}, ()

    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("discord", channels=("10",)))
    with _Server(router) as server:
        messenger = Messenger(config, store, base_urls={"discord": server.base_url})
        messenger.send("discord", "10", "hello")
        assert messenger.drain() == []
        assert messenger.drain() == []
        posts = server.calls("POST")
    assert len(posts) == 1
    assert store.deliveries()[0].status == "uncertain"


def test_unexpected_adapter_response_becomes_uncertain(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_DISCORD_TOKEN_ENV, _TOKEN)

    def router(method: str, path: str, body: Any) -> Response:
        return 200, ["not", "an", "object"], ()

    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("discord", channels=("10",)))
    with _Server(router) as server:
        messenger = Messenger(config, store, base_urls={"discord": server.base_url})
        ids = messenger.send("discord", "10", "hello")
        assert messenger.drain() == []
    assert store.deliveries()[0].status == "uncertain"
    assert store.deliveries()[0].id in ids


def test_rate_limit_defers_then_respects_persistent_backoff(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SLACK_TOKEN_ENV, _TOKEN)
    attempts: list[int] = []

    def router(method: str, path: str, body: Any) -> Response:
        attempts.append(1)
        if len(attempts) == 1:
            return 429, {"ok": False}, (("Retry-After", "30"),)
        return 200, {"ok": True, "channel": "C1", "ts": "1.5"}, ()

    now = [1000.0]
    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("slack", channels=("C1",)))
    with _Server(router) as server:
        messenger = Messenger(
            config,
            store,
            base_urls={"slack": server.base_url},
            clock=lambda: now[0],
        )
        messenger.send("slack", "C1", "hello")
        assert messenger.drain() == []
        assert store.deliveries()[0].status == "pending"
        assert len(attempts) == 1
        assert messenger.drain() == []
        assert len(attempts) == 1
        now[0] = 1031.0
        sent = messenger.drain()
    assert len(attempts) == 2
    assert sent == [store.deliveries()[0].id]
    assert store.deliveries()[0].status == "sent"


def test_drain_skips_unconfigured_and_disabled_platforms(tmp_path: Path) -> None:
    store = _store(tmp_path)
    none_id = store.add_delivery(None, "anywhere", "text")
    disabled_id = store.add_delivery("telegram", "123", "text")
    config = _config(
        tmp_path, _messenger_config("telegram", channels=("123",), enabled=False)
    )
    messenger = Messenger(config, store)
    assert messenger.drain() == []
    by_id = {delivery.id: delivery for delivery in store.deliveries()}
    assert by_id[none_id].status == "pending"
    assert by_id[disabled_id].status == "pending"


def test_poll_aggregates_enabled_platforms_in_config_order(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = _config(
        tmp_path,
        _messenger_config("discord", channels=("10",), enabled=False),
        _messenger_config("slack", channels=("C1",)),
        _messenger_config("telegram", channels=("123",)),
    )
    slack = _FakeTransport(["slack-job"])
    telegram = _FakeTransport(["telegram-job"])
    messenger = Messenger(config, store, transports={"slack": slack, "telegram": telegram})
    assert messenger.poll() == ["slack-job", "telegram-job"]
    assert messenger.poll("telegram") == ["telegram-job"]


def test_edit_and_typing_dispatch_to_the_right_platform(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = _config(
        tmp_path,
        _messenger_config("discord", channels=("10",)),
        _messenger_config("telegram", channels=("123",)),
    )
    discord = _FakeTransport()
    telegram = _FakeTransport()
    messenger = Messenger(config, store, transports={"discord": discord, "telegram": telegram})
    assert messenger.edit("telegram", "123", "55", "edited") == "55"
    messenger.typing("discord", "10", thread_id="77")
    assert telegram.edits == [("123", "55", "edited")]
    assert discord.typings == [("10", "77")]
    assert telegram.typings == []


def test_slack_typing_is_a_capability_error(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("slack", channels=("C1",)))
    messenger = Messenger(config, store, transports={"slack": _FakeTransport()})
    with pytest.raises(CapabilityError):
        messenger.typing("slack", "C1")


def test_telegram_presence_is_a_capability_error(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("telegram", channels=("123",)))
    messenger = Messenger(config, store)
    with pytest.raises(CapabilityError):
        messenger.presence("telegram")


def test_presence_returns_existing_session_context_manager(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = _config(
        tmp_path,
        _messenger_config("discord", channels=("10",)),
        _messenger_config("slack", channels=("C1",), cookie_env="ULTRA_MESSENGER_COOKIE"),
    )
    messenger = Messenger(
        config,
        store,
        discord_gateway_url="ws://127.0.0.1:1",
        slack_presence_base_url="http://127.0.0.1:1",
    )
    for platform in ("discord", "slack"):
        session = messenger.presence(platform)
        assert isinstance(session, PresenceSession)
        assert session.phase == "idle"


def test_doctor_exposes_configuration_without_secrets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_DISCORD_TOKEN_ENV, _TOKEN)
    store = _store(tmp_path)
    config = _config(
        tmp_path,
        _messenger_config(
            "discord",
            channels=("10",),
            threads=(ThreadRef(channel_id="10", thread_id="77"),),
        ),
        _messenger_config("telegram", channels=("123",), enabled=False),
    )
    statuses = Messenger(config, store).doctor()
    assert [status.platform for status in statuses] == ["discord", "telegram"]
    discord = statuses[0]
    assert isinstance(discord, PlatformStatus)
    assert discord.enabled is True
    assert discord.typing is True
    assert discord.presence is True
    assert discord.token_env == _DISCORD_TOKEN_ENV
    assert discord.channels == ("10",)
    assert discord.threads == (ThreadRef(channel_id="10", thread_id="77"),)
    assert statuses[1].enabled is False
    assert statuses[1].presence is False
    assert _TOKEN not in repr(statuses)


def test_identity_delegates_to_adapter(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = _config(tmp_path, _messenger_config("discord", channels=("10",)))
    fake = _FakeTransport()
    messenger = Messenger(config, store, transports={"discord": fake})
    assert messenger.identity("discord") == "self"
    assert fake.identity_calls == 1
