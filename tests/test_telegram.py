"""Wire-level tests for ultra_agent.telegram against a real loopback server.

Every test drives the production :class:`TelegramTransport` through the real
:class:`HttpClient` and urllib path against a ``ThreadingHTTPServer`` bound to
127.0.0.1 that mimics the Telegram Bot API. Requests are recorded and the
served pages are scripted, so synchronization never depends on sleeps, and a
fresh :class:`Store` over the same SQLite file exercises a real restart.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import pytest

from ultra_agent.http import HttpClient, TransportError
from ultra_agent.models import Delivery, Job, MessengerConfig
from ultra_agent.store import Store
from ultra_agent.telegram import (
    POLL_LIMIT,
    TELEGRAM_OFFSET_KEY,
    TelegramError,
    TelegramTransport,
)

TOKEN = "123456:SECRET-token"
USER = "100"
OTHER_USER = "999"
CHAT = "-1000"
OTHER_CHAT = "-2000"
THREAD = "77"


@pytest.fixture(autouse=True)
def _telegram_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TELEGRAM_TOKEN", TOKEN)


def _config(
    workspace: Path,
    users: tuple[str, ...] = (USER,),
    channels: tuple[str, ...] = (CHAT,),
) -> MessengerConfig:
    return MessengerConfig(
        platform="telegram",
        token_env="TELEGRAM_TOKEN",
        users=users,
        channels=channels,
    )


def _delivery(
    text: str, thread_id: str | None = None, reply_to: str | None = None
) -> Delivery:
    return Delivery(
        id="fixture", platform="telegram", channel_id=CHAT, content=text,
        thread_id=thread_id, reply_to=reply_to,
    )


@dataclass
class _Recorded:
    verb: str
    method: str
    query: dict[str, str]
    body: Any


@dataclass
class _Api:
    """Scripted Telegram Bot API surface plus recorded requests."""

    records: list[_Recorded] = field(default_factory=list)
    updates_pages: list[list[Any]] = field(default_factory=list)
    getme: Any = field(
        default_factory=lambda: {
            "ok": True,
            "result": {"id": 42, "is_bot": True, "username": "unit-bot"},
        }
    )
    send: Any = field(default_factory=lambda: {"ok": True, "result": {"message_id": 9001}})
    edit: Any = field(default_factory=lambda: {"ok": True, "result": {"message_id": 9002}})
    action: Any = field(default_factory=lambda: {"ok": True, "result": True})
    get_updates_response: tuple[int, Any] | None = None
    method_errors: dict[str, tuple[int, Any]] = field(default_factory=dict)

    def respond(self, method: str, body: Any) -> tuple[int, Any]:
        if method in self.method_errors:
            return self.method_errors[method]
        if method == "getUpdates":
            if self.get_updates_response is not None:
                return self.get_updates_response
            page = self.updates_pages.pop(0) if self.updates_pages else []
            return 200, {"ok": True, "result": page}
        if method == "getMe":
            return 200, self.getme
        if method == "sendMessage":
            return 200, self.send
        if method == "editMessageText":
            return 200, self.edit
        if method == "sendChatAction":
            return 200, self.action
        return 404, {"ok": False, "error_code": 404, "description": "unknown method"}


def _make_handler(api: _Api) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._dispatch()

        def do_POST(self) -> None:
            self._dispatch()

        def _dispatch(self) -> None:
            length = int(self.headers.get("Content-Length") or "0")
            raw = self.rfile.read(length) if length else b""
            parsed = urlsplit(self.path)
            method = parsed.path.rsplit("/", 1)[-1]
            query = dict(parse_qsl(parsed.query))
            body = json.loads(raw) if raw else None
            assert parsed.path.split("/")[1] == f"bot{TOKEN}"
            api.records.append(
                _Recorded(verb=self.command, method=method, query=query, body=body)
            )
            status, payload = api.respond(method, body)
            data = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: object) -> None:
            pass

    return _Handler


class _Server:
    def __init__(self, api: _Api) -> None:
        self.api = api
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(api))
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(
            target=self.httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )

    def __enter__(self) -> _Server:
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


def _message(
    message_id: int,
    text: str = "hello",
    user: str = USER,
    chat: str = CHAT,
    is_bot: bool = False,
    thread: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "message_id": message_id,
        "from": {"id": user, "is_bot": is_bot},
        "chat": {"id": chat},
        "text": text,
    }
    if thread is not None:
        payload["message_thread_id"] = thread
    return payload


def _update(update_id: int, message: dict[str, Any]) -> dict[str, Any]:
    return {"update_id": update_id, "message": message}


def _transport(workspace: Path, store: Store, server: _Server) -> TelegramTransport:
    return TelegramTransport(_config(workspace), store, HttpClient(), base_url=server.base_url)


def _store(tmp_path: Path) -> Store:
    return Store(tmp_path / "runtime.db")


def test_poll_enqueues_and_persists_offset_across_restart(tmp_path: Path) -> None:
    api = _Api()
    api.updates_pages = [
        [
            _update(1, _message(10, "first")),
            _update(2, _message(11, "second", thread=THREAD)),
        ]
    ]
    db = tmp_path / "runtime.db"
    with _Server(api) as server:
        ids = _transport(tmp_path, Store(db), server).poll()
        accepted = [Store(db).get_job(job_id) for job_id in ids]
        assert [s.prompt for s in accepted] == ["first", "second"]
        assert accepted[0].source_key == f"telegram:{CHAT}:10"
        assert accepted[0].reply_to == "10"
        assert accepted[1].thread_id == THREAD
        updates = [record for record in api.records if record.method == "getUpdates"]
        assert updates[0].query["limit"] == str(POLL_LIMIT)
        assert "offset" not in updates[0].query

        assert Store(db).get_checkpoint(TELEGRAM_OFFSET_KEY) == "3"
        assert [job.status for job in Store(db).list_jobs()] == [
            "awaiting_approval",
            "awaiting_approval",
        ]

        restarted = _transport(tmp_path, Store(db), server)
        assert restarted.poll() == []
        assert api.records[-1].query["offset"] == "3"


def test_poll_drains_every_full_page_of_a_backlog(tmp_path: Path) -> None:
    first = [_update(i, _message(100 + i, f"m{i}")) for i in range(1, POLL_LIMIT + 1)]
    second = [
        _update(i, _message(100 + i, f"m{i}"))
        for i in range(POLL_LIMIT + 1, POLL_LIMIT + 4)
    ]
    api = _Api()
    api.updates_pages = [first, second]
    store = _store(tmp_path)
    with _Server(api) as server:
        accepted = _transport(tmp_path, store, server).poll()
    assert len(accepted) == POLL_LIMIT + 3
    assert [r.query.get("offset") for r in api.records if r.method == "getUpdates"] == [
        None, str(POLL_LIMIT + 1)
    ]
    assert store.get_checkpoint(TELEGRAM_OFFSET_KEY) == str(POLL_LIMIT + 4)


def test_unauthorized_sender_or_channel_is_ignored(tmp_path: Path) -> None:
    api = _Api()
    api.updates_pages = [
        [
            _update(5, _message(50, "unknown user", user=OTHER_USER)),
            _update(6, _message(51, "unknown chat", chat=OTHER_CHAT)),
            _update(7, _message(52, "allowed")),
        ]
    ]
    store = _store(tmp_path)
    with _Server(api) as server:
        accepted = _transport(tmp_path, store, server).poll()
    assert [store.get_job(job_id).prompt for job_id in accepted] == ["allowed"]
    assert store.get_checkpoint(TELEGRAM_OFFSET_KEY) == "8"
    assert len(store.list_jobs()) == 1


def test_thread_and_reply_are_independent_fields(tmp_path: Path) -> None:
    api = _Api()
    api.updates_pages = [[_update(1, _message(10, "topic", thread=THREAD))]]
    store = _store(tmp_path)
    with _Server(api) as server:
        transport = _transport(tmp_path, store, server)
        (job_id,) = transport.poll()
        submission = store.get_job(job_id)
        assert (submission.thread_id, submission.reply_to) == (THREAD, "10")
        remote = transport.send(_delivery("reply", thread_id=THREAD, reply_to="10"))
    assert remote == "9001"
    body = api.records[-1].body
    assert body["chat_id"] == -1000
    assert body["message_thread_id"] == 77
    assert body["reply_parameters"] == {"message_id": 10}
    assert body["message_thread_id"] != body["reply_parameters"]["message_id"]


def test_token_never_appears_in_any_error(tmp_path: Path) -> None:
    api = _Api()
    api.method_errors["sendMessage"] = (500, {"ok": False, "description": "boom"})
    api.getme = {"ok": False, "error_code": 401, "description": "Unauthorized"}
    with _Server(api) as server:
        transport = _transport(tmp_path, _store(tmp_path), server)
        with pytest.raises(TransportError) as post_error:
            transport.send(_delivery("hi"))
        with pytest.raises(TelegramError) as ok_error:
            transport.identity()
    for exc in (post_error.value, ok_error.value):
        rendered = f"{exc!s} {exc!r} {exc.args!r}"
        assert TOKEN not in rendered
        assert "SECRET" not in rendered
    assert ok_error.value.code == 401


def test_ok_false_raises_typed_safe_error(tmp_path: Path) -> None:
    api = _Api()
    api.send = {"ok": False, "error_code": 403, "description": "Forbidden: bot was blocked"}
    with _Server(api) as server:
        transport = _transport(tmp_path, _store(tmp_path), server)
        with pytest.raises(TelegramError) as info:
            transport.send(_delivery("hi"))
    assert info.value.code == 403
    assert "blocked" not in str(info.value)
    assert TOKEN not in str(info.value)


def test_identity_requires_a_bot(tmp_path: Path) -> None:
    api = _Api()
    with _Server(api) as server:
        transport = _transport(tmp_path, _store(tmp_path), server)
        assert transport.identity() == "42"
        api.getme = {"ok": True, "result": {"id": 42, "is_bot": False}}
        with pytest.raises(TelegramError):
            transport.identity()
        api.getme = {"ok": True, "result": {"id": 42, "is_bot": True}}
        assert transport.identity() == "42"


def test_malformed_update_id_is_never_skipped(tmp_path: Path) -> None:
    api = _Api()
    api.updates_pages = [
        [_update(1, _message(10)), {"update_id": True, "message": _message(11)}],
    ]
    store = _store(tmp_path)
    with _Server(api) as server:
        transport = _transport(tmp_path, store, server)
        with pytest.raises(TelegramError):
            transport.poll()
    assert store.get_checkpoint(TELEGRAM_OFFSET_KEY) is None
    assert store.list_jobs() == []


def test_non_message_updates_are_ignored_but_advance_the_offset(tmp_path: Path) -> None:
    api = _Api()
    api.updates_pages = [[{"update_id": 4, "edited_message": _message(10)}]]
    store = _store(tmp_path)
    with _Server(api) as server:
        accepted = _transport(tmp_path, store, server).poll()
    assert accepted == []
    assert store.get_checkpoint(TELEGRAM_OFFSET_KEY) == "5"


def test_bot_and_self_messages_are_ignored(tmp_path: Path) -> None:
    api = _Api()
    api.updates_pages = [[_update(1, _message(10, "from bot", is_bot=True))]]
    store = _store(tmp_path)
    with _Server(api) as server:
        accepted = _transport(tmp_path, store, server).poll()
    assert accepted == []
    assert store.get_checkpoint(TELEGRAM_OFFSET_KEY) == "2"


def test_duplicate_update_does_not_create_a_second_job(tmp_path: Path) -> None:
    api = _Api()
    api.updates_pages = [
        [_update(1, _message(10, "once"))],
        [_update(2, _message(10, "once"))],
    ]
    store = _store(tmp_path)
    with _Server(api) as server:
        _transport(tmp_path, store, server).poll()
    assert len(store.list_jobs()) == 1


def test_typing_and_edit_use_their_endpoints(tmp_path: Path) -> None:
    api = _Api()
    with _Server(api) as server:
        transport = _transport(tmp_path, _store(tmp_path), server)
        transport.typing(CHAT, thread_id=THREAD)
        transport.edit(CHAT, "9001", "updated")
    typing = api.records[-2]
    edit = api.records[-1]
    assert typing.method == "sendChatAction"
    assert typing.body == {"chat_id": -1000, "action": "typing", "message_thread_id": 77}
    assert edit.method == "editMessageText"
    assert edit.body == {"chat_id": -1000, "message_id": 9001, "text": "updated"}


def test_rate_limit_delay_reaches_the_caller(tmp_path: Path) -> None:
    api = _Api()
    api.method_errors["sendMessage"] = (429, {"ok": False, "parameters": {"retry_after": 3}})
    with _Server(api) as server:
        transport = _transport(tmp_path, _store(tmp_path), server)
        with pytest.raises(TransportError) as info:
            transport.send(_delivery("hi"))
        assert len(api.records) == 1
    assert info.value.status == 429
    assert info.value.retry_after == 3


@pytest.mark.parametrize(
    "base_url",
    [
        "http://evil.example",
        "https://evil.example",
        "ftp://api.telegram.org",
        "https://api.telegram.org/x",
    ],
)
def test_constructor_rejects_non_canonical_endpoints(tmp_path: Path, base_url: str) -> None:
    with pytest.raises(ValueError):
        TelegramTransport(_config(tmp_path), _store(tmp_path), HttpClient(), base_url=base_url)


def test_constructor_accepts_canonical_and_loopback(tmp_path: Path) -> None:
    canonical = TelegramTransport(_config(tmp_path), _store(tmp_path))
    assert canonical.base_url == "https://api.telegram.org"
    loopback = TelegramTransport(
        _config(tmp_path), _store(tmp_path), base_url="http://127.0.0.1:8080"
    )
    assert loopback.base_url == "http://127.0.0.1:8080"


def test_submission_is_typed_and_pending(tmp_path: Path) -> None:
    api = _Api()
    api.updates_pages = [[_update(1, _message(10))]]
    with _Server(api) as server:
        store = _store(tmp_path)
        (job_id,) = _transport(tmp_path, store, server).poll()
        submission = store.get_job(job_id)
    assert isinstance(submission, Job)
    assert submission.approved is False
    assert submission.platform == "telegram"
    assert submission.channel_id == CHAT
