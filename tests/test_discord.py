"""Wire-level tests for ultra_agent.discord against a real loopback HTTP server.

Every test drives the production ``DiscordTransport`` and the real SQLite
``Store`` against a ``ThreadingHTTPServer`` bound to 127.0.0.1, so the actual
urllib path, header construction, cursor persistence and inbox transaction are
exercised together. The fixture server answers the Discord REST routes the
adapter uses and records each request, so assertions read the real wire bytes.
Requests are synchronous and gated by the running server; no test sleeps.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from ultra_agent.discord import (
    CURSOR_PREFIX,
    DEFAULT_BASE_URL,
    MAX_HISTORY_PAGES,
    USER_AGENT,
    DiscordTransport,
)
from ultra_agent.http import HttpClient, TransportError
from ultra_agent.models import Delivery, MessengerConfig, Submission
from ultra_agent.store import Store

TOKEN = "test-bot-token"
TOKEN_ENV = "ULTRA_AGENT_TEST_DISCORD_TOKEN"
SELF_ID = "7"
USER_ID = "1"
CHANNEL = "10"
NEW_MESSAGE_ID = "1000"

Response = tuple[int, bytes, tuple[tuple[str, str], ...]]
Responder = Callable[["_Record"], Response]
Body = Callable[["_Server", "_Discord"], None]


@dataclass
class _Record:
    """One request as the server saw it, including raw headers and body."""

    method: str
    path: str
    query: str
    headers: dict[str, str]
    body: bytes

    def body_json(self) -> object:
        return json.loads(self.body.decode("utf-8")) if self.body else None


class _Server:
    """A loopback HTTP server backed by a responder callable, with a recorder."""

    def __init__(self, responder: Responder) -> None:
        self.recorded: list[_Record] = []
        self._responder = responder
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(
            target=self.httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        server = self

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self._dispatch()

            def do_POST(self) -> None:
                self._dispatch()

            def do_PATCH(self) -> None:
                self._dispatch()

            def _dispatch(self) -> None:
                length = int(self.headers.get("Content-Length") or "0")
                body = self.rfile.read(length) if length else b""
                parts = urlsplit(self.path)
                record = _Record(
                    method=self.command,
                    path=parts.path,
                    query=parts.query,
                    headers=dict(self.headers.items()),
                    body=body,
                )
                server.recorded.append(record)
                status, payload, extra = server._responder(record)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                for name, value in extra:
                    self.send_header(name, value)
                self.end_headers()
                if payload:
                    self.wfile.write(payload)

            def log_message(self, format: str, *args: object) -> None:
                pass

        return _Handler

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

    def matching(self, method: str, path: str) -> list[_Record]:
        return [r for r in self.recorded if r.method == method and r.path == path]


class _Discord:
    """A minimal, mutable Discord REST fixture with error-route injection."""

    def __init__(self) -> None:
        self.self_user: dict[str, object] = {"id": SELF_ID, "bot": True}
        self.messages: dict[str, list[dict[str, object]]] = {}
        self.infinite = False
        self.errors: dict[tuple[str, str], Response] = {}

    def respond(self, record: _Record) -> Response:
        override = self.errors.get((record.method, record.path))
        if override is not None:
            return override
        if record.method == "GET" and record.path == "/users/@me":
            return _json(200, self.self_user)
        segments = [part for part in record.path.split("/") if part]
        if not segments or segments[0] != "channels" or len(segments) < 3:
            return _json(404, {"message": "not found"})
        channel = segments[1]
        if segments[2] == "messages" and len(segments) == 3 and record.method == "GET":
            return _list_messages(self, channel, record.query)
        if segments[2] == "messages" and len(segments) == 3 and record.method == "POST":
            return _json(200, {"id": NEW_MESSAGE_ID})
        if segments[2] == "messages" and len(segments) == 4 and record.method == "PATCH":
            return _json(200, {"id": segments[3]})
        if segments[2] == "typing" and len(segments) == 3 and record.method == "POST":
            return 204, b"", ()
        return _json(404, {"message": "not found"})


def _json(status: int, payload: object) -> Response:
    return status, json.dumps(payload).encode("utf-8"), ()


def _list_messages(discord: _Discord, channel: str, query: str) -> Response:
    params = parse_qs(query)
    limit = int(params.get("limit", ["100"])[0])
    before = params.get("before", [None])[0]
    if discord.infinite:
        start = int(before) if before else 1_000_000
        ids = [str(start - offset) for offset in range(1, limit + 1)]
        return _json(200, [_message(identifier) for identifier in ids])
    messages = discord.messages.get(channel, [])
    if before is not None:
        messages = [m for m in messages if int(str(m["id"])) < int(before)]
    return _json(200, messages[:limit])


def _message(
    identifier: str,
    *,
    user: str = USER_ID,
    content: str = "hello",
    bot: bool = False,
    webhook: str | None = None,
) -> dict[str, object]:
    message: dict[str, object] = {
        "id": identifier,
        "content": content,
        "channel_id": CHANNEL,
        "author": {"id": user, "bot": bot},
    }
    if webhook is not None:
        message["webhook_id"] = webhook
    return message


@pytest.fixture(autouse=True)
def _token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TOKEN_ENV, TOKEN)


def _store(tmp_path: Path) -> Store:
    return Store(tmp_path / "runtime.db")


def _transport(
    store: Store,
    server: _Server,
    *,
    users: tuple[str, ...] = (USER_ID,),
    channels: tuple[str, ...] = (CHANNEL,),
) -> DiscordTransport:
    config = MessengerConfig(
        platform="discord", token_env=TOKEN_ENV, users=users, channels=channels
    )
    return DiscordTransport(config, store, HttpClient(), base_url=server.base_url)


def _run(body: Body) -> None:
    discord = _Discord()

    def dispatch(record: _Record) -> Response:
        return discord.respond(record)

    with _Server(dispatch) as server:
        body(server, discord)


def test_base_url_policy(tmp_path: Path) -> None:
    store = _store(tmp_path)
    config = MessengerConfig(platform="discord", token_env=TOKEN_ENV)
    assert DiscordTransport(config, store).base_url == DEFAULT_BASE_URL
    assert DiscordTransport(config, store, base_url="http://127.0.0.1:9").base_url == (
        "http://127.0.0.1:9"
    )
    for bad in (
        "http://discord.com/api/v10",
        "https://evil.example/api",
        "ftp://127.0.0.1",
        "https://discord.com/api/v10?x=1",
        "http://user:pass@127.0.0.1/api/v10",
    ):
        with pytest.raises(ValueError):
            DiscordTransport(config, store, base_url=bad)


def test_missing_token_fails_without_a_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)

    def body(server: _Server, discord: _Discord) -> None:
        transport = _transport(_store(tmp_path), server)
        with pytest.raises(TransportError) as info:
            transport.identity()
        assert "token" in str(info.value)
        assert server.recorded == []

    _run(body)


def test_identity_proves_bot_and_caches(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        transport = _transport(_store(tmp_path), server)
        assert transport.identity() == SELF_ID
        assert transport.identity() == SELF_ID
        requests = server.matching("GET", "/users/@me")
        assert len(requests) == 1
        assert requests[0].headers["Authorization"] == f"Bot {TOKEN}"
        assert requests[0].headers["User-Agent"] == USER_AGENT

    _run(body)


def test_identity_rejects_a_non_bot_account(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        discord.self_user = {"id": SELF_ID, "bot": False}
        transport = _transport(_store(tmp_path), server)
        with pytest.raises(TransportError):
            transport.identity()

    _run(body)


def test_baseline_records_newest_without_jobs(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        store = _store(tmp_path)
        discord.messages[CHANNEL] = [_message("7"), _message("6"), _message("5")]
        transport = _transport(store, server)
        assert transport.poll() == []
        assert store.list_jobs() == []
        assert store.get_checkpoint(CURSOR_PREFIX + CHANNEL) == "7"
        transport.poll()
        assert len(server.matching("GET", "/users/@me")) == 1

    _run(body)


def test_empty_channel_baseline_does_not_swallow_the_next_message(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        store = _store(tmp_path)
        discord.messages[CHANNEL] = []
        transport = _transport(store, server)
        assert transport.poll() == []
        assert store.get_checkpoint(CURSOR_PREFIX + CHANNEL) == "0"
        discord.messages[CHANNEL] = [_message("3", content="first")]
        ids = transport.poll()
        assert len(ids) == 1
        assert store.get_job(ids[0]).prompt == "first"

    _run(body)


def test_later_poll_enqueues_new_messages_chronologically(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        store = _store(tmp_path)
        discord.messages[CHANNEL] = [_message("7")]
        transport = _transport(store, server)
        transport.poll()
        discord.messages[CHANNEL] = [
            _message("10", content="m10"),
            _message("9", content="m9"),
            _message("8", content="m8"),
            _message("7"),
        ]
        ids = transport.poll()
        assert [store.get_job(job).prompt for job in ids] == ["m8", "m9", "m10"]
        assert store.get_checkpoint(CURSOR_PREFIX + CHANNEL) == "10"
        jobs = store.list_jobs()
        assert [job.source_key for job in jobs] == [
            "discord:10:8",
            "discord:10:9",
            "discord:10:10",
        ]
        assert jobs[0].reply_to == "8"
        assert all(job.platform == "discord" for job in jobs)
        assert all(job.thread_id is None and job.channel_id == CHANNEL for job in jobs)
        assert all(job.status == "awaiting_approval" for job in jobs)

    _run(body)


def test_rejected_messages_never_become_jobs_but_advance_the_cursor(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        store = _store(tmp_path)
        store.set_checkpoint(CURSOR_PREFIX + CHANNEL, "14")
        discord.messages[CHANNEL] = [
            _message("20", content="ok"),
            _message("19", user="2", content="stranger"),
            _message("18", bot=True, content="bot"),
            _message("17", webhook="wh-1", content="hook"),
            _message("16", user=SELF_ID, content="self"),
            _message("15", content="   "),
            _message("14"),
        ]
        transport = _transport(store, server)
        ids = transport.poll()
        assert len(ids) == 1
        assert store.get_job(ids[0]).prompt == "ok"
        assert store.get_checkpoint(CURSOR_PREFIX + CHANNEL) == "20"
        assert len(store.list_jobs()) == 1

    _run(body)


def test_catch_up_walks_pages_backward_across_a_restart(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        store = _store(tmp_path)
        store.set_checkpoint(CURSOR_PREFIX + CHANNEL, "50")
        discord.messages[CHANNEL] = [
            _message(str(i), content=f"m{i}") for i in range(250, 0, -1)
        ]
        transport = _transport(store, server)
        ids = transport.poll()
        assert len(ids) == 200
        prompts = [store.get_job(job).prompt for job in ids]
        assert prompts[0] == "m51"
        assert prompts[-1] == "m250"
        assert store.get_checkpoint(CURSOR_PREFIX + CHANNEL) == "250"
        pages = server.matching("GET", f"/channels/{CHANNEL}/messages")
        assert len(pages) == 3
        assert pages[0].query == "limit=100"
        assert pages[1].query == "limit=100&before=151"
        assert pages[2].query == "limit=100&before=51"

    _run(body)


def test_page_limit_fails_without_advancing_the_cursor(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        store = _store(tmp_path)
        store.set_checkpoint(CURSOR_PREFIX + CHANNEL, "1")
        discord.infinite = True
        transport = _transport(store, server)
        with pytest.raises(TransportError) as info:
            transport.poll()
        assert "page" in str(info.value)
        assert store.get_checkpoint(CURSOR_PREFIX + CHANNEL) == "1"
        assert store.list_jobs() == []
        pages = server.matching("GET", f"/channels/{CHANNEL}/messages")
        assert len(pages) == MAX_HISTORY_PAGES

    _run(body)


def test_duplicate_intake_reuses_existing_jobs(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        store = _store(tmp_path)
        discord.messages[CHANNEL] = [_message("7")]
        transport = _transport(store, server)
        transport.poll()
        discord.messages[CHANNEL] = [
            _message("9", content="m9"),
            _message("8", content="m8"),
            _message("7"),
        ]
        first = transport.poll()
        assert len(first) == 2
        store.set_checkpoint(CURSOR_PREFIX + CHANNEL, "7")
        second = transport.poll()
        assert sorted(second) == sorted(first)
        assert len(store.list_jobs()) == 2

    _run(body)


def test_send_uses_thread_target_and_wire_shape(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        transport = _transport(_store(tmp_path), server)
        delivery = Delivery(
            id="d1",
            channel_id=CHANNEL,
            content="hi <@1>",
            reply_to="5",
            thread_id="99",
            platform="discord",
        )
        assert transport.send(delivery) == NEW_MESSAGE_ID
        record = server.matching("POST", "/channels/99/messages")[0]
        assert record.headers["Authorization"] == f"Bot {TOKEN}"
        assert record.headers["Content-Type"] == "application/json"
        assert record.body_json() == {
            "content": "hi <@1>",
            "allowed_mentions": {"parse": []},
            "message_reference": {"message_id": "5"},
        }

    _run(body)


def test_send_uses_channel_when_no_thread(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        transport = _transport(_store(tmp_path), server)
        delivery = Delivery(id="d2", channel_id=CHANNEL, content="plain", platform="discord")
        transport.send(delivery)
        record = server.matching("POST", f"/channels/{CHANNEL}/messages")[0]
        assert record.body_json() == {"content": "plain", "allowed_mentions": {"parse": []}}

    _run(body)


def test_send_rate_limit_is_reported_and_not_retried(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        transport = _transport(_store(tmp_path), server)
        discord.errors[("POST", f"/channels/{CHANNEL}/messages")] = (
            429,
            b'{"retry_after":5}',
            (("Retry-After", "5"),),
        )
        with pytest.raises(TransportError) as info:
            transport.send(Delivery(id="d3", channel_id=CHANNEL, content="x"))
        assert info.value.status == 429
        assert info.value.retry_after == 5.0
        assert len(server.recorded) == 1

    _run(body)


def test_send_server_error_is_ambiguous_and_posted_once(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        transport = _transport(_store(tmp_path), server)
        discord.errors[("POST", f"/channels/{CHANNEL}/messages")] = _json(
            500, {"message": "boom"}
        )
        with pytest.raises(TransportError) as info:
            transport.send(Delivery(id="d4", channel_id=CHANNEL, content="x"))
        assert info.value.status == 500
        assert info.value.ambiguous is True
        assert len(server.recorded) == 1

    _run(body)


def test_unauthorized_failure_never_leaks_the_token(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        transport = _transport(_store(tmp_path), server)
        discord.errors[("GET", "/users/@me")] = _json(401, {"message": "401: Unauthorized"})
        with pytest.raises(TransportError) as info:
            transport.identity()
        rendered = f"{info.value!s} {info.value!r} {info.value.args!r}"
        assert TOKEN not in rendered
        assert info.value.status == 401

    _run(body)


def test_edit_patches_content(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        transport = _transport(_store(tmp_path), server)
        assert transport.edit(CHANNEL, "5", "edited") == "5"
        record = server.matching("PATCH", f"/channels/{CHANNEL}/messages/5")[0]
        assert record.body_json() == {"content": "edited"}
        assert record.headers["Content-Type"] == "application/json"

    _run(body)


def test_typing_posts_to_channel_or_thread_and_accepts_204(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        transport = _transport(_store(tmp_path), server)
        assert transport.typing(CHANNEL) is None
        assert transport.typing(CHANNEL, thread_id="99") is None
        assert len(server.matching("POST", f"/channels/{CHANNEL}/typing")) == 1
        assert len(server.matching("POST", "/channels/99/typing")) == 1

    _run(body)


def test_finished_job_chunks_each_reference_the_source(tmp_path: Path) -> None:
    def body(server: _Server, discord: _Discord) -> None:
        store = _store(tmp_path)
        job_id = store.enqueue(
            Submission(
                source_key="discord:10:5",
                prompt="prompt",
                approved=True,
                channel_id=CHANNEL,
                reply_to="5",
                platform="discord",
            )
        )
        claimed = store.claim()
        assert claimed is not None
        store.finish(job_id, "completed", "A" * 4000)
        deliveries = [d for d in store.deliveries() if d.job_id == job_id]
        assert len(deliveries) == 3
        transport = _transport(store, server)
        for delivery in deliveries:
            assert transport.send(delivery) == NEW_MESSAGE_ID
        posts = server.matching("POST", f"/channels/{CHANNEL}/messages")
        assert len(posts) == 3
        for post in posts:
            body_json = post.body_json()
            assert body_json["message_reference"] == {"message_id": "5"}
            assert body_json["allowed_mentions"] == {"parse": []}

    _run(body)
