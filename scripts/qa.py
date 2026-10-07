#!/usr/bin/env python3
"""Repeatable real-surface QA entry for ultra-agent.

This script drives the *shipped* surface, not a simulation of it:

* ``ultra_agent.cli.main(argv, runtime_factory=...)`` with a factory that builds
  a real :class:`~ultra_agent.runtime.Runtime` (real SQLite ``Store``, real
  Mnemosyne ``Memory``) whose only injection is the messenger base URL and the
  clock;
* the production :class:`~ultra_agent.messenger.Messenger`, which constructs the
  real :class:`~ultra_agent.discord.DiscordTransport` against a temporary
  loopback HTTP server carrying a dummy token;
* ``python -m ultra_agent --help`` as an actual subprocess;
* the real agent-turn boundary through ``tests/fixtures/omo.py``.

Everything it touches is temporary and isolated: one ``TemporaryDirectory`` for
the workspaces and the heavy-slot directory, and one loopback HTTP server per
scenario with a bounded ``shutdown``/``server_close``/``thread.join`` teardown.
No runtime state or evidence is written inside the repository, no sleeps or
wait-polling are used, and only loopback HTTP (with fixture credentials) is
contacted. It exits 0 only when every scenario passes and every temporary
resource is released; otherwise it exits 1 and records the failing scenario and
the cleanup receipt in ``<evidence>/qa-report.json``.
"""

from __future__ import annotations

import argparse
import contextlib
import http.server
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from ultra_agent.cli import main
from ultra_agent.config import load_config
from ultra_agent.messenger import Messenger
from ultra_agent.models import Config
from ultra_agent.runner_lock import RunnerBusy, RunnerLock
from ultra_agent.runtime import Runtime
from ultra_agent.store import Store

REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "omo.py"

TOKEN_ENV = "UA_QA_DISCORD_TOKEN"
TOKEN_VALUE = "dummy-qa-token"
ALLOWED_USER = "1"
OTHER_USER = "2"
CHANNEL_ID = "10"
BOT_ID = "99"
AUTHORIZED_ID = "1001"
UNAUTHORIZED_ID = "1002"
BOT_MESSAGE_ID = "1003"
AUTHORIZED_TEXT = "report MARKER-QA-7742"
MARKER_TITLE = "qa-marker"
MARKER_BODY = "MARKER-QA-7742"

PAGE_LIMIT = 100
"""The production adapter's page size; fillers force a multi-page catch-up."""

FILLER_COUNT = PAGE_LIMIT + 2


def _clock() -> float:
    """A fixed clock: deterministic ordering without any sleep or wall wait."""
    return 1_700_000_000.0


class DiscordLoopback(http.server.ThreadingHTTPServer):
    """A tiny Discord REST double: identity, paginated history and sends."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], *, token: str) -> None:
        super().__init__(address, _DiscordHandler)
        self.token = token
        self.messages: list[dict[str, object]] = []
        self.posts: list[dict[str, object]] = []
        self.typing = 0
        self.fail_posts = False
        self.remote_seq = 0
        self.lock = threading.Lock()

    @property
    def base_url(self) -> str:
        host, port = self.server_address[0], self.server_address[1]
        return f"http://{host}:{port}"

    def add_message(
        self, message_id: str, author_id: str, content: str, *, bot: bool = False
    ) -> None:
        with self.lock:
            self.messages.append(
                {
                    "id": message_id,
                    "channel_id": CHANNEL_ID,
                    "author": {"id": author_id, "bot": bot},
                    "content": content,
                }
            )


class _DiscordHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, fmt: str, *args: object) -> None:
        return None

    def _server(self) -> DiscordLoopback:
        server = self.server
        assert isinstance(server, DiscordLoopback)
        return server

    def _send(self, status: int, payload: object | None = None) -> None:
        body = b"" if payload is None else json.dumps(payload).encode("utf-8")
        self.send_response(status)
        if payload is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _authorized(self) -> bool:
        return self.headers.get("Authorization") == f"Bot {self._server().token}"

    def do_GET(self) -> None:
        server = self._server()
        if not self._authorized():
            self._send(401, {"message": "unauthorized"})
            return
        parts = urlsplit(self.path)
        if parts.path == "/users/@me":
            self._send(200, {"id": BOT_ID, "bot": True, "username": "qa-bot"})
            return
        if parts.path == f"/channels/{CHANNEL_ID}/messages":
            query = parse_qs(parts.query)
            limit = int(query.get("limit", [str(PAGE_LIMIT)])[0])
            before = query.get("before", [None])[0]
            with server.lock:
                messages = list(server.messages)
            if before is not None:
                boundary = int(before)
                messages = [item for item in messages if int(str(item["id"])) < boundary]
            messages.sort(key=lambda item: int(str(item["id"])), reverse=True)
            self._send(200, messages[:limit])
            return
        self._send(404, {"message": "not found"})

    def do_POST(self) -> None:
        server = self._server()
        if not self._authorized():
            self._send(401, {"message": "unauthorized"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        payload: object = json.loads(raw.decode("utf-8")) if raw else None
        parts = urlsplit(self.path)
        if parts.path == f"/channels/{CHANNEL_ID}/messages":
            with server.lock:
                server.posts.append(payload if isinstance(payload, dict) else {})
                server.remote_seq += 1
                remote_id = str(900000 + server.remote_seq)
            if server.fail_posts:
                self._send(500, {"message": "server error"})
                return
            self._send(200, {"id": remote_id, "channel_id": CHANNEL_ID})
            return
        if parts.path == f"/channels/{CHANNEL_ID}/typing":
            with server.lock:
                server.typing += 1
            self._send(204)
            return
        self._send(404, {"message": "not found"})


@contextlib.contextmanager
def serve_discord(cleanup: dict[str, object]) -> Iterator[DiscordLoopback]:
    """Start one loopback server and guarantee a bounded teardown."""
    server = DiscordLoopback(("127.0.0.1", 0), token=TOKEN_VALUE)
    thread = threading.Thread(target=server.serve_forever, name="qa-discord", daemon=True)
    thread.start()
    receipt: dict[str, object] = {"stopped": False, "closed": False, "thread_joined": False}
    servers = cleanup["servers"]
    assert isinstance(servers, list)
    servers.append(receipt)
    try:
        yield server
    finally:
        server.shutdown()
        receipt["stopped"] = True
        server.server_close()
        receipt["closed"] = True
        thread.join(timeout=5.0)
        receipt["thread_joined"] = not thread.is_alive()


class Scenario:
    """Collects the checks of one scenario; the first failure ends it."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.checks: list[str] = []

    def check(self, condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(message)
        self.checks.append(message)


def _run_scenario(name: str, body: Callable[[Scenario], None]) -> dict[str, object]:
    scenario = Scenario(name)
    try:
        body(scenario)
    except Exception as exc:
        return {
            "name": name,
            "ok": False,
            "checks": scenario.checks,
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {"name": name, "ok": True, "checks": scenario.checks}


def make_factory(
    *, base_url: str | None = None, clock: Callable[[], float] = _clock
) -> Callable[[Config], Runtime]:
    """Build the one injection seam: base URL and clock, nothing else faked."""

    def factory(config: Config) -> Runtime:
        store = Store(config.db_path)
        base_urls = {"discord": base_url} if base_url is not None else {}
        messenger = Messenger(config, store, base_urls=base_urls, clock=clock)
        return Runtime(config, messenger=messenger, clock=clock)

    return factory


def _toml(command: tuple[str, ...], slot_dir: Path, *, discord: bool) -> str:
    lines = [
        f"command = {json.dumps(list(command))}",
        "tools = []",
        "verification_command = []",
        "",
        "[host]",
        "min_memory_percent = 0.1",
        "min_disk_gib = 0.1",
        "max_lanes = 0",
        "heavy_slots = 1",
        f"slot_dir = {json.dumps(str(slot_dir))}",
    ]
    if discord:
        lines += [
            "",
            "[[messengers]]",
            'platform = "discord"',
            f"token_env = {json.dumps(TOKEN_ENV)}",
            f"users = {json.dumps([ALLOWED_USER])}",
            f"channels = {json.dumps([CHANNEL_ID])}",
            "enabled = true",
        ]
    return "\n".join(lines) + "\n"


def _init_workspace(root: Path, name: str, toml: str) -> Path:
    workspace = root / name
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = main(["--workspace", str(workspace), "init"])
    if code != 0:
        raise AssertionError(f"init failed for {name}: {buffer.getvalue()}")
    (workspace / "ultra-agent.toml").write_text(toml, encoding="utf-8")
    return workspace


def _cli(
    workspace: Path, factory: Callable[[Config], Runtime], argv: list[str]
) -> tuple[int, dict[str, object] | None, str]:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = main(["--workspace", str(workspace), *argv], runtime_factory=factory)
    raw = buffer.getvalue().strip()
    parsed: dict[str, object] | None = None
    if raw:
        try:
            decoded = json.loads(raw.splitlines()[-1])
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, dict):
            parsed = decoded
    return code, parsed, raw


def _payload(parsed: dict[str, object] | None, what: str) -> dict[str, object]:
    if parsed is None:
        raise AssertionError(f"{what}: no JSON payload emitted")
    return parsed


def _job(payload: dict[str, object], what: str) -> dict[str, object]:
    value = payload.get("job")
    if not isinstance(value, dict):
        raise AssertionError(f"{what}: no job object")
    return value


def _default_session(status: dict[str, object]) -> object:
    lanes = status["lanes"]
    assert isinstance(lanes, list)
    return next(item for item in lanes if item["key"] == "default")["session_id"]


def _scenario_help_subprocess(scenario: Scenario, root: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "ultra_agent", "--help"],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        timeout=60,
    )
    scenario.check(result.returncode == 0, "python -m ultra_agent --help exits 0")
    scenario.check("usage" in result.stdout.lower(), "help prints the argparse usage line")


def _seed_marker(scenario: Scenario, workspace: Path, factory: Callable[[Config], Runtime]) -> None:
    code, parsed, _ = _cli(
        workspace, factory, ["memory", "write", MARKER_TITLE, MARKER_BODY, "--source", "qa:seed"]
    )
    scenario.check(code == 0, "seed memory marker written")
    scenario.check(_payload(parsed, "memory write").get("ok") is True, "memory write ok")


def _scenario_discord_flow(scenario: Scenario, root: Path, cleanup: dict[str, object]) -> None:
    toml = _toml((sys.executable, str(FIXTURE), "runtime"), root / "slots-flow", discord=True)
    workspace = _init_workspace(root, "ws_flow", toml)
    with serve_discord(cleanup) as server:
        factory = make_factory(base_url=server.base_url)
        _seed_marker(scenario, workspace, factory)

        code, parsed, _ = _cli(workspace, factory, ["once"])
        scenario.check(code == 0, "once on an empty channel exits 0")
        scenario.check(_payload(parsed, "once").get("ok") is True, "empty baseline tick ok")
        code, status, _ = _cli(workspace, factory, ["status"])
        status = _payload(status, "status")
        scenario.check(code == 0 and status["jobs"] == [], "baseline made no job")

        for index in range(FILLER_COUNT):
            server.add_message(str(2000 + index), OTHER_USER, "filler")
        server.add_message(BOT_MESSAGE_ID, BOT_ID, "bot chatter", bot=True)
        server.add_message(UNAUTHORIZED_ID, OTHER_USER, "unauthorized chatter")
        server.add_message(AUTHORIZED_ID, ALLOWED_USER, AUTHORIZED_TEXT)

        code, parsed, _ = _cli(workspace, factory, ["once"])
        scenario.check(code == 0, "once after new history exits 0")
        code, status, _ = _cli(workspace, factory, ["status"])
        status = _payload(status, "status")
        pending = status["pending_approval"]
        scenario.check(
            isinstance(pending, list) and len(pending) == 1,
            "only the authorized message awaits approval",
        )
        jobs = status["jobs"]
        scenario.check(
            isinstance(jobs, list) and len(jobs) == 1,
            "unauthorized and bot records made no job",
        )
        job = jobs[0]
        assert isinstance(job, dict)
        scenario.check(job["prompt"] == AUTHORIZED_TEXT, "job carries the authorized prompt")
        scenario.check(job["reply_to"] == AUTHORIZED_ID, "job references the original message")
        job_id = str(job["id"])

        code, parsed, _ = _cli(workspace, factory, ["approve", job_id])
        approved = _job(_payload(parsed, "approve"), "approve")
        scenario.check(code == 0 and approved["status"] == "queued", "approved job is queued")

        code, parsed, _ = _cli(workspace, factory, ["once"])
        tick = _payload(parsed, "once")["tick"]
        assert isinstance(tick, dict)
        executed = tick["job"]
        assert isinstance(executed, dict)
        scenario.check(code == 0 and executed["status"] == "completed", "fixture turn completed")
        scenario.check(tick["handoff"] is None, "a successful turn writes no handoff")

        scenario.check(len(server.posts) == 1, "exactly one reply was sent")
        post = server.posts[0]
        scenario.check(post.get("allowed_mentions") == {"parse": []}, "reply disables mentions")
        reference = post.get("message_reference")
        scenario.check(
            isinstance(reference, dict) and reference.get("message_id") == AUTHORIZED_ID,
            "reply references the original message",
        )

        code, parsed, _ = _cli(workspace, factory, ["memory", "get", f"job {job_id}"])
        content = _payload(parsed, "memory get")["content"]
        scenario.check(
            isinstance(content, str) and MARKER_BODY in content,
            "provenance memory stored and recalled",
        )

        reopened = make_factory(base_url=server.base_url)
        code, status, _ = _cli(workspace, reopened, ["status"])
        status = _payload(status, "reopened status")
        jobs = status["jobs"]
        scenario.check(isinstance(jobs, list) and len(jobs) == 1, "reopen shows no replay")
        stored = jobs[0]
        assert isinstance(stored, dict)
        scenario.check(stored["status"] == "completed", "reopen keeps the completed result")
        scenario.check(stored["result"] == executed["result"], "result persisted across reopen")
        scenario.check(
            _default_session(status) == stored["session_id"],
            "reopened lane keeps the same session id",
        )
        deliveries = status["deliveries"]
        assert isinstance(deliveries, list)
        scenario.check(
            len(deliveries) == 1 and deliveries[0]["status"] == "sent",
            "delivery persisted as sent",
        )
        scenario.check(deliveries[0]["remote_id"] is not None, "sent delivery has a remote id")

        posts_before = len(server.posts)
        code, parsed, _ = _cli(workspace, reopened, ["once"])
        scenario.check(code == 0, "a second once with nothing new exits 0")
        scenario.check(len(server.posts) == posts_before, "no reply is replayed")
        code, status, _ = _cli(workspace, reopened, ["status"])
        jobs = _payload(status, "status")["jobs"]
        scenario.check(isinstance(jobs, list) and len(jobs) == 1, "no job is replayed")


def _scenario_response_loss(scenario: Scenario, root: Path, cleanup: dict[str, object]) -> None:
    toml = _toml((sys.executable, str(FIXTURE), "runtime"), root / "slots-loss", discord=True)
    workspace = _init_workspace(root, "ws_loss", toml)
    with serve_discord(cleanup) as server:
        server.fail_posts = True
        factory = make_factory(base_url=server.base_url)

        argv = ["messenger", "send", "discord", "--to", CHANNEL_ID, "--text", "hi"]
        code, parsed, _ = _cli(workspace, factory, argv)
        result = _payload(parsed, "messenger send")
        scenario.check(code == 1, "an accepted-but-lost send exits nonzero")
        statuses = result["statuses"]
        assert isinstance(statuses, dict)
        scenario.check(set(statuses.values()) == {"uncertain"}, "lost send recorded uncertain")
        delivery_ids = result["delivery_ids"]
        assert isinstance(delivery_ids, list)
        scenario.check(len(delivery_ids) == 1, "one delivery was queued")
        scenario.check(len(server.posts) == 1, "the server received exactly one POST")

        code, parsed, _ = _cli(workspace, factory, ["once"])
        scenario.check(code == 0, "a later once exits 0")
        scenario.check(len(server.posts) == 1, "an uncertain send is never automatically repeated")

        delivery_id = str(delivery_ids[0])
        code, parsed, _ = _cli(workspace, factory, ["resend", delivery_id])
        scenario.check(code == 1, "resend without the acknowledgement flag is refused")
        scenario.check(
            "acknowledge-possible-duplicate" in str(_payload(parsed, "resend")["error"]),
            "resend refusal names the required flag",
        )

        argv = ["resend", delivery_id, "--acknowledge-possible-duplicate"]
        code, parsed, _ = _cli(workspace, factory, argv)
        scenario.check(code == 0, "acknowledged resend succeeds")
        resent = _payload(parsed, "resend")["delivery"]
        assert isinstance(resent, dict)
        scenario.check(resent["status"] == "pending", "resend queues a fresh pending delivery")
        scenario.check(resent["id"] != delivery_id, "resend is a distinct delivery")
        scenario.check(len(server.posts) == 1, "resend performs no network send by itself")


def _scenario_compaction_failure(scenario: Scenario, root: Path) -> None:
    command = (sys.executable, str(FIXTURE), "compaction_failed")
    workspace = _init_workspace(
        root, "ws_cf", _toml(command, root / "slots-cf", discord=False)
    )
    factory = make_factory()

    code, parsed, _ = _cli(workspace, factory, ["submit", "compaction-failure-work", "--approve"])
    job = _job(_payload(parsed, "submit"), "submit")
    scenario.check(code == 0 and job["status"] == "queued", "approved job is queued")
    job_id = str(job["id"])

    code, status, _ = _cli(workspace, factory, ["status"])
    old_session = _default_session(_payload(status, "status"))

    code, parsed, _ = _cli(workspace, factory, ["once"])
    tick = _payload(parsed, "once")["tick"]
    assert isinstance(tick, dict)
    executed = tick["job"]
    assert isinstance(executed, dict)
    scenario.check(code == 1, "a compaction failure makes once exit nonzero")
    scenario.check(executed["status"] == "uncertain", "the job is left uncertain")
    scenario.check(executed["session_id"] == old_session, "job keeps the pre-rotation session")
    handoff = tick["handoff"]
    scenario.check(
        isinstance(handoff, str) and Path(handoff).is_file(),
        "a durable handoff file was written",
    )

    code, status, _ = _cli(workspace, factory, ["status"])
    status = _payload(status, "status")
    scenario.check(_default_session(status) != old_session, "the lane rotated to a fresh session")
    uncertain = status["uncertain_jobs"]
    scenario.check(
        isinstance(uncertain, list) and job_id in uncertain,
        "the old job stays uncertain",
    )

    code, parsed, _ = _cli(workspace, factory, ["once"])
    blocked = _payload(parsed, "once")["tick"]
    assert isinstance(blocked, dict)
    scenario.check(code == 1, "the unresolved job blocks execution")
    scenario.check(blocked["job"] is None, "no new job runs while unresolved")
    scenario.check(bool(blocked["hold"]), "the block is reported as a hold")

    code, parsed, _ = _cli(
        workspace, factory, ["resolve", job_id, "--acknowledge-uncertain-effects"]
    )
    scenario.check(code == 0, "explicit reconciliation succeeds")

    code, parsed, _ = _cli(workspace, factory, ["once"])
    cleared = _payload(parsed, "once")["tick"]
    assert isinstance(cleared, dict)
    scenario.check(code == 0, "execution resumes after reconciliation")
    scenario.check(cleared["hold"] == [], "the block clears once reconciled")


def _scenario_normal_compaction(scenario: Scenario, root: Path) -> None:
    command = (sys.executable, str(FIXTURE), "normal")
    workspace = _init_workspace(
        root, "ws_normal", _toml(command, root / "slots-normal", discord=False)
    )
    factory = make_factory()

    code, status, _ = _cli(workspace, factory, ["status"])
    old_session = _default_session(_payload(status, "status"))

    code, parsed, _ = _cli(workspace, factory, ["submit", "normal-work", "--approve"])
    scenario.check(code == 0, "submit exits 0")

    code, parsed, _ = _cli(workspace, factory, ["once"])
    tick = _payload(parsed, "once")["tick"]
    assert isinstance(tick, dict)
    executed = tick["job"]
    assert isinstance(executed, dict)
    scenario.check(code == 0 and executed["status"] == "completed", "job completed")
    scenario.check(tick["handoff"] is None, "normal compaction writes no handoff")

    code, status, _ = _cli(workspace, factory, ["status"])
    scenario.check(
        _default_session(_payload(status, "status")) == old_session,
        "normal compaction does not rotate",
    )


def _scenario_runner_lock(scenario: Scenario, root: Path) -> None:
    toml = _toml((sys.executable, str(FIXTURE), "runtime"), root / "slots-lock", discord=False)
    workspace = _init_workspace(root, "ws_lock", toml)
    factory = make_factory()

    code, parsed, _ = _cli(workspace, factory, ["submit", "locked-work", "--approve"])
    job = _job(_payload(parsed, "submit"), "submit")
    job_id = str(job["id"])

    lock = RunnerLock(load_config(workspace).state_dir / "runner.lock")
    with lock:
        code, parsed, _ = _cli(workspace, factory, ["once"])
        scenario.check(code == 1, "once while the lock is held exits nonzero")
        scenario.check(
            "owned by another runner" in str(_payload(parsed, "once")["error"]),
            "the refusal names the held runner lock",
        )
        code, status, _ = _cli(workspace, factory, ["status"])
        stored = _payload(status, "status")["jobs"]
        assert isinstance(stored, list)
        current = next(item for item in stored if item["id"] == job_id)
        scenario.check(current["status"] == "queued", "a refused once does not mutate the job")

    code, parsed, _ = _cli(workspace, factory, ["once"])
    tick = _payload(parsed, "once")["tick"]
    assert isinstance(tick, dict)
    executed = tick["job"]
    assert isinstance(executed, dict)
    scenario.check(code == 0 and executed["status"] == "completed", "the released job runs")


def _scenario_malformed_config(scenario: Scenario, root: Path) -> None:
    toml = _toml((sys.executable,), root / "slots-bad", discord=False)
    workspace = _init_workspace(root, "ws_bad", toml)
    (workspace / "ultra-agent.toml").write_text("command = [\n", encoding="utf-8")
    factory = make_factory()
    code, parsed, _ = _cli(workspace, factory, ["status"])
    scenario.check(code == 1, "a malformed configuration exits nonzero")
    scenario.check(_payload(parsed, "status").get("ok") is False, "the failure is reported as JSON")


def _no_leftover_children() -> bool:
    try:
        os.waitpid(-1, os.WNOHANG)
    except ChildProcessError:
        return True
    # Zero means a child is still running; a positive PID was an unreaped exit.
    # Only ECHILD proves that this process has no remaining children.
    return False


def run(evidence: Path) -> int:
    """Execute every scenario and write the report; return the process code."""
    evidence.mkdir(parents=True, exist_ok=True)
    os.environ[TOKEN_ENV] = TOKEN_VALUE

    cleanup: dict[str, object] = {
        "servers": [],
        "temp_root_removed": False,
        "no_leftover_children": False,
        "lock_released": False,
    }
    scenarios: list[dict[str, object]] = []
    temp_root = tempfile.TemporaryDirectory(prefix="ultra-agent-qa-")
    root = Path(temp_root.name)
    try:
        runners = [
            ("help_subprocess", lambda s: _scenario_help_subprocess(s, root)),
            (
                "discord_authorization_execution_reply",
                lambda s: _scenario_discord_flow(s, root, cleanup),
            ),
            (
                "response_loss_uncertain_no_replay",
                lambda s: _scenario_response_loss(s, root, cleanup),
            ),
            ("compaction_failure_handoff", lambda s: _scenario_compaction_failure(s, root)),
            ("normal_compaction_no_rotate", lambda s: _scenario_normal_compaction(s, root)),
            ("runner_lock_guard", lambda s: _scenario_runner_lock(s, root)),
            ("malformed_config_nonzero", lambda s: _scenario_malformed_config(s, root)),
        ]
        for name, body in runners:
            scenarios.append(_run_scenario(name, body))

        cleanup["no_leftover_children"] = _no_leftover_children()
        probe = RunnerLock(root / "ws_lock" / "state" / "runner.lock")
        try:
            with probe:
                cleanup["lock_released"] = True
        except RunnerBusy:
            cleanup["lock_released"] = False
    finally:
        temp_root.cleanup()
        cleanup["temp_root_removed"] = not root.exists()

    servers = cleanup["servers"]
    assert isinstance(servers, list)
    servers_clean = all(
        isinstance(entry, dict)
        and entry.get("stopped")
        and entry.get("closed")
        and entry.get("thread_joined")
        for entry in servers
    )
    cleanup["servers_clean"] = servers_clean

    passed = sum(1 for item in scenarios if item["ok"])
    failed = len(scenarios) - passed
    ok = (
        failed == 0
        and servers_clean
        and all(
            cleanup[key] is True
            for key in ("temp_root_removed", "no_leftover_children", "lock_released")
        )
    )
    report = {
        "ok": ok,
        "command": " ".join(sys.argv),
        "python": sys.executable,
        "repository": str(REPO),
        "evidence_dir": str(evidence),
        "counts": {"passed": passed, "failed": failed},
        "scenarios": scenarios,
        "cleanup": cleanup,
    }
    (evidence / "qa-report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    for item in scenarios:
        marker = "PASS" if item["ok"] else "FAIL"
        print(f"[{marker}] {item['name']}", file=sys.stderr)
        if not item["ok"]:
            print(f"       {item['error']}", file=sys.stderr)
    print(f"report: {evidence / 'qa-report.json'}", file=sys.stderr)
    return 0 if ok else 1


def main_entry(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="qa.py", description="real-surface QA for ultra-agent")
    parser.add_argument(
        "--evidence",
        required=True,
        metavar="DIR",
        help="external directory for the JSON report (never inside the repository)",
    )
    args = parser.parse_args(argv)
    return run(Path(str(args.evidence)).expanduser().resolve())


if __name__ == "__main__":
    sys.exit(main_entry())
