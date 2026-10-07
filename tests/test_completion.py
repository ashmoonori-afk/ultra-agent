"""Behavior tests for durable objection windows and live Done verification.

The tests run against a real SQLite file (reopened to simulate a restart), real
local bare Git remotes with dirty, untracked and unpushed failures, a fake ``gh``
executable and a real loopback HTTP server for the npm boundary. Clocks are
injected as explicit values and the HTTP server is started before the request,
so nothing here sleeps or measures elapsed time.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote

import pytest

from ultra_agent.completion import (
    CommandSignal,
    Completion,
    IssueSignal,
    NpmSignal,
    PullRequestSignal,
    SignalError,
    parse_signals,
)
from ultra_agent.models import Config, Submission
from ultra_agent.registry import CLAIM_SECONDS, Registry, RegistryError
from ultra_agent.store import Store

LANE = "lane-a"
REPO_ROOT = Path(__file__).resolve().parents[1]
_SUCCEED = (sys.executable, "-c", "raise SystemExit(0)")
_FAIL = (sys.executable, "-c", "raise SystemExit(3)")

_GIT_ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@example.invalid",
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_SYSTEM": os.devnull,
}


def _config(
    tmp_path: Path,
    *,
    verification_command: tuple[str, ...] = _SUCCEED,
    timeout_seconds: float = 60.0,
) -> Config:
    workspace = Path(os.path.realpath(tmp_path))
    (workspace / "state" / "sessions").mkdir(parents=True, exist_ok=True)
    (workspace / "work" / LANE).mkdir(parents=True, exist_ok=True)
    return Config(
        workspace=workspace,
        verification_command=verification_command,
        timeout_seconds=timeout_seconds,
    )


def _stack(
    tmp_path: Path,
    *,
    verification_command: tuple[str, ...] = _SUCCEED,
    client: object | None = None,
    npm_registry: str | None = None,
) -> tuple[Completion, Registry, Store, Config]:
    config = _config(tmp_path, verification_command=verification_command)
    store = Store(config.db_path)
    registry = Registry(store, config)
    kwargs: dict[str, object] = {}
    if client is not None:
        kwargs["client"] = client
    if npm_registry is not None:
        kwargs["npm_registry"] = npm_registry
    return Completion(config, store, registry, **kwargs), registry, store, config


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(("git", *args), cwd=cwd, check=True, capture_output=True, env=_GIT_ENV)


def _clean_pushed_repo(tmp_path: Path) -> Path:
    remote = tmp_path / "origin.git"
    _git("init", "--bare", "-b", "main", str(remote), cwd=tmp_path)
    clone = tmp_path / "clone"
    _git("clone", str(remote), str(clone), cwd=tmp_path)
    (clone / "file.txt").write_text("one\n", encoding="utf-8")
    _git("add", "file.txt", cwd=clone)
    _git("commit", "-m", "init", cwd=clone)
    _git("push", "-u", "origin", "main", cwd=clone)
    return clone


def _fake_gh(tmp_path: Path) -> Path:
    binary = tmp_path / "bin" / "gh"
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_text(
        "#!/bin/sh\n"
        'case "$*" in\n'
        "  *pulls*) printf '%s\\n' \"$FAKE_GH_PR\" ;;\n"
        "  *issues*) printf '%s\\n' \"$FAKE_GH_ISSUE\" ;;\n"
        "  *) exit 1 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    return binary.parent


class _NpmHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.server.recorded.append(self.path)
        if self.path in self.server.ok_paths:
            self.send_response(200)
            self.end_headers()
            package, version = self.path[1:].split("/")
            self.wfile.write(json.dumps({
                "name": unquote(package),
                "version": self.server.version_override or unquote(version),
            }).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args: object) -> None:
        return


class _NpmServer:
    def __init__(self, ok_paths: set[str], version_override: str | None = None) -> None:
        self.recorded: list[str] = []
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _NpmHandler)
        self._server.recorded = self.recorded
        self._server.ok_paths = ok_paths
        self._server.version_override = version_override
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=10)


def test_parse_signals_accepts_the_four_supported_forms(tmp_path: Path) -> None:
    config = _config(tmp_path)
    parsed = parse_signals(
        "command ; pr:acme/widgets#12 ; issue:acme/widgets#7 ; npm:left-pad@1.3.0",
        config,
    )
    assert parsed == (
        CommandSignal(),
        PullRequestSignal(owner="acme", repo="widgets", number=12),
        IssueSignal(owner="acme", repo="widgets", number=7),
        NpmSignal(package="left-pad", version="1.3.0"),
    )


def test_parse_signals_rejects_unknown_malformed_and_empty(tmp_path: Path) -> None:
    with_command = _config(tmp_path)
    without_command = _config(tmp_path, verification_command=())
    for bad in ("", "   ", ";", "   ;;  ", "deploy:prod", "pr:acme/widgets",
                "pr:acme/widgets#x", "issue:acme#1", "npm:left-pad",
                "npm:left-pad@^1.0.0", "npm:left-pad@latest", "rm -rf /"):
        with pytest.raises(SignalError):
            parse_signals(bad, with_command)
    with pytest.raises(SignalError):
        parse_signals("command", without_command)


def test_claim_window_is_durable_and_ticks_do_not_extend_it(tmp_path: Path) -> None:
    completion, registry, store, config = _stack(tmp_path)
    registry.launch(LANE, "worker", "command")

    claimed = completion.claim(LANE, "tests pass", 1000.0)
    assert claimed.status == "done-claimed"
    assert claimed.deadline == 1000.0 + CLAIM_SECONDS

    reopened_registry = Registry(Store(config.db_path), config)
    reopened = Completion(config, Store(config.db_path), reopened_registry)
    before = reopened.tick(1299.0)
    assert [lane.status for lane in before] == ["done-claimed"]
    assert reopened_registry.get(LANE).deadline == 1300.0

    settled = reopened.tick(1300.0)
    assert [lane.status for lane in settled] == ["closed"]


def test_tick_emits_claim_notification_once(tmp_path: Path) -> None:
    completion, registry, _, _ = _stack(tmp_path)
    registry.launch(LANE, "worker", "command")
    completion.claim(LANE, "evidence", 0.0)

    first = completion.tick(1.0)
    assert [lane.claim_emitted for lane in first] == [True]
    assert completion.tick(2.0) == []
    assert registry.get(LANE).status == "done-claimed"
    assert registry.get(LANE).deadline == CLAIM_SECONDS


def test_close_refuses_open_window_and_unclaimed_lane(tmp_path: Path) -> None:
    completion, registry, _, _ = _stack(tmp_path)
    registry.launch(LANE, "worker", "command")
    with pytest.raises(RegistryError):
        completion.close(LANE, 10.0)

    completion.claim(LANE, "evidence", 0.0)
    with pytest.raises(RegistryError):
        completion.close(LANE, 299.0)
    assert registry.get(LANE).status == "done-claimed"


def test_close_after_window_closes_a_clean_pushed_lane(tmp_path: Path) -> None:
    clone = _clean_pushed_repo(tmp_path)
    completion, registry, _, config = _stack(tmp_path)
    registry.launch(LANE, "worker", "command", worktrees=(clone,))
    completion.claim(LANE, "evidence", 0.0)

    closed = completion.close(LANE, CLAIM_SECONDS)
    assert closed.status == "closed"

    reopened = Registry(Store(config.db_path), config)
    assert reopened.get(LANE).status == "closed"
    assert [event.status for event in reopened.history(LANE)] == [
        "working", "done-claimed", "verified-done", "closed"
    ]
    assert completion.close(LANE, CLAIM_SECONDS + 1.0).status == "closed"


def test_dirty_worktree_refuses_closure(tmp_path: Path) -> None:
    clone = _clean_pushed_repo(tmp_path)
    (clone / "file.txt").write_text("changed\n", encoding="utf-8")
    completion, registry, _, _ = _stack(tmp_path)
    registry.launch(LANE, "worker", "command", worktrees=(clone,))
    completion.claim(LANE, "evidence", 0.0)

    settled = completion.tick(CLAIM_SECONDS)
    assert [lane.status for lane in settled] == ["not-done"]
    assert registry.get(LANE).status == "not-done"


def test_untracked_file_refuses_closure(tmp_path: Path) -> None:
    clone = _clean_pushed_repo(tmp_path)
    (clone / "stray.txt").write_text("untracked\n", encoding="utf-8")
    completion, registry, _, _ = _stack(tmp_path)
    registry.launch(LANE, "worker", "command", worktrees=(clone,))
    completion.claim(LANE, "evidence", 0.0)

    settled = completion.tick(CLAIM_SECONDS)
    assert [lane.status for lane in settled] == ["not-done"]


def test_unpushed_commit_refuses_closure(tmp_path: Path) -> None:
    clone = _clean_pushed_repo(tmp_path)
    (clone / "second.txt").write_text("two\n", encoding="utf-8")
    _git("add", "second.txt", cwd=clone)
    _git("commit", "-m", "unpushed", cwd=clone)
    completion, registry, _, _ = _stack(tmp_path)
    registry.launch(LANE, "worker", "command", worktrees=(clone,))
    completion.claim(LANE, "evidence", 0.0)

    settled = completion.tick(CLAIM_SECONDS)
    assert [lane.status for lane in settled] == ["not-done"]


def test_missing_remote_refuses_closure(tmp_path: Path) -> None:
    clone = _clean_pushed_repo(tmp_path)
    _git("remote", "remove", "origin", cwd=clone)
    completion, registry, _, _ = _stack(tmp_path)
    registry.launch(LANE, "worker", "command", worktrees=(clone,))
    completion.claim(LANE, "evidence", 0.0)

    settled = completion.tick(CLAIM_SECONDS)
    assert [lane.status for lane in settled] == ["not-done"]


def test_running_execution_refuses_closure(tmp_path: Path) -> None:
    clone = _clean_pushed_repo(tmp_path)
    completion, registry, store, _ = _stack(tmp_path)
    registry.launch(LANE, "worker", "command", worktrees=(clone,))
    store.enqueue(Submission("j", "work", approved=True, lane=LANE))
    assert store.claim() is not None
    completion.claim(LANE, "evidence", 0.0)

    settled = completion.tick(CLAIM_SECONDS)
    assert [lane.status for lane in settled] == ["not-done"]


def test_failing_verification_command_refuses_closure(tmp_path: Path) -> None:
    completion, registry, _, _ = _stack(tmp_path, verification_command=_FAIL)
    registry.launch(LANE, "worker", "command")
    completion.claim(LANE, "evidence", 0.0)

    settled = completion.tick(CLAIM_SECONDS)
    assert [lane.status for lane in settled] == ["not-done"]


def test_verification_command_runs_in_lane_cwd(tmp_path: Path) -> None:
    completion, registry, _, config = _stack(tmp_path)
    lane_cwd = config.work_dir / LANE
    assert_command = (
        sys.executable,
        "-c",
        "import os,sys;"
        "raise SystemExit(0 if os.path.realpath(os.getcwd())=="
        "os.path.realpath(sys.argv[1]) else 1)",
        str(lane_cwd),
    )
    config = Config(
        workspace=config.workspace,
        verification_command=assert_command,
        timeout_seconds=60.0,
    )
    store = Store(config.db_path)
    registry = Registry(store, config)
    completion = Completion(config, store, registry)
    registry.launch(LANE, "worker", "command")
    completion.claim(LANE, "evidence", 0.0)

    settled = completion.tick(CLAIM_SECONDS)
    assert [lane.status for lane in settled] == ["closed"]


def test_merged_pull_request_signal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bindir = _fake_gh(tmp_path)
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ['PATH']}")
    completion, registry, _, config = _stack(tmp_path)
    registry.launch(LANE, "worker", "pr:acme/widgets#12")
    completion.claim(LANE, "evidence", 0.0)

    monkeypatch.setenv("FAKE_GH_PR", "2024-01-01T00:00:00Z")
    assert [lane.status for lane in completion.tick(CLAIM_SECONDS)] == ["closed"]

    registry.launch("lane-b", "worker", "pr:acme/widgets#13", cwd=config.work_dir / LANE)
    completion.claim("lane-b", "evidence", 0.0)
    monkeypatch.setenv("FAKE_GH_PR", "null")
    assert [lane.status for lane in completion.tick(CLAIM_SECONDS)] == ["not-done"]


def test_closed_issue_signal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bindir = _fake_gh(tmp_path)
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ['PATH']}")
    completion, registry, _, config = _stack(tmp_path)
    registry.launch(LANE, "worker", "issue:acme/widgets#7")
    completion.claim(LANE, "evidence", 0.0)

    monkeypatch.setenv("FAKE_GH_ISSUE", "closed")
    assert [lane.status for lane in completion.tick(CLAIM_SECONDS)] == ["closed"]

    registry.launch("lane-b", "worker", "issue:acme/widgets#8", cwd=config.work_dir / LANE)
    completion.claim("lane-b", "evidence", 0.0)
    monkeypatch.setenv("FAKE_GH_ISSUE", "open")
    assert [lane.status for lane in completion.tick(CLAIM_SECONDS)] == ["not-done"]


def test_npm_exact_version_boundary(tmp_path: Path) -> None:
    server = _NpmServer({"/%40scope%2Fname/1.2.3", "/plainpkg/1.0.0"})
    try:
        completion, registry, _, _ = _stack(
            tmp_path, npm_registry=server.base_url
        )
        registry.launch(LANE, "worker", "npm:@scope/name@1.2.3")
        completion.claim(LANE, "evidence", 0.0)
        assert [lane.status for lane in completion.tick(CLAIM_SECONDS)] == ["closed"]

        registry.launch("lane-b", "worker", "npm:plainpkg@9.9.9")
        completion.claim("lane-b", "evidence", 0.0)
        assert [lane.status for lane in completion.tick(CLAIM_SECONDS)] == ["not-done"]
        # The successful lane is re-read immediately before closure; the
        # failing lane stops at its first read.
        assert server.recorded == [
            "/%40scope%2Fname/1.2.3",
            "/%40scope%2Fname/1.2.3",
            "/plainpkg/9.9.9",
        ]
    finally:
        server.close()


def test_racing_objection_wins_over_stale_check(tmp_path: Path) -> None:
    completion, registry, _, config = _stack(tmp_path)
    script = (
        f"import sys; sys.path.insert(0, {str(REPO_ROOT)!r}); "
        "from pathlib import Path; "
        "from ultra_agent.models import Config; "
        "from ultra_agent.registry import Registry; "
        "from ultra_agent.store import Store; "
        f"c = Config(workspace=Path({str(config.workspace)!r})); s = Store(c.db_path); "
        f"r = Registry(s, c); r.object({LANE!r}, ['not done'], 1300.0)"
    )
    command = (sys.executable, "-c", script)
    config = Config(
        workspace=config.workspace, verification_command=command, timeout_seconds=60.0
    )
    registry = Registry(Store(config.db_path), config)
    completion = Completion(config, Store(config.db_path), registry)
    registry.launch(LANE, "worker", "command")
    completion.claim(LANE, "evidence", 0.0)

    settled = completion.tick(CLAIM_SECONDS)
    assert [lane.status for lane in settled] == ["not-done"]
    assert "closed" not in [event.status for event in registry.history(LANE)]


def test_queued_work_refuses_closure(tmp_path: Path) -> None:
    completion, registry, store, _ = _stack(tmp_path)
    registry.launch(LANE, "worker", "command")
    completion.claim(LANE, "evidence", 0)
    store.enqueue(Submission("pending", "unfinished", approved=True, lane=LANE))
    assert completion.close(LANE, CLAIM_SECONDS).status == "not-done"


def test_invalid_merged_timestamp_cannot_prove_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bindir = _fake_gh(tmp_path)
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_GH_PR", "false")
    completion, registry, _, _ = _stack(tmp_path)
    registry.launch(LANE, "worker", "pr:acme/widgets#12")
    completion.claim(LANE, "evidence", 0)
    assert completion.close(LANE, CLAIM_SECONDS).status == "not-done"


def test_wrong_npm_version_cannot_prove_completion(tmp_path: Path) -> None:
    server = _NpmServer({"/plainpkg/1.0.0"}, version_override="2.0.0")
    try:
        completion, registry, _, _ = _stack(tmp_path, npm_registry=server.base_url)
        registry.launch(LANE, "worker", "npm:plainpkg@1.0.0")
        completion.claim(LANE, "evidence", 0)
        assert completion.close(LANE, CLAIM_SECONDS).status == "not-done"
    finally:
        server.close()


def test_new_work_after_last_check_cannot_race_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    completion, registry, store, _ = _stack(tmp_path)
    registry.launch(LANE, "worker", "command")
    completion.claim(LANE, "evidence", 0)
    evaluate = completion.evaluate
    calls = []

    def checked(lane):
        outcome = evaluate(lane)
        calls.append(1)
        if len(calls) == 2:
            store.enqueue(Submission("new", "new work", approved=True, lane=LANE))
        return outcome

    monkeypatch.setattr(completion, "evaluate", checked)
    assert completion.close(LANE, CLAIM_SECONDS).status != "closed"
