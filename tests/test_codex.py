"""Codex runner tests against the real fixture subprocess.

Every test drives :func:`ultra_agent.runner_codex.run_turn` with a real child
process: the fixture records its argv and stdin to a log so the exact protocol
is asserted, and no sleep is used (the timeout case blocks on a pipe read).
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path

import pytest

from ultra_agent.models import Config
from ultra_agent.runner_codex import run_turn
from ultra_agent.runner_state import load_session, save_session, session_path

FIXTURE = Path(__file__).parent / "fixtures" / "codex.py"
LOCAL = "11111111-2222-4333-8444-555555555555"
RESUMED = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"

_MCP_CALL = ["mcp", "list", "--json"]
_FEATURE_FLAGS = [
    "-c", "features.shell_tool=false",
    "-c", "features.unified_exec=false",
    "-c", "features.web_search=false",
    "-c", "features.web_search_request=false",
]


def _config(
    tmp_path: Path, mode: str, *, tools: tuple[str, ...] = (), model: str | None = None,
    timeout: float = 10.0,
) -> Config:
    workspace = tmp_path / "workspace"
    (workspace / "state" / "sessions").mkdir(parents=True, exist_ok=True)
    (workspace / "work").mkdir(parents=True, exist_ok=True)
    return Config(
        workspace=workspace, command=(sys.executable, str(FIXTURE), mode),
        tools=tools, model=model, timeout_seconds=timeout,
    )


def _log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    log = tmp_path / "codex-calls.jsonl"
    monkeypatch.setenv("CODEX_FIXTURE_LOG", str(log))
    return log


def _calls(log: Path) -> list[dict[str, object]]:
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line]


def _argv(call: dict[str, object]) -> list[str]:
    value = call["argv"]
    assert isinstance(value, list)
    return [str(item) for item in value]


def _exec_calls(log: Path) -> list[dict[str, object]]:
    return [call for call in _calls(log) if _argv(call)[:1] == ["exec"]]


def test_initial_run_argv_and_saved_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given an initial lane with no tools and a fake codex that replies.
    log = _log(tmp_path, monkeypatch)
    config = _config(tmp_path, "ok")

    # When the first turn runs.
    result = run_turn(config, LOCAL, "hello prompt")

    # Then the reply is authoritative and the exact argv and stdin are recorded.
    assert result.success
    assert result.text == "hello"
    recorded = _calls(log)
    assert _argv(recorded[0]) == _MCP_CALL
    assert _argv(recorded[1]) == [
        "exec", "--json", "--skip-git-repo-check", "--sandbox", "read-only",
        "-c", 'approval_policy="never"', *_FEATURE_FLAGS, "-",
    ]
    assert recorded[1]["stdin"] == "hello prompt"
    # And the native thread id is saved for resumption.
    native = load_session(config, LOCAL, "codex")
    assert native is not None
    _ = uuid.UUID(native)


def test_resume_uses_saved_native_id_and_reply_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a lane whose first turn already saved a native thread id.
    log = _log(tmp_path, monkeypatch)
    first = _config(tmp_path, "ok")
    assert run_turn(first, LOCAL, "first").success
    native = load_session(first, LOCAL, "codex")
    assert native is not None

    # When a second turn runs against the same lane.
    second = _config(tmp_path, "resume")
    result = run_turn(second, LOCAL, "second")

    # Then it resumes that exact thread and returns the resumed reply.
    assert result.success
    assert result.text == "resumed reply"
    argv = _argv(_exec_calls(log)[-1])
    assert argv[-3:] == ["resume", native, "-"]
    assert argv.index("--sandbox") < argv.index("resume")


def test_restriction_flags_present_and_each_mcp_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given configured MCP servers and the default no-tools policy.
    log = _log(tmp_path, monkeypatch)
    config = _config(tmp_path, "servers")

    # When the turn runs.
    assert run_turn(config, LOCAL, "hi").success

    # Then every feature is disabled and every server is disabled by name.
    argv = _argv(_exec_calls(log)[0])
    for flag in _FEATURE_FLAGS:
        assert flag in argv
    assert "mcp_servers.alpha.enabled=false" in argv
    assert "mcp_servers.beta.enabled=false" in argv


@pytest.mark.parametrize("mode", ["nonzero_exit", "malformed", "bad_name"])
def test_mcp_list_problem_fails_closed_without_exec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    # Given an MCP listing that fails, is malformed, or has an invalid name.
    log = _log(tmp_path, monkeypatch)
    config = _config(tmp_path, mode)

    # When the turn is attempted.
    result = run_turn(config, LOCAL, "hi")

    # Then it fails closed and no exec was ever spawned.
    assert not result.success
    assert result.error
    assert _exec_calls(log) == []
    assert [_argv(call) for call in _calls(log)] == [_MCP_CALL]


def test_native_tools_skip_restrictions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given the native-tools policy and a working codex.
    log = _log(tmp_path, monkeypatch)
    config = _config(tmp_path, "ok", tools=("native",))

    # When the turn runs.
    result = run_turn(config, LOCAL, "hi")

    # Then restrictions are skipped and no MCP listing is performed.
    assert result.success
    assert [_argv(call) for call in _calls(log)] == [
        ["exec", "--json", "--skip-git-repo-check", "--sandbox", "read-only",
         "-c", 'approval_policy="never"', "-"],
    ]


def test_invalid_tools_fails_without_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given an unsupported tools value.
    log = _log(tmp_path, monkeypatch)
    config = _config(tmp_path, "ok", tools=("shell",))

    # When the turn is attempted.
    result = run_turn(config, LOCAL, "hi")

    # Then it fails and nothing is spawned.
    assert not result.success
    assert result.error is not None and "native" in result.error
    assert _calls(log) == []


@pytest.mark.parametrize(
    "mode", ["failed", "error_event", "malformed", "unknown_event", "no_completed",
             "empty_reply", "nonzero_exit"]
)
def test_failure_modes_are_not_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    # Given a codex run that fails at the protocol or exit level.
    _log(tmp_path, monkeypatch)
    config = _config(tmp_path, mode, tools=("native",))

    # When the turn runs.
    result = run_turn(config, LOCAL, "hi")

    # Then it is never reported as a success.
    assert not result.success
    assert result.error is not None


def test_context_overflow_sets_compaction_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a turn that fails with a context-window error.
    _log(tmp_path, monkeypatch)
    config = _config(tmp_path, "context_overflow", tools=("native",))

    # When the turn runs.
    result = run_turn(config, LOCAL, "hi")

    # Then it fails and is flagged for compaction.
    assert not result.success
    assert result.compaction_failed


def test_inherited_pass_fds_reach_the_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a pipe whose write end is inherited by the child.
    read_fd, write_fd = os.pipe()
    monkeypatch.setenv("CODEX_FIXTURE_FD", str(write_fd))
    _log(tmp_path, monkeypatch)
    config = _config(tmp_path, "ok", tools=("native",))
    try:
        # When the turn runs with the write fd passed through.
        assert run_turn(config, LOCAL, "hi", pass_fds=(write_fd,)).success
    finally:
        os.close(write_fd)

    # Then the child wrote to the inherited descriptor.
    assert os.read(read_fd, 64) == b"fixture-fd"
    os.close(read_fd)


def test_timeout_is_reported_without_sleep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a codex that blocks on its own pipe past the deadline.
    _log(tmp_path, monkeypatch)
    config = _config(tmp_path, "slow", tools=("native",), timeout=0.5)

    # When the turn runs.
    result = run_turn(config, LOCAL, "hi")

    # Then the timeout is a failure.
    assert not result.success
    assert result.error == "Command timed out"


def test_invalid_session_state_returns_failed_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a malformed native-session descriptor for the lane.
    log = _log(tmp_path, monkeypatch)
    config = _config(tmp_path, "ok", tools=("native",))
    session_path(config, LOCAL).write_text(json.dumps({"broken": True}) + "\n", encoding="utf-8")

    # When the turn is attempted.
    result = run_turn(config, LOCAL, "hi")

    # Then it fails without running and without resetting the state.
    assert not result.success
    assert result.error is not None
    assert _calls(log) == []


def test_resume_thread_mismatch_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a saved thread id and a codex that starts a different thread.
    _log(tmp_path, monkeypatch)
    config = _config(tmp_path, "mismatch", tools=("native",))
    save_session(config, LOCAL, "codex", RESUMED)

    # When the turn runs.
    result = run_turn(config, LOCAL, "hi")

    # Then the identity mismatch is a failure.
    assert not result.success
    assert result.error is not None


def test_thread_id_saved_even_when_turn_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a first turn that starts a thread and then fails.
    _log(tmp_path, monkeypatch)
    config = _config(tmp_path, "failed", tools=("native",))

    # When the turn runs.
    result = run_turn(config, LOCAL, "hi")

    # Then it failed but the native thread id is still resumable.
    assert not result.success
    native = load_session(config, LOCAL, "codex")
    assert native is not None
    _ = uuid.UUID(native)
