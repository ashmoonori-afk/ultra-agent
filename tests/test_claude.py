"""Real-subprocess proofs for the Claude Code CLI runner over the shared boundary."""

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from ultra_agent import runner_claude
from ultra_agent.models import Config
from ultra_agent.runner_state import load_session

FIXTURE = Path(__file__).parent / "fixtures" / "claude.py"


def config_for(tmp_path: Path, mode: str) -> Config:
    config = Config(
        workspace=tmp_path,
        command=(sys.executable, str(FIXTURE), mode),
        timeout_seconds=5,
    )
    config.work_dir.mkdir()
    config.sessions_dir.mkdir(parents=True)
    return config


def last_report(tmp_path: Path) -> dict[str, object]:
    lines = (tmp_path / "claude.log").read_text(encoding="utf-8").splitlines()
    parsed: object = json.loads(lines[-1])
    assert isinstance(parsed, dict)
    return parsed


def test_initial_turn_argv_stdin_and_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a fresh lane UUID, a configured model and no built-in tools.
    monkeypatch.setenv("CLAUDE_FIXTURE_LOG", str(tmp_path / "claude.log"))
    session = str(uuid.uuid4())
    config = replace(config_for(tmp_path, "ok"), model="opus")
    # When the first turn runs a real subprocess.
    result = runner_claude.run_turn(config, session, "hello")
    # Then the exact argv forces the UUID and the default restriction, stdin-only.
    assert result.success, result.error
    assert result.text == "fixture reply"
    report = last_report(tmp_path)
    expected = [
        str(FIXTURE), "ok", "-p", "--output-format", "json",
        "--session-id", session, "--model", "opus",
        "--tools", "", "--disallowedTools", "mcp__*",
        "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}',
        "--permission-mode", "dontAsk", "--permission-prompts", "none",
    ]
    assert report["argv"] == expected
    assert report["stdin"] == "hello"
    assert "--dangerously-skip-permissions" not in report["argv"]
    assert "bypassPermissions" not in report["argv"]
    assert load_session(config, session, "claude") == session


def test_second_turn_resumes_saved_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a first turn that recorded the lane descriptor.
    monkeypatch.setenv("CLAUDE_FIXTURE_LOG", str(tmp_path / "claude.log"))
    session = str(uuid.uuid4())
    config = config_for(tmp_path, "ok")
    assert runner_claude.run_turn(config, session, "one").success
    # When a second turn runs on the same lane.
    second = runner_claude.run_turn(config, session, "two")
    # Then it resumes the saved native id instead of forcing a new session.
    assert second.success, second.error
    argv = last_report(tmp_path)["argv"]
    assert "--resume" in argv
    assert argv[argv.index("--resume") + 1] == session
    assert "--session-id" not in argv


def test_configured_tools_replace_disabled_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given an explicit tool allowlist.
    monkeypatch.setenv("CLAUDE_FIXTURE_LOG", str(tmp_path / "claude.log"))
    config = replace(config_for(tmp_path, "ok"), tools=("Read", "Bash"))
    # When a turn runs.
    assert runner_claude.run_turn(config, str(uuid.uuid4()), "hi").success
    # Then the tools list is passed and the empty-tools disable is omitted.
    argv = last_report(tmp_path)["argv"]
    assert argv[argv.index("--tools") + 1] == "Read,Bash"
    assert "" not in argv
    assert "--disallowedTools" not in argv


@pytest.mark.parametrize(
    "mode",
    ["malformed", "error", "wrong_session", "empty_result", "trailing_garbage", "nonzero_exit"],
)
def test_failure_modes_never_succeed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    # Given each malformed or failing wire mode.
    monkeypatch.setenv("CLAUDE_FIXTURE_LOG", str(tmp_path / "claude.log"))
    # When a turn runs.
    result = runner_claude.run_turn(config_for(tmp_path, mode), str(uuid.uuid4()), "hi")
    # Then it fails with a short error that never echoes raw output.
    assert not result.success
    assert result.error
    assert "fixture reply" not in result.error


def test_context_overflow_sets_compaction_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given the context-overflow error envelope.
    monkeypatch.setenv("CLAUDE_FIXTURE_LOG", str(tmp_path / "claude.log"))
    # When a turn runs.
    config = config_for(tmp_path, "context_overflow")
    result = runner_claude.run_turn(config, str(uuid.uuid4()), "hi")
    # Then it is a failure flagged as compaction failure.
    assert not result.success
    assert result.compaction_failed is True


def test_context_overflow_with_nonzero_exit_still_requests_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given Claude printing its overflow envelope and exiting nonzero, as it does.
    monkeypatch.setenv("CLAUDE_FIXTURE_LOG", str(tmp_path / "claude.log"))
    session = str(uuid.uuid4())
    # When the turn runs.
    result = runner_claude.run_turn(config_for(tmp_path, "overflow_exit"), session, "hi")
    # Then the failure is classified for handoff and the session stays resumable.
    assert not result.success
    assert result.compaction_failed is True
    assert load_session(config_for_existing(tmp_path), session, "claude") == session


def config_for_existing(tmp_path: Path) -> Config:
    return Config(workspace=tmp_path)


def test_error_envelope_keeps_session_resumable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given an is_error envelope carrying a valid session id.
    monkeypatch.setenv("CLAUDE_FIXTURE_LOG", str(tmp_path / "claude.log"))
    session = str(uuid.uuid4())
    config = config_for(tmp_path, "error")
    # When the turn fails.
    result = runner_claude.run_turn(config, session, "hi")
    # Then the descriptor is saved and no compaction failure is claimed.
    assert not result.success
    assert result.compaction_failed is False
    assert load_session(config, session, "claude") == session


def test_timeout_is_a_failure_without_sleep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a child that blocks on a pipe read.
    monkeypatch.setenv("CLAUDE_FIXTURE_LOG", str(tmp_path / "claude.log"))
    config = replace(config_for(tmp_path, "slow"), timeout_seconds=0.5)
    # When the turn exceeds its deadline.
    result = runner_claude.run_turn(config, str(uuid.uuid4()), "hi")
    # Then the owned group is killed and the turn fails.
    assert not result.success
    assert result.error


def test_invalid_descriptor_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a malformed descriptor already on disk.
    monkeypatch.setenv("CLAUDE_FIXTURE_LOG", str(tmp_path / "claude.log"))
    session = str(uuid.uuid4())
    config = config_for(tmp_path, "ok")
    path = config.sessions_dir / f"{session}.jsonl"
    path.write_text("not a descriptor\n", encoding="utf-8")
    # When a turn runs.
    result = runner_claude.run_turn(config, session, "hi")
    # Then it fails without resetting or overwriting the state.
    assert not result.success
    assert path.read_text(encoding="utf-8") == "not a descriptor\n"


def test_pass_fds_reach_the_child(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given an inherited write descriptor handed to the child.
    monkeypatch.setenv("CLAUDE_FIXTURE_LOG", str(tmp_path / "claude.log"))
    read_fd, write_fd = os.pipe()
    monkeypatch.setenv("CLAUDE_FIXTURE_FD", str(write_fd))
    try:
        # When a turn runs with the descriptor passed through.
        result = runner_claude.run_turn(
            config_for(tmp_path, "ok"), str(uuid.uuid4()), "hi", pass_fds=(write_fd,)
        )
    finally:
        os.close(write_fd)
    # Then the child wrote through the inherited descriptor.
    assert result.success, result.error
    try:
        assert os.read(read_fd, 64) == b"child-inherited"
    finally:
        os.close(read_fd)
