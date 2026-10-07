"""Real wire and process-group proofs; synchronization uses sockets and pidfds."""

from __future__ import annotations

import fcntl
import json
import os
import selectors
import socket
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from ultra_agent import driver
from ultra_agent.models import (
    Config,
    MessengerConfig,
    RelayPeer,
    RelayServerConfig,
    RelaySource,
)

FIXTURE = Path(__file__).parent / "fixtures" / "omo.py"


def config_for(tmp_path: Path, mode: str) -> Config:
    config = Config(
        workspace=tmp_path,
        command=(sys.executable, str(FIXTURE), mode),
        timeout_seconds=5,
    )
    config.work_dir.mkdir()
    config.sessions_dir.mkdir(parents=True)
    return config


@pytest.mark.parametrize("tools", [(), ("read", "bash")])
def test_turn_argv_stdin_and_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                         tools: tuple[str, ...]) -> None:
    # Given all configured transport credentials and parent session identifiers.
    config = replace(
        config_for(tmp_path, "echo"), model="provider/model", tools=tools,
        messengers=(
            MessengerConfig("discord", "TEST_DISCORD", enabled=False),
            MessengerConfig("telegram", "TEST_TELEGRAM"),
            MessengerConfig("slack", "TEST_SLACK", cookie_env="TEST_COOKIE"),
        ),
        peers=(RelayPeer("peer", "http://localhost", "source", "TEST_PEER"),),
        relay_server=RelayServerConfig(
            sources=(RelaySource("source", "discord", "channel", "TEST_SOURCE"),),
        ),
    )
    excluded = (
        "TEST_DISCORD", "TEST_TELEGRAM", "TEST_SLACK", "TEST_COOKIE", "TEST_PEER",
        "TEST_SOURCE", "PI_SESSION_ID", "PI_SESSION_FILE", "PI_SESSION_CWD", "PI_GOAL_STORE_FILE",
        "CLAUDECODE", "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "NOTIFY_SOCKET",
    )
    for name in excluded:
        monkeypatch.setenv(name, "fixture-value")
    monkeypatch.setenv("TEST_PROVIDER_AUTH", "provider-fixture")
    prompt = "input with 'quotes'\n$(not-a-command)"
    # When a real subprocess runs.
    result = driver.run_turn(config, "session-fixture", prompt)
    # Then only stdin contains the prompt and auth isolation is exact.
    assert result.success, result.error
    report = json.loads(result.text)
    expected = [
        "-p", "--mode", "json", "--session-id", "session-fixture",
        "--session-dir", str(config.sessions_dir), "--model", "provider/model",
    ]
    expected += ["--tools", ",".join(tools)] if tools else ["--no-tools"]
    assert report["argv"] == expected
    assert report["prompt"] == prompt
    assert report["cwd"] == str(config.work_dir)
    assert all(name not in report["env"] for name in excluded)
    assert report["env"]["TEST_PROVIDER_AUTH"] == "provider-fixture"
    assert result.exit_code == 0


@pytest.mark.parametrize(
    ("mode", "success", "compaction_failed"),
    [
        ("normal", True, False), ("external-owner", True, False),
        ("retry_compaction", True, False), ("plain", True, False),
        ("missingusage", True, False),
        ("error", False, False), ("error_message", False, False),
        ("empty", False, False), ("noagentend", False, False),
        ("tooltext", False, False), ("compaction_failed", False, True),
        ("noise", False, False), ("retry", False, False), ("exit_error", False, False),
        ("jsonnoise", False, False), ("missing_retry", False, False),
        ("wrong_session", False, False), ("no_session", False, False),
        ("recovered", True, False), ("cancelled_compaction", True, False),
        ("aborted", False, False),
        ("observed_events", True, False),
    ],
)
def test_authoritative_wire_outcome(tmp_path: Path, mode: str, success: bool,
                                    compaction_failed: bool) -> None:
    # Given actual documented JSON lifecycle events.
    config = config_for(tmp_path, mode)
    # When the turn finishes.
    result = driver.run_turn(config, "fixture", "input")
    # Then thinking, deltas and tool text cannot become the reply.
    assert result.success is success
    assert result.compaction_failed is compaction_failed
    if success:
        assert result.text == "last authoritative text"
    else:
        assert result.error


@pytest.mark.parametrize(
    ("runner", "fixture", "reply"),
    [("codex", "codex.py", None), ("claude", "claude.py", "fixture reply")],
)
def test_configured_runner_reaches_its_backend(
    tmp_path: Path, runner: str, fixture: str, reply: str | None
) -> None:
    # Given a lane whose configuration selects a non-omo backend.
    path = Path(__file__).parent / "fixtures" / fixture
    config = replace(
        config_for(tmp_path, "ok"), runner=runner,
        command=(sys.executable, str(path), "ok"), tools=("native",) if runner == "codex" else (),
    )
    session = "6f1a2b3c-4d5e-4f60-8a7b-9c0d1e2f3a4b"
    # When the facade runs a turn.
    result = driver.run_turn(config, session, "hello")
    # Then that backend's protocol produced the reply and its session descriptor.
    assert result.success, result.error
    if reply is not None:
        assert result.text == reply
    assert (config.sessions_dir / f"{session}.jsonl").is_file()


@pytest.mark.parametrize("mode", ["huge", "huge_stderr"])
def test_output_cap(tmp_path: Path, mode: str) -> None:
    config = config_for(tmp_path, mode)
    result = driver.run_turn(config, "fixture", "input")
    assert not result.success
    assert result.error == "Command output exceeds 4 MiB"
    assert len(result.text.encode()) <= driver.MAX_OUTPUT_BYTES


def test_pipe_backpressure_and_maximum_input(tmp_path: Path) -> None:
    config = config_for(tmp_path, "backpressure")
    result = driver.run_turn(config, "fixture", "p" * driver.MAX_PROMPT_BYTES)
    assert result.success, result.error
    assert result.text == str(driver.MAX_PROMPT_BYTES)


def test_prompt_cap_prevents_launch(tmp_path: Path) -> None:
    config = config_for(tmp_path, "plain")
    result = driver.run_turn(config, "fixture", "p" * (driver.MAX_PROMPT_BYTES + 1))
    assert not result.success
    assert result.error == "Prompt exceeds 1 MiB"
    assert result.exit_code is None


def test_fixed_verification_argv_and_stdout(tmp_path: Path,
                                           monkeypatch: pytest.MonkeyPatch) -> None:
    config = replace(
        config_for(tmp_path, "plain"),
        verification_command=(sys.executable, str(FIXTURE), "command", "$(literal)"),
        messengers=(MessengerConfig("slack", "TEST_TOKEN", cookie_env="TEST_COOKIE"),),
    )
    monkeypatch.setenv("TEST_TOKEN", "fixture")
    monkeypatch.setenv("TEST_COOKIE", "fixture")
    result = driver.run_verification(config)
    assert result.success, result.error
    report = json.loads(result.text)
    assert report["argv"] == ["$(literal)"]
    assert report["cwd"] == str(config.work_dir)
    assert "TEST_TOKEN" not in report["env"] and "TEST_COOKIE" not in report["env"]


@pytest.mark.parametrize("mode", ["timeout", "linger", "child_error", "child_huge"])
def test_cleanup_kills_descendants_not_unrelated_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str,
) -> None:
    # Given a socket subscription established before the fixture launches a child.
    config = replace(config_for(tmp_path, mode), timeout_seconds=1)
    address = str(tmp_path / "ready.sock")
    monkeypatch.setenv("FIXTURE_SOCKET", address)
    with socket.socket(socket.AF_UNIX) as listener, ThreadPoolExecutor() as executor:
        listener.bind(address)
        listener.listen()
        listener.settimeout(5)
        future = executor.submit(driver.run_turn, config, "fixture", "input")
        connection, _ = listener.accept()
        with connection:
            connection.settimeout(5)
            pid = int(connection.recv(100))
            pidfd = os.pidfd_open(pid)
            connection.sendall(b"go")
            try:
                # A separate process group must survive the owned group's cleanup.
                with subprocess.Popen(
                    (sys.executable, str(FIXTURE), "command"),
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, start_new_session=True,
                ) as unrelated:
                    try:
                        result = future.result(timeout=5)
                        assert not result.success
                        assert result.exit_code is not None
                        with selectors.DefaultSelector() as selector:
                            selector.register(pidfd, selectors.EVENT_READ)
                            assert selector.select(timeout=5), "descendant still alive"
                        assert unrelated.poll() is None
                    finally:
                        unrelated.kill()
                        unrelated.wait()
            finally:
                os.close(pidfd)


def test_cancellation_reaps_leader_and_kills_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = config_for(tmp_path, "timeout")
    address = str(tmp_path / "cancel.sock")
    monkeypatch.setenv("FIXTURE_SOCKET", address)
    real_waitid = os.waitid
    child_pid = 0
    child_fd = -1
    leader_pid = 0
    with socket.socket(socket.AF_UNIX) as listener:
        listener.bind(address)
        listener.listen()
        listener.settimeout(5)

        def cancel_after_ready(idtype: int, pid: int, options: int) -> os.waitid_result | None:
            nonlocal child_pid, leader_pid, child_fd
            leader_pid = pid
            connection, _ = listener.accept()
            with connection:
                child_pid = int(connection.recv(100))
                child_fd = os.pidfd_open(child_pid)
                raise KeyboardInterrupt

        monkeypatch.setattr(os, "waitid", cancel_after_ready)
        with pytest.raises(KeyboardInterrupt):
            driver.run_turn(config, "fixture", "")
    monkeypatch.setattr(os, "waitid", real_waitid)
    with pytest.raises(ChildProcessError):
        os.waitpid(leader_pid, os.WNOHANG)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child_fd, selectors.EVENT_READ)
            assert selector.select(timeout=5)
    finally:
        os.close(child_fd)


def test_public_process_boundary_runs_fixed_argv(tmp_path: Path) -> None:
    from ultra_agent.process import run_process

    result = run_process(
        (sys.executable, str(FIXTURE), "command", "$(literal)"),
        tmp_path, 5, None, b"payload",
    )
    assert result.success, result.error
    report = json.loads(result.text)
    assert report["argv"] == ["$(literal)"]
    assert report["cwd"] == str(tmp_path)


def test_command_errors_are_typed_and_bounded(tmp_path: Path) -> None:
    result = driver.run_command(("nonexistent-fixture-executable",), tmp_path, 1)
    assert not result.success and result.exit_code is None
    assert result.error == "Could not start command"


def test_command_returns_bounded_stdout_without_json_parsing(tmp_path: Path) -> None:
    result = driver.run_command((sys.executable, str(FIXTURE), "huge"), tmp_path, 5)
    assert not result.success
    assert result.error == "Command output exceeds 4 MiB"
    assert 0 < len(result.text.encode()) <= driver.MAX_OUTPUT_BYTES


def test_unconfigured_verification_does_not_claim_success(tmp_path: Path) -> None:
    result = driver.run_verification(Config(workspace=tmp_path))
    assert not result.success
    assert result.error == "No command configured"


def test_lane_working_directory_is_used(tmp_path: Path) -> None:
    config = config_for(tmp_path, "echo")
    lane_dir = tmp_path / "lane"
    lane_dir.mkdir()
    result = driver.run_turn(config, "lane-id", "input", cwd=lane_dir)
    assert result.success
    assert json.loads(result.text)["cwd"] == str(lane_dir)


def test_child_retains_lock_after_parent_descriptor_closes(tmp_path: Path) -> None:
    lock_path = tmp_path / "heavy.lock"
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    address = str(tmp_path / "inherited.sock")
    script = (
        "import os,socket,sys; os.fstat(int(sys.argv[2])); "
        "s=socket.socket(socket.AF_UNIX); s.connect(sys.argv[1]); "
        "s.sendall(b'ready'); s.recv(1); s.close()"
    )
    with socket.socket(socket.AF_UNIX) as listener, ThreadPoolExecutor() as executor:
        listener.bind(address)
        listener.listen()
        listener.settimeout(5)
        future = executor.submit(
            driver.run_command,
            (sys.executable, "-c", script, address, str(descriptor)),
            tmp_path, 5, pass_fds=(descriptor,),
        )
        try:
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(5)
                assert connection.recv(5) == b"ready"
                os.close(descriptor)
                descriptor = -1
                contender = os.open(lock_path, os.O_RDWR)
                try:
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    connection.sendall(b"x")
                    assert future.result(timeout=5).success
                    fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(contender)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
