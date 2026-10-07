"""Direct service environment and event-driven, real-process startup tests."""

from __future__ import annotations

import json
import os
import selectors
import socket
import sys
import threading
from pathlib import Path

import pytest

from ultra_agent import startup
from ultra_agent.config import init_workspace
from ultra_agent.models import Config

_CHILD = Path(__file__).parent / "fixtures" / "startup_child.py"


def test_service_environment_parses_literals_and_restores(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = init_workspace(tmp_path / "ws")
    value = '$(false) `false` $HOME "quoted" \\ newline\n'
    (config.state_dir / "service.env").write_text(
        f"# comment\nEXISTING={json.dumps(value)}\nNEW_VALUE=\"new\"\n", encoding="utf-8"
    )
    monkeypatch.setenv("EXISTING", "old")
    monkeypatch.delenv("NEW_VALUE", raising=False)
    assert startup.load_service_env(config) == {"EXISTING": value, "NEW_VALUE": "new"}
    with pytest.raises(RuntimeError), startup.service_environment(config):
        assert os.environ["EXISTING"] == value
        assert os.environ["NEW_VALUE"] == "new"
        raise RuntimeError("handler failed")
    assert os.environ["EXISTING"] == "old"
    assert "NEW_VALUE" not in os.environ


@pytest.mark.parametrize("text", [
    "export NAME=\"value\"", "BAD-NAME=\"value\"", "1NAME=\"value\"",
    "NAME=value", "NAME=42", "NAME=null", "NAME=\"value\" trailing",
    "NAME=\"a\"\nNAME=\"b\"", "NAME=\"\\u0000\"", "NAME",
])
def test_service_environment_rejects_malformed_files(tmp_path: Path, text: str) -> None:
    config = init_workspace(tmp_path / "ws")
    (config.state_dir / "service.env").write_text(text, encoding="utf-8")
    with pytest.raises(startup.StartupError):
        startup.load_service_env(config)


def test_missing_service_environment_is_optional(tmp_path: Path) -> None:
    assert startup.load_service_env(init_workspace(tmp_path / "ws")) == {}


@pytest.mark.parametrize("abstract", [False, True])
def test_notify_ready_sends_both_once_and_closes_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, abstract: bool
) -> None:
    reader, writer = os.pipe()
    name = str(tmp_path / "notify.sock")
    address = "\0" + name if abstract else name
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as server:
        server.bind(address)
        server.settimeout(2)
        monkeypatch.setenv("NOTIFY_SOCKET", "@" + name if abstract else name)
        monkeypatch.setenv(startup.READY_FD_ENV, str(writer))
        with selectors.DefaultSelector() as events:
            events.register(reader, selectors.EVENT_READ)
            try:
                startup.notify_ready()
                assert events.select(2)
                assert os.read(reader, 2) == b"\x01"
                assert server.recv(128) == b"READY=1"
                assert "NOTIFY_SOCKET" not in os.environ
                assert startup.READY_FD_ENV not in os.environ
                with pytest.raises(OSError):
                    os.fstat(writer)
                startup.notify_ready()
                assert os.read(reader, 1) == b""
            finally:
                os.close(reader)


def test_notify_socket_failure_still_closes_and_signals_pipe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader, writer = os.pipe()
    monkeypatch.setenv("NOTIFY_SOCKET", str(tmp_path / "missing.sock"))
    monkeypatch.setenv(startup.READY_FD_ENV, str(writer))
    try:
        with pytest.raises(OSError):
            startup.notify_ready()
        assert os.read(reader, 2) == b"\x01"
        assert os.read(reader, 1) == b""
    finally:
        os.close(reader)


def _command(mode: str) -> tuple[str, ...]:
    return sys.executable, str(_CHILD.resolve()), "keeper", mode


def _launch(
    config: Config, environment: dict[str, str], mode: str, timeout: float = 5
) -> tuple[threading.Thread, list[startup.StartupResult], list[BaseException]]:
    results: list[startup.StartupResult] = []
    failures: list[BaseException] = []

    def run() -> None:
        try:
            results.append(startup.start_background(
                config, environment, command=_command(mode), timeout=timeout
            ))
        except (startup.StartupError, OSError) as exc:
            failures.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, results, failures


def test_readiness_is_actual_idempotent_and_stop_cleans_owned_tree(tmp_path: Path) -> None:
    config = init_workspace(tmp_path / "ws")
    path = str(tmp_path / "events.sock")
    descriptors: list[int] = []
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(path)
        server.listen(1)
        server.settimeout(5)
        environment = dict(os.environ, STARTUP_EVENTS=path)
        thread, results, failures = _launch(config, environment, "tree")
        try:
            connection, _ = server.accept()
            with connection:
                connection.settimeout(5)
                message = json.loads(connection.recv(1024))
                descriptors = [os.pidfd_open(message[key]) for key in ("pid", "descendant")]
                assert thread.is_alive(), "spawn was incorrectly treated as readiness"
                connection.sendall(b"1")
            thread.join(5)
            assert not thread.is_alive()
            assert not failures
            result = results[0]
            assert result.ready and not result.already_running
            assert startup.background_status(config) == startup.StartupResult(
                result.pid, result.start_identity, True, True
            )
            repeated = startup.start_background(config, environment, command=("missing",))
            assert repeated.pid == result.pid and repeated.already_running
            assert (config.state_dir / "background.json").stat().st_mode & 0o777 == 0o600
            assert startup.stop_background(config)
            with selectors.DefaultSelector() as events:
                for descriptor in descriptors:
                    events.register(descriptor, selectors.EVENT_READ)
                assert len(events.select(5)) == len(descriptors)
            assert startup.background_status(config) is None
            assert not startup.stop_background(config)
        finally:
            thread.join(15)
            startup.stop_background(config)
            for descriptor in descriptors:
                os.close(descriptor)


@pytest.mark.parametrize("mode", ["eof", "exit", "hang", "stubborn_tree"])
def test_failure_never_ready_and_reaps_owned_child(tmp_path: Path, mode: str) -> None:
    config = init_workspace(tmp_path / "ws")
    path = str(tmp_path / "events.sock")
    descriptors: list[int] = []
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(path)
        server.listen(1)
        server.settimeout(5)
        thread, results, failures = _launch(
            config, dict(os.environ, STARTUP_EVENTS=path), mode, timeout=1
        )
        try:
            connection, _ = server.accept()
            with connection:
                connection.settimeout(5)
                message = json.loads(connection.recv(1024))
                descriptors.append(os.pidfd_open(message["pid"]))
                if message["descendant"] is not None:
                    descriptors.append(os.pidfd_open(message["descendant"]))
                connection.sendall(b"1")
            thread.join(15)
            assert not thread.is_alive()
            assert not results and len(failures) == 1
            assert isinstance(failures[0], startup.StartupError)
            with selectors.DefaultSelector() as events:
                for descriptor in descriptors:
                    events.register(descriptor, selectors.EVENT_READ)
                assert len(events.select(5)) == len(descriptors)
            assert not Path(f"/proc/{message['pid']}").exists()
            assert not (config.state_dir / "background.json").exists()
        finally:
            thread.join(15)
            startup.stop_background(config)
            for descriptor in descriptors:
                os.close(descriptor)


def test_stop_refuses_unrelated_process_record(tmp_path: Path) -> None:
    config = init_workspace(tmp_path / "ws")
    identity, _, _ = startup._identity(os.getpid())
    startup._write_record(config, startup._Record(
        os.getpid(), identity, (sys.executable,), str(config.workspace.resolve()), True
    ))
    assert startup.background_status(config) is None
    assert not startup.stop_background(config)
    assert not (config.state_dir / "background.json").exists()


@pytest.mark.parametrize("mismatch", ["identity", "argv"])
def test_mismatched_record_cannot_stop_live_owner(tmp_path: Path, mismatch: str) -> None:
    config = init_workspace(tmp_path / "ws")
    path = str(tmp_path / "events.sock")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(path)
        server.listen(1)
        server.settimeout(5)
        thread, results, failures = _launch(config, dict(os.environ, STARTUP_EVENTS=path), "ready")
        connection, _ = server.accept()
        with connection:
            connection.settimeout(5)
            connection.recv(1024)
            connection.sendall(b"1")
        thread.join(5)
        assert not thread.is_alive() and not failures
        record = startup._read_record(config)
        assert record is not None
        descriptor = os.pidfd_open(results[0].pid)
        try:
            startup._write_record(config, startup._Record(
                record.pid,
                str(int(record.start_identity) + 1) if mismatch == "identity"
                else record.start_identity,
                (*record.command, "unowned") if mismatch == "argv" else record.command,
                record.workspace, True,
            ))
            assert not startup.stop_background(config)
            with selectors.DefaultSelector() as events:
                events.register(descriptor, selectors.EVENT_READ)
                assert not events.select(0)
        finally:
            startup._write_record(config, record)
            startup.stop_background(config)
            os.close(descriptor)


def test_spawn_failure_has_no_record_or_descriptor_leak(tmp_path: Path) -> None:
    config = init_workspace(tmp_path / "ws")
    before = set(Path("/proc/self/fd").iterdir())
    with pytest.raises(FileNotFoundError):
        startup.start_background(config, {}, command=("/no/such/executable",))
    assert set(Path("/proc/self/fd").iterdir()) == before
    assert startup.background_status(config) is None
