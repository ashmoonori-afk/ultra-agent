"""Tests for explicit systemd unit rendering/installation and the keeper."""

from __future__ import annotations

import os
import select
import shlex
import shutil
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from ultra_agent import service
from ultra_agent.config import init_workspace

_CHILD = Path(__file__).parent / "fixtures" / "service_child.py"


def _console(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / "console"
    directory.mkdir()
    executable = directory / "ultra-agent"
    executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", f"{directory}{os.pathsep}{os.environ['PATH']}")
    return executable


def _write_unit(tmp_path: Path, workspace: Path, executable: Path | str) -> Path:
    unit_path = tmp_path / "units" / service.UNIT_NAME
    unit_path.parent.mkdir(parents=True, exist_ok=True)
    unit_path.write_text(service.render_service(workspace, executable), encoding="utf-8")
    return unit_path


def test_render_service_quotes_spaces_backslashes_and_percent(tmp_path: Path) -> None:
    workspace = tmp_path / 'work space' / 'quote"back\\slash%pct$var'
    text = service.render_service(workspace, executable="/opt/ultra agent/bin")

    assert "Restart=on-failure" in text
    assert "RestartSec=5" in text
    assert "KillMode=control-group" in text
    assert "WantedBy=default.target" in text

    exec_lines = [line for line in text.splitlines() if line.startswith("ExecStart=")]
    assert len(exec_lines) == 1
    assert exec_lines[0].endswith('"run"')
    assert exec_lines[0].startswith('ExecStart="/opt/ultra agent/bin" "--workspace" "')
    assert '"--workspace"' in exec_lines[0]
    assert "%%pct" in exec_lines[0]
    assert "$$var" in exec_lines[0]

    env_lines = [line for line in text.splitlines() if line.startswith("EnvironmentFile=")]
    assert len(env_lines) == 1
    assert env_lines[0].startswith("EnvironmentFile=-")
    assert str(workspace / "state" / "service.env") not in env_lines[0]
    assert "\\ " in env_lines[0]
    assert "%%pct" in env_lines[0]
    assert "$$var" not in env_lines[0]
    assert "state/service.env" in env_lines[0]


def test_render_service_rejects_newline_and_nul() -> None:
    with pytest.raises(ValueError):
        service.render_service(Path("/tmp/bad\nworkspace"))
    with pytest.raises(ValueError):
        service.render_service(Path("/tmp/ok"), executable="bad\x00executable")


def test_render_service_uses_documented_default_executable(tmp_path: Path) -> None:
    text = service.render_service(tmp_path / "ws")
    exec_lines = [line for line in text.splitlines() if line.startswith("ExecStart=")]
    assert exec_lines == [f'ExecStart="ultra-agent" "--workspace" "{tmp_path}/ws" "run"']


def test_install_service_writes_exact_unit_without_enabling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = _console(tmp_path, monkeypatch)
    config = init_workspace(tmp_path / "ws")
    unit_dir = tmp_path / "user-units"

    unit_path = service.install_service(config, unit_dir=unit_dir, enable=False)

    assert unit_path == unit_dir / service.UNIT_NAME
    assert unit_path.read_text(encoding="utf-8") == service.render_service(
        config.workspace, executable
    )


def test_default_unit_dir_is_user_systemd_dir() -> None:
    assert service.default_unit_dir() == Path.home() / ".config" / "systemd" / "user"


def test_install_service_enable_runs_systemctl_and_surfaces_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _console(tmp_path, monkeypatch)
    config = init_workspace(tmp_path / "ws")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    log = tmp_path / "systemctl.log"
    fake = bindir / "systemctl"
    body = f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> {shlex.quote(str(log))}\nexit ${{FAKE_RC:-0}}\n"
    fake.write_text(body, encoding="utf-8")
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ['PATH']}")
    unit_dir = tmp_path / "units"

    monkeypatch.setenv("FAKE_RC", "0")
    unit_path = service.install_service(config, unit_dir=unit_dir, enable=True)
    assert unit_path.exists()
    assert log.read_text(encoding="utf-8").splitlines() == [
        "--user daemon-reload",
        "--user enable --now ultra-agent.service",
    ]

    monkeypatch.setenv("FAKE_RC", "3")
    with pytest.raises(service.ServiceError):
        service.install_service(config, unit_dir=unit_dir, enable=True)


def test_rendered_unit_validates_with_systemd_analyze(tmp_path: Path) -> None:
    analyzer = shutil.which("systemd-analyze")
    unit_path = _write_unit(tmp_path, tmp_path / "work space" / "ws", sys.executable)
    if analyzer is None:
        print("systemd-analyze unavailable: unit validation not performed")
        return
    result = subprocess.run(
        [analyzer, "verify", str(unit_path)], capture_output=True, text=True, check=False
    )
    combined = result.stdout + result.stderr
    assert "is not absolute, ignoring" not in combined, combined
    assert result.returncode == 0, combined


def test_keeper_restarts_failing_child_then_exits_on_success(tmp_path: Path) -> None:
    counter = tmp_path / "counter.txt"
    waits: list[float] = []

    def wait(timeout: float) -> bool:
        waits.append(timeout)
        return False

    code = service.keeper(
        (sys.executable, str(_CHILD), "flaky", str(counter)),
        tmp_path,
        wait=wait,
    )

    assert code == 0
    assert counter.read_text(encoding="utf-8") == "2"
    backoffs = [delay for delay in waits if delay >= service._BACKOFF_INITIAL_SECONDS]
    assert backoffs == [service._BACKOFF_INITIAL_SECONDS]
    assert max(waits) <= service._BACKOFF_MAX_SECONDS


def test_keeper_terminates_and_reaps_owned_group_on_stop(tmp_path: Path) -> None:
    socket_path = tmp_path / "ready.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(socket_path))
    server.listen(1)
    stop = threading.Event()
    result: dict[str, int] = {}

    def run() -> None:
        result["code"] = service.keeper(
            (sys.executable, str(_CHILD), "signal", str(socket_path)),
            tmp_path,
            stop_event=stop,
            wait=stop.wait,
        )

    thread = threading.Thread(target=run)
    thread.start()
    try:
        connection, _ = server.accept()
        message = connection.recv(256).decode("utf-8").strip()
        connection.close()
        child_pid = int(message.split()[1])
        stop.set()
        thread.join(timeout=15)
        assert not thread.is_alive(), "keeper did not return after stop"
        assert result["code"] == 0
        assert not Path(f"/proc/{child_pid}").exists()
    finally:
        stop.set()
        server.close()
        thread.join(timeout=15)


def test_keeper_rejects_empty_command(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        service.keeper((), tmp_path)


def test_keeper_kills_descendant_even_after_leader_exits(tmp_path: Path) -> None:
    socket_path = tmp_path / "tree.sock"
    stop = threading.Event()
    with socket.socket(socket.AF_UNIX) as server:
        server.bind(str(socket_path))
        server.listen(1)
        server.settimeout(5)
        thread = threading.Thread(target=service.keeper, args=(
            (sys.executable, str(_CHILD), "tree", str(socket_path)), tmp_path, stop,
        ))
        thread.start()
        descriptor = None
        try:
            connection, _ = server.accept()
            with connection:
                connection.settimeout(5)
                child_pid = int(connection.recv(256).decode().split()[1])
            descriptor = os.pidfd_open(child_pid)
            stop.set()
            thread.join(10)
            assert not thread.is_alive()
            ready, _, _ = select.select([descriptor], [], [], 5)
            assert ready == [descriptor], "descendant still running after keeper stopped"
        finally:
            stop.set()
            thread.join(10)
            if descriptor is not None:
                os.close(descriptor)
