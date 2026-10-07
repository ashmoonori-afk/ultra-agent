"""Explicit systemd reboot service and a bounded restart keeper.

This module renders and installs a systemd *user* unit for the daemon, and
supervises a child process outside systemd. Both are explicit operator
actions: importing the module has no side effects, :func:`install_service`
writes only into the requested unit directory, and :func:`keeper` never
registers a boot hook.

Rendering rules:

* every ``ExecStart`` argument is double-quoted with C-style escapes, so a
  workspace containing spaces, backslashes, quotes, ``$`` or ``%`` stays a
  single literal argument;
* ``%`` is doubled because systemd expands specifiers in unit values and a
  doubled ``%%`` yields a literal ``%``;
* ``EnvironmentFile=-<workspace/state/service.env>`` is optional (the ``-``
  prefix) and names a private, operator-owned file; the unit never contains a
  credential value;
* newlines and NUL bytes are rejected rather than escaped, because they cannot
  appear in a well-formed unit line or an OS path argument.

Enabling is opt-in: only ``enable=True`` runs ``systemctl --user daemon-reload``
and ``systemctl --user enable --now``. A nonzero exit raises
:class:`ServiceError` carrying the command's own diagnostics; success is never
invented.

The keeper owns the child process group (``start_new_session=True``), restarts
nonzero exits with bounded exponential backoff, exits on a normal zero exit or
on an explicit stop event, and terminates and reaps the whole owned group when
cancelled. The backoff wait is an injected ``wait(timeout) -> bool`` so tests
can run without sleeping.
"""

from __future__ import annotations

import os
import selectors
import shutil
import signal
import subprocess
import sys
import threading
from collections.abc import Callable
from pathlib import Path

from ultra_agent.models import Config

UNIT_NAME = "ultra-agent.service"
"""Fixed systemd unit filename installed by :func:`install_service`."""

_EXECUTABLE_DEFAULT = "ultra-agent"
_SERVICE_ENV_RELATIVE = ("state", "service.env")

_BACKOFF_INITIAL_SECONDS = 1.0
_BACKOFF_MAX_SECONDS = 30.0
_POLL_INTERVAL_SECONDS = 0.2
_TERMINATE_GRACE_SECONDS = 5.0


class ServiceError(RuntimeError):
    """Raised when explicit unit installation or enabling fails."""


def default_unit_dir() -> Path:
    """The per-user systemd unit directory: ``~/.config/systemd/user``."""
    return Path.home() / ".config" / "systemd" / "user"


def _reject_unsafe(value: str) -> str:
    if "\n" in value or "\r" in value:
        raise ValueError("service values must not contain newlines")
    if "\x00" in value:
        raise ValueError("service values must not contain NUL bytes")
    return value


def _quote_exec_argument(value: str) -> str:
    """Double-quote one ExecStart argument with systemd C-style escapes."""
    _reject_unsafe(value)
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("$", "$$")
        .replace("%", "%%")
    )
    return f'"{escaped}"'


def _escape_environment_file_path(value: str) -> str:
    """Escape an unquoted ``EnvironmentFile=`` path.

    systemd does not strip surrounding quotes on this setting, so the path is
    emitted with backslash escapes instead.
    """
    _reject_unsafe(value)
    return (
        value.replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace('"', '\\"')
        .replace("%", "%%")
    )


def render_service(workspace: Path, executable: Path | str = _EXECUTABLE_DEFAULT) -> str:
    """Render the systemd user unit for ``workspace`` as text.

    ``executable`` is the documented CLI entry point; the rendered
    ``ExecStart`` runs ``<executable> --workspace <workspace> run``. The unit
    restarts on failure with control-group cleanup and holds no credentials,
    only an optional, operator-owned ``state/service.env`` reference.
    """
    workspace_path = Path(workspace)
    environment_file = workspace_path.joinpath(*_SERVICE_ENV_RELATIVE)
    argv = (str(executable), "--workspace", str(workspace_path), "run")
    exec_start = " ".join(_quote_exec_argument(arg) for arg in argv)
    environment_line = "EnvironmentFile=-" + _escape_environment_file_path(
        str(environment_file)
    )
    return "\n".join(
        [
            "[Unit]",
            "Description=ultra-agent",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=notify",
            "NotifyAccess=main",
            environment_line,
            f"ExecStart={exec_start}",
            "Restart=on-failure",
            "RestartSec=5",
            "KillMode=control-group",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )


def _systemctl(*arguments: str) -> None:
    command = ("systemctl", "--user", *arguments)
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as exc:
        raise ServiceError(
            f"cannot run {' '.join(command)}: {exc.strerror or exc}"
        ) from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise ServiceError(
            f"{' '.join(command)} failed with exit code {result.returncode}"
            + (f": {detail}" if detail else "")
        )


def install_service(
    config: Config,
    unit_dir: Path | None = None,
    enable: bool = False,
) -> Path:
    """Write the unit into ``unit_dir`` and optionally enable it.

    The default directory is :func:`default_unit_dir`; the file is written to
    ``unit_dir/ultra-agent.service``. When ``enable`` is true the unit is
    reloaded and enabled with ``systemctl --user daemon-reload`` followed by
    ``systemctl --user enable --now``; any failure raises :class:`ServiceError`.
    """
    target_dir = Path(unit_dir) if unit_dir is not None else default_unit_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    unit_path = target_dir / UNIT_NAME
    executable = shutil.which("ultra-agent")
    if executable is None:
        candidate = Path(sys.executable).with_name("ultra-agent")
        if not candidate.is_file():
            raise ServiceError("install the ultra-agent console command before its service")
        executable = str(candidate)
    unit_path.write_text(
        render_service(config.workspace, Path(executable).resolve()), encoding="utf-8"
    )
    if enable:
        _systemctl("daemon-reload")
        _systemctl("enable", "--now", UNIT_NAME)
    return unit_path


def terminate_group(
    process: subprocess.Popen[bytes],
    *,
    grace_seconds: float = _TERMINATE_GRACE_SECONDS,
) -> None:
    """Terminate, then if needed kill, the child's whole process group."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    descriptor = os.pidfd_open(process.pid)
    try:
        with selectors.DefaultSelector() as events:
            events.register(descriptor, selectors.EVENT_READ)
            events.select(grace_seconds)
    finally:
        os.close(descriptor)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def _await_exit(
    process: subprocess.Popen[bytes],
    stop_event: threading.Event,
    wait: Callable[[float], bool],
) -> tuple[int | None, bool]:
    """Return ``(exit_code, stopped)`` for the running child."""
    while True:
        ended = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        if ended is not None:
            code = ended.si_status if ended.si_code == os.CLD_EXITED else -ended.si_status
            return code, False
        if wait(_POLL_INTERVAL_SECONDS) or stop_event.is_set():
            return None, True


def keeper(
    command: tuple[str, ...],
    cwd: Path,
    stop_event: threading.Event | None = None,
    wait: Callable[[float], bool] | None = None,
    *,
    ready_fd: int | None = None,
) -> int:
    """Supervise ``command`` in ``cwd``, restarting nonzero exits.

    The child runs in its own process group, so cancellation terminates and
    reaps the whole owned group. A normal zero exit or an explicit stop returns
    ``0``; a nonzero exit is retried after bounded exponential backoff. The
    ``wait(timeout) -> bool`` callable drives both the exit poll and the backoff
    (default: block on ``stop_event``), and returns true once a stop was seen.
    ``ready_fd`` transfers ownership to the keeper, which forwards it only to
    the first child and closes its own copy even if spawning fails. Restarted
    children have neither that descriptor nor ULTRA_AGENT_READY_FD.
    No boot hook is installed.
    """
    if not command:
        if ready_fd is not None:
            os.close(ready_fd)
        raise ValueError("keeper command must not be empty")
    cwd_path = Path(cwd)
    stop = stop_event if stop_event is not None else threading.Event()
    waiter: Callable[[float], bool] = wait if wait is not None else stop.wait
    backoff = _BACKOFF_INITIAL_SECONDS
    try:
        while True:
            if stop.is_set():
                return 0
            environment = dict(os.environ)
            environment.pop("ULTRA_AGENT_READY_FD", None)
            if ready_fd is not None:
                environment["ULTRA_AGENT_READY_FD"] = str(ready_fd)
            try:
                process = subprocess.Popen(
                    command, cwd=os.fspath(cwd_path), start_new_session=True,
                    env=environment, pass_fds=() if ready_fd is None else (ready_fd,),
                )
            finally:
                if ready_fd is not None:
                    os.close(ready_fd)
                    ready_fd = None
            try:
                code, stopped = _await_exit(process, stop, waiter)
            finally:
                terminate_group(process)
            if stopped:
                return 0
            if code == 0:
                return 0
            if waiter(backoff) or stop.is_set():
                return 0
            backoff = min(backoff * 2.0, _BACKOFF_MAX_SECONDS)
    finally:
        if ready_fd is not None:
            os.close(ready_fd)
