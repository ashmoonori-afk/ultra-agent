"""Private service environment and Linux-owned background readiness handshake."""

from __future__ import annotations

import fcntl
import json
import math
import os
import re
import selectors
import signal
import socket
import subprocess
import sys
import threading
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TypeGuard

from ultra_agent.models import Config
from ultra_agent.service import terminate_group

READY_FD_ENV = "ULTRA_AGENT_READY_FD"
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_RECORD = "background.json"
_STOP_TIMEOUT = 10.0


class StartupError(RuntimeError):
    """Startup failed without an application readiness acknowledgement."""


@dataclass(frozen=True, slots=True)
class StartupResult:
    """Owned keeper identity; ready means the first child acknowledged readiness."""

    pid: int
    start_identity: str
    ready: bool
    already_running: bool = False


@dataclass(frozen=True, slots=True)
class _Record:
    pid: int
    start_identity: str
    command: tuple[str, ...]
    workspace: str
    ready: bool


def load_service_env(config: Config) -> dict[str, str]:
    """Parse optional NAME=JSON-string assignments without shell interpretation."""
    try:
        text = (config.state_dir / "service.env").read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except UnicodeError:
        raise StartupError("invalid service environment encoding") from None
    values: dict[str, str] = {}
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, encoded = line.partition("=")
        if not separator or _NAME.fullmatch(name) is None or name in values:
            raise StartupError(f"invalid service environment assignment on line {number}")
        try:
            value: object = json.loads(encoded)
        except json.JSONDecodeError:
            raise StartupError(f"invalid service environment value on line {number}") from None
        if not isinstance(value, str) or "\x00" in value:
            raise StartupError(f"invalid service environment value on line {number}")
        values[name] = value
    return values


@contextmanager
def service_environment(config: Config) -> Generator[None]:
    """Overlay service variables for synchronous CLI handlers, restoring on exit."""
    values = load_service_env(config)
    previous = {name: os.environ.get(name) for name in values}
    try:
        os.environ.update(values)
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def notify_ready() -> None:
    """Consume readiness destinations once, notify both, and close the pipe copy."""
    raw_fd = os.environ.pop(READY_FD_ENV, None)
    address = os.environ.pop("NOTIFY_SOCKET", None)
    descriptor = int(raw_fd) if raw_fd is not None else None
    try:
        if address:
            destination = "\0" + address[1:] if address.startswith("@") else address
            with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as connection:
                connection.sendto(b"READY=1", destination)
    finally:
        if descriptor is not None:
            try:
                os.write(descriptor, b"\x01")
            finally:
                os.close(descriptor)


@contextmanager
def _locked(config: Config) -> Generator[None]:
    config.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(
        config.state_dir / "background.lock",
        os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    with os.fdopen(descriptor, "rb") as lock:
        os.fchmod(lock.fileno(), 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _object_dict(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict)


def _object_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _string_list(value: object) -> TypeGuard[list[str]]:
    return _object_list(value) and all(isinstance(item, str) for item in value)


def _read_record(config: Config) -> _Record | None:
    try:
        with (config.state_dir / _RECORD).open(encoding="utf-8") as source:
            raw: object = json.load(source)
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, UnicodeError):
        raise StartupError("invalid background process record") from None
    if _object_dict(raw):
        pid = raw.get("pid")
        identity = raw.get("start_identity")
        command = raw.get("command")
        workspace = raw.get("workspace")
        ready = raw.get("ready")
        if (
            type(pid) is int
            and pid > 1
            and isinstance(identity, str)
            and identity.isdecimal()
            and _string_list(command)
            and command
            and workspace == str(config.workspace.resolve())
            and isinstance(workspace, str)
            and type(ready) is bool
        ):
            return _Record(pid, identity, tuple(command), workspace, ready)
    raise StartupError("invalid background process record")


def _identity(pid: int) -> tuple[str, int, int]:
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return fields[19], int(fields[2]), int(fields[3])


def _matches(record: _Record) -> bool:
    try:
        identity, group, session = _identity(record.pid)
        proc = Path(f"/proc/{record.pid}")
        argv = tuple(
            os.fsdecode(part) for part in (proc / "cmdline").read_bytes().split(b"\0")[:-1]
        )
        return (
            identity == record.start_identity
            and group == session == record.pid
            and argv == record.command
            and (proc / "cwd").resolve() == Path(record.workspace)
        )
    except (FileNotFoundError, ProcessLookupError):
        return False


@contextmanager
def _owned(record: _Record) -> Generator[int | None]:
    try:
        descriptor = os.pidfd_open(record.pid)
    except ProcessLookupError:
        yield None
        return
    try:
        with selectors.DefaultSelector() as events:
            events.register(descriptor, selectors.EVENT_READ)
            yield descriptor if not events.select(0) and _matches(record) else None
    finally:
        os.close(descriptor)


def background_status(config: Config) -> StartupResult | None:
    """Return a verified live keeper record, not a guessed PID or current tick state."""
    with _locked(config):
        record = _read_record(config)
        if record is not None:
            with _owned(record) as descriptor:
                if descriptor is not None:
                    return StartupResult(record.pid, record.start_identity, record.ready, True)
        return None


def stop_background(config: Config) -> bool:
    """Signal only a pidfd-verified keeper; its signal handler cleans child groups."""
    with _locked(config):
        record = _read_record(config)
        if record is None:
            return False
        with _owned(record) as descriptor:
            if descriptor is None:
                (config.state_dir / _RECORD).unlink(missing_ok=True)
                return False
            with selectors.DefaultSelector() as events:
                events.register(descriptor, selectors.EVENT_READ)
                try:
                    signal.pidfd_send_signal(descriptor, signal.SIGTERM)
                except ProcessLookupError:
                    # The verified owner exited before delivery; the pidfd cannot retarget.
                    pass
                if not events.select(_STOP_TIMEOUT):
                    raise StartupError("background keeper did not stop; record retained")
        (config.state_dir / _RECORD).unlink(missing_ok=True)
        return True


def _write_record(config: Config, record: _Record) -> None:
    temporary = config.state_dir / (_RECORD + ".tmp")
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        os.fchmod(output.fileno(), 0o600)
        json.dump(asdict(record), output)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(config.state_dir / _RECORD)


def start_background(
    config: Config,
    environment: Mapping[str, str],
    *,
    timeout: float = 60,
    command: Sequence[str] | None = None,
) -> StartupResult:
    """Start the CLI keeper and await its first child's byte, EOF, exit, or deadline.

    An injected command must have the keeper's SIGTERM cleanup contract. No boot
    hook is installed. Success retains a private identity record, never secrets.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("startup timeout must be finite and positive")
    workspace = str(config.workspace.resolve())
    run = (sys.executable, "-m", "ultra_agent", "--workspace", workspace, "run")
    argv = tuple(command) if command is not None else (
        sys.executable, "-m", "ultra_agent", "--workspace", workspace,
        "keeper", "--cwd", workspace, "--", *run,
    )
    if not argv:
        raise ValueError("startup command must not be empty")
    with _locked(config):
        existing = _read_record(config)
        if existing is not None:
            with _owned(existing) as descriptor:
                if descriptor is not None:
                    return StartupResult(
                        existing.pid, existing.start_identity, existing.ready, True
                    )
        reader, writer = os.pipe2(os.O_CLOEXEC)
        process: subprocess.Popen[bytes] | None = None
        pidfd: int | None = None
        succeeded = False
        try:
            with selectors.DefaultSelector() as events:
                events.register(reader, selectors.EVENT_READ, "ready")
                child_env = dict(environment)
                child_env.pop("NOTIFY_SOCKET", None)
                child_env[READY_FD_ENV] = str(writer)
                process = subprocess.Popen(
                    argv, cwd=workspace, env=child_env, pass_fds=(writer,),
                    start_new_session=True, stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
                os.close(writer)
                writer = -1
                pidfd = os.pidfd_open(process.pid)
                events.register(pidfd, selectors.EVENT_READ, "exit")
                identity, _, _ = _identity(process.pid)
                observed = events.select(timeout)
                if not observed:
                    raise StartupError("background startup timed out before readiness")
                if any(key.data == "exit" for key, _ in observed):
                    raise StartupError("background process exited before readiness")
                if os.read(reader, 1) != b"\x01":
                    raise StartupError("background process closed readiness pipe without readiness")
                record = _Record(process.pid, identity, argv, workspace, True)
                with _owned(record) as owned:
                    if owned is None:
                        raise StartupError("background process exited during readiness")
                _write_record(config, record)
                succeeded = True
                threading.Thread(target=process.wait, daemon=True).start()
                return StartupResult(process.pid, identity, True)
        finally:
            if writer >= 0:
                os.close(writer)
            os.close(reader)
            if pidfd is not None:
                os.close(pidfd)
            if process is not None and not succeeded:
                # The keeper may itself need the child termination grace period.
                terminate_group(process, grace_seconds=_STOP_TIMEOUT)
                (config.state_dir / _RECORD).unlink(missing_ok=True)
