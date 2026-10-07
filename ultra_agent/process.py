"""Bounded POSIX process boundary: fixed argv, deadlines and owned groups.

Every agent turn and operator verification runs through :func:`run_process`,
which multiplexes stdout/stderr/stdin, enforces the output and prompt caps and
always kills the owned process group before reaping the leader. It never uses a
shell, so a fixed argv cannot be reinterpreted.

:func:`child_environment` copies the parent environment minus transport secrets
and parent-session identity markers. Provider authentication variables are
preserved deliberately: the child CLI must stay logged in.
"""

from __future__ import annotations

import math
import os
import selectors
import signal
import subprocess
import time
from pathlib import Path
from typing import Final

from ultra_agent.models import CheckResult, Config

MAX_OUTPUT_BYTES: Final = 4 * 1024 * 1024
MAX_PROMPT_BYTES: Final = 1024 * 1024

_PARENT_MARKERS: Final = frozenset({
    "PI_SESSION_ID", "PI_SESSION_FILE", "PI_SESSION_CWD", "PI_GOAL_STORE_FILE",
    "CLAUDECODE", "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "NOTIFY_SOCKET",
})


def child_environment(config: Config) -> dict[str, str]:
    """Keep provider authentication, excluding transport secrets and parent identity."""
    environment = os.environ.copy()
    excluded = set(_PARENT_MARKERS)
    for messenger in config.messengers:
        excluded.add(messenger.token_env)
        if messenger.cookie_env:
            excluded.add(messenger.cookie_env)
    for peer in config.peers:
        excluded.add(peer.token_env)
    if config.relay_server:
        excluded.update(source.token_env for source in config.relay_server.sources)
    for name in excluded:
        environment.pop(name, None)
    return environment


def run_process(
    command: tuple[str, ...],
    cwd: Path,
    timeout_seconds: float,
    env: dict[str, str] | None,
    input_bytes: bytes = b"",
    pass_fds: tuple[int, ...] = (),
) -> CheckResult:
    """Multiplex all pipes; keep the leader unreaped until its group is killed."""
    if not command:
        return CheckResult(False, error="No command configured")
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        return CheckResult(False, error="Invalid command timeout")
    if len(input_bytes) > MAX_PROMPT_BYTES:
        return CheckResult(False, error="Prompt exceeds 1 MiB")
    output = bytearray()
    total = 0
    offset = 0
    error: str | None = None
    deadline = time.monotonic() + timeout_seconds
    try:
        process = subprocess.Popen(
            command, cwd=cwd, env=env, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
            pass_fds=pass_fds,
        )
    except OSError:
        return CheckResult(False, error="Could not start command")
    with process, selectors.DefaultSelector() as selector:
        assert process.stdin is not None
        assert process.stdout is not None
        assert process.stderr is not None
        try:
            for stream in (process.stdout, process.stderr):
                os.set_blocking(stream.fileno(), False)
                _ = selector.register(stream, selectors.EVENT_READ)
            if input_bytes:
                os.set_blocking(process.stdin.fileno(), False)
                _ = selector.register(process.stdin, selectors.EVENT_WRITE)
            else:
                process.stdin.close()
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    error = "Command timed out"
                    break
                # WNOWAIT anchors the owned process group until cleanup: no PID reuse.
                ended = os.waitid(
                    os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT,
                )
                if ended is not None and not selector.get_map():
                    break
                events = selector.select(min(remaining, 0.05))
                for key, mask in events:
                    fd = key.fd
                    if mask & selectors.EVENT_WRITE:
                        try:
                            offset += os.write(fd, input_bytes[offset:offset + 65536])
                        except BrokenPipeError:
                            offset = len(input_bytes)
                        if offset == len(input_bytes):
                            _ = selector.unregister(fd)
                            process.stdin.close()
                    else:
                        chunk = os.read(fd, 65536)
                        if not chunk:
                            _ = selector.unregister(fd)
                            continue
                        available = max(0, MAX_OUTPUT_BYTES - total)
                        if fd == process.stdout.fileno():
                            output.extend(chunk[:available])
                        total += len(chunk)
                        if total > MAX_OUTPUT_BYTES:
                            error = "Command output exceeds 4 MiB"
                            break
                if error:
                    break
                if ended is not None and selector.get_map() and not events:
                    error = "Command left lingering pipe owners"
                    break
        except OSError:
            error = "Command pipe failure"
        finally:
            # Also runs on cancellation and unexpected exceptions, before leader reap.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            exit_code = process.wait()
    text = output.decode("utf-8", errors="replace")
    if error is None and exit_code != 0:
        error = "Command exited unsuccessfully"
    return CheckResult(error is None, text, error, exit_code)


def run_command(
    command: tuple[str, ...],
    cwd: Path,
    timeout_seconds: float,
    env: dict[str, str] | None = None,
    *,
    pass_fds: tuple[int, ...] = (),
) -> CheckResult:
    """Run fixed argv without a shell, returning bounded stdout in ``text``."""
    return run_process(command, cwd, timeout_seconds, env, pass_fds=pass_fds)


def run_verification(
    config: Config, *, cwd: Path | None = None, pass_fds: tuple[int, ...] = ()
) -> CheckResult:
    """Run the configured fixed verification argv with transport secrets removed."""
    return run_command(
        config.verification_command, cwd or config.work_dir,
        config.timeout_seconds, child_environment(config),
        pass_fds=pass_fds,
    )


__all__ = [
    "MAX_OUTPUT_BYTES",
    "MAX_PROMPT_BYTES",
    "child_environment",
    "run_command",
    "run_process",
    "run_verification",
]
