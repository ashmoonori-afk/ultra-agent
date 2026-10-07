"""Bounded POSIX process boundary and authoritative omo JSON replies."""

from __future__ import annotations

import json
import math
import os
import selectors
import signal
import subprocess
import time
from pathlib import Path
from typing import Final

from ultra_agent.models import CheckResult, Config, JSONValue, TurnResult

MAX_OUTPUT_BYTES: Final = 4 * 1024 * 1024
MAX_PROMPT_BYTES: Final = 1024 * 1024
_NON_REPLY_EVENTS: Final = frozenset({
    "session", "turn_start", "turn_end", "message_start", "message_update",
    "tool_execution_start", "tool_execution_update", "tool_execution_end",
    "queue_update", "compaction_start", "compaction_progress", "session_info_changed",
    "thinking_level_changed", "thinking_level_clamped", "system_prompt_change",
    "tool_hook_status", "auto_retry_start", "auto_retry_end",
    "summarization_retry_scheduled", "summarization_retry_attempt_start",
    "summarization_retry_finished",
    "settings_source_selected", "entry_appended", "agent_settled", "agent_idle",
})


def child_environment(config: Config) -> dict[str, str]:
    """Keep provider authentication, excluding transport secrets and parent identity."""
    environment = os.environ.copy()
    excluded = {
        "PI_SESSION_ID", "PI_SESSION_FILE", "PI_SESSION_CWD", "PI_GOAL_STORE_FILE",
    }
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


def _execute(
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
    return _execute(command, cwd, timeout_seconds, env, pass_fds=pass_fds)


def run_verification(
    config: Config, *, cwd: Path | None = None, pass_fds: tuple[int, ...] = ()
) -> CheckResult:
    """Run the configured fixed verification argv with transport secrets removed."""
    return run_command(
        config.verification_command, cwd or config.work_dir,
        config.timeout_seconds, child_environment(config),
        pass_fds=pass_fds,
    )


def run_turn(
    config: Config, session_id: str, prompt: str, *,
    cwd: Path | None = None, pass_fds: tuple[int, ...] = (),
) -> TurnResult:
    """Run omo with stdin-only input and parse final authoritative assistant text."""
    command = (
        *config.command, "-p", "--mode", "json", "--session-id", session_id,
        "--session-dir", str(config.sessions_dir),
    )
    if config.model:
        command += ("--model", config.model)
    command += ("--tools", ",".join(config.tools)) if config.tools else ("--no-tools",)
    result = _execute(
        command, cwd or config.work_dir, config.timeout_seconds, child_environment(config),
        prompt.encode("utf-8"), pass_fds,
    )
    text = ""
    terminal = False
    session_seen = False
    assistant_error: str | None = None
    compaction_failed = False
    error = result.error
    try:
        for line in result.text.splitlines():
            event: JSONValue = json.loads(line)
            if not isinstance(event, dict) or not isinstance(event.get("type"), str):
                error = error or "Invalid omo JSON protocol"
                break
            match event["type"]:
                case "session":
                    if event.get("id") != session_id:
                        error = error or "Session identity mismatch"
                    session_seen = True
                case "agent_start":
                    terminal = False
                case "agent_end":
                    terminal = event.get("willRetry") is False
                case "compaction_end":
                    failed = (
                        event.get("willRetry") is False
                        and event.get("rejectionCause")
                        not in ("external-owner", "cancelled-by-extension")
                        and bool(
                            event.get("errorMessage")
                            or event.get("rejectionCause")
                            in ("would-overflow", "circuit-breaker", "per-turn-cap")
                        )
                    )
                    compaction_failed = compaction_failed or failed
                case "message_end":
                    message = event.get("message")
                    if not isinstance(message, dict) or message.get("role") != "assistant":
                        continue
                    terminal = False
                    content = message.get("content")
                    if not isinstance(content, list):
                        error = error or "Invalid assistant content"
                        break
                    parts: list[str] = []
                    for part in content:
                        if isinstance(part, dict) and part.get("type") == "text":
                            value = part.get("text")
                            if not isinstance(value, str):
                                error = error or "Invalid assistant text"
                                break
                            parts.append(value)
                    text = "".join(parts)
                    assistant_error = (
                        "Assistant turn did not complete"
                        if message.get("stopReason") in ("error", "aborted")
                        or message.get("errorMessage")
                        else None
                    )
                case _:
                    # Lifecycle, usage, thinking, and tool events are not replies.
                    if event["type"] not in _NON_REPLY_EVENTS:
                        error = error or "Unknown omo JSON event"
                        break
    except (ValueError, RecursionError):
        error = error or "Invalid omo JSON protocol"
    if compaction_failed:
        error = error or "Compaction failed"
    error = error or assistant_error
    if not session_seen:
        error = error or "Missing session identity"
    if not terminal:
        error = error or "Missing terminal agent_end"
    if not text.strip():
        error = error or "Empty assistant reply"
    return TurnResult(error is None, text, error, compaction_failed, result.exit_code)
