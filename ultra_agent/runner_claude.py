"""Claude Code headless JSON turns over the shared process boundary.

Success requires a valid ``result`` envelope, ``is_error`` false, a non-empty
reply and the exact expected session identity. The stable local lane UUID is
forced on the first turn and the recorded native id is resumed afterwards; a
returned id is persisted even on an error envelope so the lane stays resumable.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import TypeGuard

from ultra_agent.models import Config, TurnResult
from ultra_agent.process import child_environment, run_process
from ultra_agent.runner_state import SessionStateError, load_session, save_session

_EMPTY_MCP_CONFIG = '{"mcpServers":{}}'
_OVERFLOW_MARKERS = ("prompt is too long", "context window", "context length")
_NONZERO_EXIT = "Command exited unsuccessfully"


def _is_object(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict)


def _normalized_uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value))
    except ValueError:
        return None


def _command(config: Config, native: str | None, session_id: str) -> tuple[str, ...]:
    command: tuple[str, ...] = (*config.command, "-p", "--output-format", "json")
    command += ("--resume", native) if native is not None else ("--session-id", session_id)
    if config.model:
        command += ("--model", config.model)
    if config.tools:
        command += ("--tools", ",".join(config.tools))
    else:
        command += ("--tools", "", "--disallowedTools", "mcp__*")
    return command + (
        "--strict-mcp-config", "--mcp-config", _EMPTY_MCP_CONFIG,
        "--permission-mode", "dontAsk", "--permission-prompts", "none",
    )


def run_turn(
    config: Config, session_id: str, prompt: str, *,
    cwd: Path | None = None, pass_fds: tuple[int, ...] = (),
) -> TurnResult:
    """Run one bounded Claude turn with stdin-only input and JSON output."""
    try:
        native = load_session(config, session_id, "claude")
    except SessionStateError:
        return TurnResult(False, error="Invalid Claude session descriptor")
    expected = native if native is not None else session_id
    command = _command(config, native, session_id)
    result = run_process(
        command, cwd or config.work_dir, config.timeout_seconds, child_environment(config),
        prompt.encode("utf-8"), pass_fds,
    )
    # Claude exits nonzero on a failed turn but still prints its error envelope,
    # which is what distinguishes context overflow; timeouts and caps never parse.
    if not result.success and result.error != _NONZERO_EXIT:
        return TurnResult(False, error=result.error, exit_code=result.exit_code)
    try:
        parsed: object = json.loads(result.text)
    except (ValueError, RecursionError):
        return TurnResult(False, error="Invalid Claude JSON output", exit_code=result.exit_code)
    if not _is_object(parsed) or parsed.get("type") != "result":
        return TurnResult(False, error="Invalid Claude result envelope", exit_code=result.exit_code)
    envelope = parsed
    returned = _normalized_uuid(envelope.get("session_id"))
    if returned is None or returned != expected:
        return TurnResult(
            False, error="Claude session identity mismatch", exit_code=result.exit_code,
        )
    is_error = envelope.get("is_error")
    reply = envelope.get("result")
    if not isinstance(is_error, bool) or not isinstance(reply, str):
        return TurnResult(False, error="Invalid Claude result envelope", exit_code=result.exit_code)
    save_session(config, session_id, "claude", returned)
    if is_error:
        subtype = envelope.get("subtype")
        detail = f"{reply} {subtype}" if isinstance(subtype, str) else reply
        overflow = any(marker in detail.lower() for marker in _OVERFLOW_MARKERS)
        error = "Claude context overflow" if overflow else "Claude reported an error"
        return TurnResult(
            False, error=error, compaction_failed=overflow, exit_code=result.exit_code,
        )
    if not result.success:
        return TurnResult(False, error="Claude exited unsuccessfully", exit_code=result.exit_code)
    if not reply.strip():
        return TurnResult(False, error="Empty Claude reply", exit_code=result.exit_code)
    return TurnResult(True, reply, None, False, result.exit_code)


__all__ = ["run_turn"]
