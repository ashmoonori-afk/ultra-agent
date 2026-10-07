"""Authoritative omo JSON turn parsing over the shared process boundary."""

from __future__ import annotations

import json
from pathlib import Path

from ultra_agent.models import Config, JSONValue, TurnResult
from ultra_agent.process import child_environment, run_process

_NON_REPLY_EVENTS = frozenset({
    "session", "turn_start", "turn_end", "message_start", "message_update",
    "tool_execution_start", "tool_execution_update", "tool_execution_end",
    "queue_update", "compaction_start", "compaction_progress", "session_info_changed",
    "thinking_level_changed", "thinking_level_clamped", "system_prompt_change",
    "tool_hook_status", "auto_retry_start", "auto_retry_end",
    "summarization_retry_scheduled", "summarization_retry_attempt_start",
    "summarization_retry_finished",
    "settings_source_selected", "entry_appended", "agent_settled", "agent_idle",
})


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
    result = run_process(
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


__all__ = ["run_turn"]
