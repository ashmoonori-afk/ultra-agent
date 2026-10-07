"""Codex CLI ``exec --json`` turn parsing over the shared process boundary."""

from __future__ import annotations

import json
import re
from pathlib import Path

from ultra_agent import runner_state
from ultra_agent.models import Config, JSONValue, TurnResult
from ultra_agent.process import CheckResult, child_environment, run_process
from ultra_agent.runner_state import SessionStateError

_KNOWN_EVENTS = frozenset({
    "thread.started", "turn.started", "turn.completed", "turn.failed",
    "item.started", "item.updated", "item.completed", "error",
})
_RESTRICTION_FLAGS = (
    "-c", "features.shell_tool=false",
    "-c", "features.unified_exec=false",
    "-c", "features.web_search=false",
    "-c", "features.web_search_request=false",
)
_MCP_LIST_TIMEOUT = 30.0
# `codex mcp list --json` prints an array of objects each carrying a `name`
# (codex-rs/cli/src/mcp_cmd.rs run_list); names are used as TOML table keys.
_MCP_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_CONTEXT_MARKERS = ("context window", "context_length_exceeded", "maximum context length")


class _RestrictionError(RuntimeError):
    """A restriction could not be applied, so no unrestricted turn may run."""


def run_turn(
    config: Config, session_id: str, prompt: str, *,
    cwd: Path | None = None, pass_fds: tuple[int, ...] = (),
) -> TurnResult:
    """Run one bounded Codex turn and parse its authoritative agent message."""
    try:
        resumed_id = runner_state.load_session(config, session_id, "codex")
    except SessionStateError:
        return TurnResult(False, "", "Invalid native session state", False, None)
    try:
        restrictions = _restrictions(config, cwd or config.work_dir)
    except _RestrictionError as exc:
        return TurnResult(False, "", str(exc), False, None)
    command = _command(config, resumed_id, restrictions)
    result = run_process(
        command, cwd or config.work_dir, config.timeout_seconds, child_environment(config),
        prompt.encode("utf-8"), pass_fds,
    )
    return _parse(config, session_id, resumed_id, result)


def _restrictions(config: Config, cwd: Path) -> tuple[str, ...]:
    if not config.tools:
        return (*_RESTRICTION_FLAGS, *_disabled_mcp_flags(config, cwd))
    if config.tools == ("native",):
        return ()
    raise _RestrictionError("Codex supports only empty or ['native'] tools")


def _disabled_mcp_flags(config: Config, cwd: Path) -> tuple[str, ...]:
    result = run_process(
        (*config.command, "mcp", "list", "--json"), cwd, _MCP_LIST_TIMEOUT,
        child_environment(config),
    )
    flags: list[str] = []
    for name in _mcp_names(result):
        flags += ["-c", f"mcp_servers.{name}.enabled=false"]
    return tuple(flags)


def _mcp_names(result: CheckResult) -> tuple[str, ...]:
    if not result.success:
        raise _RestrictionError("Codex MCP listing failed")
    try:
        parsed: JSONValue = json.loads(result.text)
    except (ValueError, RecursionError) as exc:
        raise _RestrictionError("Codex MCP listing is malformed") from exc
    if not isinstance(parsed, list):
        raise _RestrictionError("Codex MCP listing is malformed")
    names: list[str] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            raise _RestrictionError("Codex MCP listing is malformed")
        name = entry.get("name")
        if not isinstance(name, str) or not _MCP_NAME.fullmatch(name):
            raise _RestrictionError("Codex MCP server name is invalid")
        names.append(name)
    return tuple(names)


def _command(
    config: Config, resumed_id: str | None, restrictions: tuple[str, ...]
) -> tuple[str, ...]:
    # Options precede the `resume` subcommand: `--sandbox` is not a global flag in
    # codex-rs/exec/src/cli.rs, so `exec resume <id> --sandbox ...` would be rejected.
    parts = [
        *config.command, "exec", "--json", "--skip-git-repo-check",
        "--sandbox", config.codex_sandbox, "-c", 'approval_policy="never"',
    ]
    if config.model:
        parts += ["--model", config.model]
    parts += restrictions
    if resumed_id is not None:
        parts += ["resume", resumed_id]
    parts.append("-")
    return tuple(parts)


def _is_context_overflow(message: str) -> bool:
    lowered = message.lower()
    return any(marker in lowered for marker in _CONTEXT_MARKERS)


def _parse(
    config: Config, session_id: str, resumed_id: str | None, result: CheckResult
) -> TurnResult:
    thread_id = resumed_id
    reply = ""
    error = result.error
    compaction_failed = False
    pending_turn = False
    terminal = False
    failed_after = False
    for line in result.text.splitlines():
        if not line.strip():
            continue
        try:
            parsed: JSONValue = json.loads(line)
        except (ValueError, RecursionError):
            error = error or "Invalid Codex JSON protocol"
            break
        if not isinstance(parsed, dict) or not isinstance(parsed.get("type"), str):
            error = error or "Invalid Codex JSON protocol"
            break
        kind = parsed["type"]
        if kind not in _KNOWN_EVENTS:
            error = error or "Unknown Codex event"
            break
        match kind:
            case "thread.started":
                value = parsed.get("thread_id")
                if not isinstance(value, str) or not value:
                    error = error or "Invalid Codex thread event"
                    break
                if resumed_id is not None and value != resumed_id:
                    error = error or "Codex thread identity mismatch"
                    break
                thread_id = value
                if resumed_id is None:
                    try:
                        _ = runner_state.save_session(config, session_id, "codex", value)
                    except SessionStateError:
                        error = error or "Invalid Codex thread id"
                        break
            case "turn.started":
                pending_turn = True
                terminal = False
            case "turn.completed":
                if not isinstance(parsed.get("usage"), dict):
                    error = error or "Invalid Codex turn completion"
                    break
                if pending_turn:
                    terminal = True
                pending_turn = False
            case "turn.failed":
                detail = parsed.get("error")
                message = detail.get("message") if isinstance(detail, dict) else None
                if not isinstance(message, str):
                    error = error or "Invalid Codex turn failure"
                    break
                compaction_failed = compaction_failed or _is_context_overflow(message)
                failed_after = failed_after or terminal
                pending_turn = False
            case "error":
                message = parsed.get("message")
                if not isinstance(message, str):
                    error = error or "Invalid Codex error event"
                    break
                compaction_failed = compaction_failed or _is_context_overflow(message)
                failed_after = failed_after or terminal
            case _:
                item = parsed.get("item")
                if not isinstance(item, dict) or not isinstance(item.get("type"), str):
                    error = error or "Invalid Codex item"
                    break
                if kind == "item.completed" and item["type"] == "agent_message":
                    text = item.get("text")
                    if not isinstance(text, str):
                        error = error or "Invalid Codex agent message"
                        break
                    reply = text
    if compaction_failed:
        error = error or "Context overflow"
    if thread_id is None:
        error = error or "Missing Codex thread id"
    if failed_after:
        error = error or "Codex turn failed"
    if not terminal:
        error = error or "Missing Codex turn completion"
    if not reply.strip():
        error = error or "Empty Codex reply"
    return TurnResult(
        result.success and error is None, reply, error, compaction_failed, result.exit_code,
    )


__all__ = ["run_turn"]
