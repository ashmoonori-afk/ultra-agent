"""Thin driver facade: process boundary, omo turn parsing and stable exports."""

from __future__ import annotations

from pathlib import Path

from ultra_agent import runner_omo
from ultra_agent.models import Config, TurnResult
from ultra_agent.process import (
    MAX_OUTPUT_BYTES,
    MAX_PROMPT_BYTES,
    child_environment,
    run_command,
    run_verification,
)


def run_turn(
    config: Config, session_id: str, prompt: str, *,
    cwd: Path | None = None, pass_fds: tuple[int, ...] = (),
) -> TurnResult:
    """Run one bounded agent turn; only the omo backend is wired so far."""
    return runner_omo.run_turn(config, session_id, prompt, cwd=cwd, pass_fds=pass_fds)


__all__ = [
    "MAX_OUTPUT_BYTES",
    "MAX_PROMPT_BYTES",
    "child_environment",
    "run_command",
    "run_turn",
    "run_verification",
]
