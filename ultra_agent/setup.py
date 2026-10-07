"""One-command setup: pick a native agent CLI, configure the workspace, start it."""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

from ultra_agent import service
from ultra_agent.config import ConfigError, load_config
from ultra_agent.models import Config, RunnerKind
from ultra_agent.setup_agents import (
    AgentChoice,
    SetupAgentError,
    choose_agent,
    detect_agents,
    ensure_login,
    install_agent,
)
from ultra_agent.setup_config import collect_inputs, write_setup
from ultra_agent.startup import load_service_env, start_background

ServiceMode = Literal["systemd", "process"]

_INPUT_NAMES: Final = frozenset({
    "ULTRA_AGENT_RUNNER", "ULTRA_AGENT_PLATFORM", "ULTRA_AGENT_BOT_TOKEN",
    "ULTRA_AGENT_CHANNEL", "ULTRA_AGENT_USER", "ULTRA_AGENT_NONINTERACTIVE",
    "ULTRA_AGENT_SERVICE", "ULTRA_AGENT_API_BASE",
})
_TRUE: Final = frozenset({"1", "true", "yes"})


@dataclass(frozen=True, slots=True)
class SetupResult:
    workspace: Path
    runner: RunnerKind
    executable: Path
    service: ServiceMode
    ready: bool
    already_running: bool
    pid: int | None


def _interactive(environ: Mapping[str, str]) -> bool:
    if environ.get("ULTRA_AGENT_NONINTERACTIVE", "").lower() in _TRUE:
        return False
    try:
        with open("/dev/tty", "rb"):
            return True
    except OSError:
        return False


def _existing(workspace: Path) -> Config | None:
    try:
        config = load_config(workspace)
    except ConfigError:
        return None
    return config if config.messengers or config.runner != "omo" else None


def _select(workspace: Path, environ: Mapping[str, str]) -> AgentChoice:
    requested = environ.get("ULTRA_AGENT_RUNNER", "auto") or "auto"
    configured = _existing(workspace)
    if configured is not None:
        if requested not in ("auto", configured.runner):
            raise SetupAgentError(
                f"workspace already uses {configured.runner}; refusing to switch to "
                f"{requested} without an explicit rotation"
            )
        located = shutil.which(configured.command[0])
        if located is None:
            raise SetupAgentError(f"configured runner command {configured.command[0]!r} is missing")
        return AgentChoice(configured.runner, Path(located).resolve())
    detected = detect_agents(environ.get("PATH"))
    runner = choose_agent(requested, detected)
    executable = detected.get(runner) or install_agent(runner, env=environ)
    return AgentChoice(runner, executable)


def _service_mode(environ: Mapping[str, str]) -> ServiceMode:
    requested = environ.get("ULTRA_AGENT_SERVICE", "auto") or "auto"
    match requested:
        case "systemd" | "process":
            return requested
        case "auto":
            return "systemd" if _systemd_available() else "process"
        case _:
            raise SetupAgentError("ULTRA_AGENT_SERVICE must be auto, systemd or process")


def _systemd_available() -> bool:
    if shutil.which("systemctl") is None:
        return False
    probe = subprocess.run(
        ("systemctl", "--user", "is-system-running"),
        capture_output=True, text=True, timeout=10, check=False,
    )
    return probe.stdout.strip() in ("running", "degraded")


def run_setup(workspace: Path, environ: Mapping[str, str]) -> SetupResult:
    """Configure and start the workspace; ready is true only after a real first tick."""
    interactive = _interactive(environ)
    choice = _select(workspace, environ)
    ensure_login(choice, interactive=interactive)
    configured = _existing(workspace)
    stored = load_service_env(configured) if configured is not None else {}
    if configured is not None and all(m.token_env in stored for m in configured.messengers):
        config = configured
    else:
        inputs = collect_inputs(environ, interactive=interactive, tty=None)
        config = write_setup(workspace, inputs, choice.runner, choice.executable)
    mode = _service_mode(environ)
    if mode == "systemd":
        # Type=notify: `enable --now` returns only after the runtime reports READY.
        service.install_service(config, enable=True)
        return SetupResult(
            config.workspace, choice.runner, choice.executable, mode, True, False, None,
        )
    child = {name: value for name, value in os.environ.items() if name not in _INPUT_NAMES}
    started = start_background(config, child)
    return SetupResult(
        config.workspace, choice.runner, choice.executable, mode,
        started.ready, started.already_running, started.pid,
    )
