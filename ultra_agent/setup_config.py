"""Collect setup inputs and write the private workspace configuration.

Two responsibilities, both at the installation boundary:

* :func:`collect_inputs` reads the messenger platform, bot token, channel and
  allowed sender id from named environment variables, or - only when explicitly
  interactive - prompts on the controlling terminal. The token is read without
  echo and never leaves this module in a prompt, a log or an exception message.
* :func:`write_setup` initializes the workspace, writes ``ultra-agent.toml``
  through the strict production parser and stores the token in the private
  ``state/service.env`` file (mode 0600) using the ``NAME=<JSON string>`` format
  consumed by :func:`ultra_agent.startup.load_service_env`.

An existing configuration with a messenger or a non-default runner is preserved
byte-for-byte; only a missing token assignment is added. Secrets never appear in
``ultra-agent.toml``, in exception messages, in ``repr`` or in any log.
"""

from __future__ import annotations

import getpass
import json
import os
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

from ultra_agent.config import MAX_ID_LENGTH, Config, init_workspace, load_config
from ultra_agent.models import Platform, RunnerKind
from ultra_agent.startup import load_service_env

_PLATFORM_ENV = "ULTRA_AGENT_PLATFORM"
_TOKEN_ENV = "ULTRA_AGENT_BOT_TOKEN"
_CHANNEL_ENV = "ULTRA_AGENT_CHANNEL"
_USER_ENV = "ULTRA_AGENT_USER"
_API_BASE_ENV = "ULTRA_AGENT_API_BASE"

_REQUIRED: tuple[tuple[str, str], ...] = (
    ("platform", _PLATFORM_ENV),
    ("token", _TOKEN_ENV),
    ("channel", _CHANNEL_ENV),
    ("user", _USER_ENV),
)

_PROMPTS: dict[str, str] = {
    "platform": "Messenger platform (discord/telegram/slack): ",
    "token": "Bot token (hidden): ",
    "channel": "Channel or chat id: ",
    "user": "Allowed sender id: ",
}


class SetupConfigError(RuntimeError):
    """Raised when setup input is missing or invalid, never echoing a secret."""


@dataclass(frozen=True, slots=True)
class SetupInputs:
    """Collected messenger settings; the token is excluded from every ``repr``."""

    platform: Platform
    token: str = field(repr=False)
    channel: str
    user: str
    api_base: str | None = None


def collect_inputs(
    environ: Mapping[str, str],
    *,
    interactive: bool,
    tty: TextIO | None,
) -> SetupInputs:
    """Return validated setup inputs from the environment or the terminal.

    Noninteractive calls never prompt: any required value that is absent or empty
    raises :class:`SetupConfigError` naming the missing environment variables,
    never their values. Interactive calls prompt only for absent required values
    and read the token without echo. ``ULTRA_AGENT_API_BASE`` is a test-only
    endpoint override and is never prompted for.
    """
    missing = [name for _, name in _REQUIRED if not environ.get(name)]
    if missing and not interactive:
        raise SetupConfigError(
            "missing required environment variable(s): " + ", ".join(missing)
        )
    if missing:
        with _terminal(tty) as terminal:
            return _prompt_inputs(environ, terminal)
    return _assemble(_from_env(environ), environ.get(_API_BASE_ENV))


def _from_env(environ: Mapping[str, str]) -> dict[str, str]:
    return {key: environ.get(name, "") for key, name in _REQUIRED}


@contextmanager
def _terminal(tty: TextIO | None) -> Generator[TextIO]:
    """Yield the caller's tty stream, or the controlling terminal when absent."""
    if tty is not None:
        yield tty
        return
    try:
        handle = open("/dev/tty", "r+", encoding="utf-8")
    except OSError:
        raise SetupConfigError(
            "no terminal available for interactive setup; set "
            f"{_PLATFORM_ENV}, {_TOKEN_ENV}, {_CHANNEL_ENV} and {_USER_ENV} instead"
        ) from None
    try:
        yield handle
    finally:
        handle.close()


def _prompt_inputs(environ: Mapping[str, str], tty: TextIO) -> SetupInputs:
    values: dict[str, str] = {}
    for key, name in _REQUIRED:
        provided = environ.get(name)
        if provided:
            values[key] = provided
        elif key == "token":
            values[key] = getpass.getpass(_PROMPTS[key], stream=tty)
        else:
            values[key] = _read_line(tty, _PROMPTS[key])
    return _assemble(values, environ.get(_API_BASE_ENV))


def _read_line(tty: TextIO, prompt: str) -> str:
    tty.write(prompt)
    tty.flush()
    return tty.readline().strip()


def _assemble(values: Mapping[str, str], api_base: str | None) -> SetupInputs:
    inputs = SetupInputs(
        platform=_validate_platform(values.get("platform", "")),
        token=_validate_token(values.get("token", "")),
        channel=_validate_identifier(values.get("channel", ""), "channel"),
        user=_validate_identifier(values.get("user", ""), "user"),
        api_base=api_base if api_base else None,
    )
    return inputs


def _validate_platform(value: str) -> Platform:
    match value:
        case "discord" | "telegram" | "slack":
            return value
        case _:
            raise SetupConfigError("platform must be one of discord, telegram or slack")


def _validate_token(token: str) -> str:
    if not token:
        raise SetupConfigError("token must not be empty")
    if any(character.isspace() for character in token) or "\x00" in token:
        raise SetupConfigError("token must not contain whitespace or control characters")
    return token


def _validate_identifier(value: str, where: str) -> str:
    if not value:
        raise SetupConfigError(f"{where} must not be empty")
    if len(value) > MAX_ID_LENGTH:
        raise SetupConfigError(f"{where} must be at most {MAX_ID_LENGTH} characters")
    if any(character.isspace() for character in value):
        raise SetupConfigError(f"{where} must not contain whitespace")
    return value


def write_setup(
    config_workspace: Path,
    inputs: SetupInputs,
    runner: RunnerKind,
    executable: Path,
) -> Config:
    """Initialize the workspace and write the configuration and private token.

    An existing configuration that already names a messenger or a non-default
    runner is preserved unchanged; only a missing token assignment in
    ``state/service.env`` is added. Otherwise a fresh ``ultra-agent.toml`` is
    written atomically and re-parsed through the production loader, so the
    messenger endpoint override is validated by the same strict rules as any
    other configuration. The returned :class:`Config` is the parsed result.
    """
    command = _absolute_executable(executable)
    config = init_workspace(config_workspace)
    config.workspace.chmod(0o700)
    if not config.messengers and config.runner == "omo":
        _replace_atomic(config.config_path, _render(inputs, runner, command))
        config = load_config(config.workspace)
    _ensure_token(config, _token_env(inputs.platform), inputs.token)
    return config


def _absolute_executable(executable: Path) -> str:
    text = os.fspath(executable)
    if not Path(text).is_absolute():
        raise SetupConfigError("runner executable must be an absolute path")
    return text


def _token_env(platform: Platform) -> str:
    return f"ULTRA_AGENT_{platform.upper()}_TOKEN"


def _render(inputs: SetupInputs, runner: RunnerKind, command: str) -> str:
    lines = [
        "# Generated by ultra-agent setup. Secrets never appear here.",
        f"command = {json.dumps([command])}",
        f"runner = {json.dumps(runner)}",
        "tools = []",
        "verification_command = []",
        'writing_style = ""',
        "",
        "[host]",
        "min_memory_percent = 20.0",
        "min_disk_gib = 1.0",
        "max_lanes = 20",
        "heavy_slots = 1",
        "",
        "[[messengers]]",
        f"platform = {json.dumps(inputs.platform)}",
        f"token_env = {json.dumps(_token_env(inputs.platform))}",
        f"users = {json.dumps([inputs.user])}",
        f"channels = {json.dumps([inputs.channel])}",
    ]
    if inputs.api_base is not None:
        lines.append(f"base_url = {json.dumps(inputs.api_base)}")
    lines.append("enabled = true")
    return "\n".join(lines) + "\n"


def _ensure_token(config: Config, name: str, token: str) -> None:
    if name in load_service_env(config):
        return
    path = config.state_dir / "service.env"
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    if existing and not existing.endswith("\n"):
        existing += "\n"
    _replace_atomic(path, existing + f"{name}={json.dumps(token)}\n")


def _replace_atomic(path: Path, text: str) -> None:
    """Write ``text`` privately through a temp file, then rename it into place."""
    temporary = path.with_name(path.name + ".setup.tmp")
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        os.fchmod(output.fileno(), 0o600)
        output.write(text)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
