"""Detect, install and log in the native agent CLIs used as runners.

Setup borrows the machine's existing ``codex``, ``claude`` or ``omo`` binary
when it is a native Linux executable, installs Codex (or Claude, on explicit
request) through the provider's official standalone installer, and drives the
provider's own login flow. It never reads or copies an authentication file: the
provider CLI owns its credentials and setup only asks whether it is logged in.

A Windows npm shim seen through WSL is not a native runner: it cannot take part
in the runtime's POSIX process ownership, so detection rejects it. Rejection is
decided by :func:`is_windows_shim_path` (a pure path predicate) and by the first
two bytes of the file, which must be a ``#!`` shebang or an ELF magic number.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Final, Protocol, TypeAlias

from ultra_agent.models import RunnerKind

TTY: Final = "/dev/tty"
"""The controlling terminal, opened by the default runner instead of piped stdin."""

_RUNNERS: Final[tuple[RunnerKind, ...]] = ("codex", "claude", "omo")

_INSTALL_PIPELINES: Final[dict[str, str]] = {
    "codex": "curl -fsSL https://chatgpt.com/codex/install.sh | sh",
    "claude": "curl -fsSL https://claude.ai/install.sh | bash",
}

_LOGIN_ARGV: Final[dict[str, tuple[str, ...]]] = {
    "codex": ("login",),
    "claude": ("auth", "login"),
}

_STATUS_ARGV: Final[dict[str, tuple[str, ...]]] = {
    "codex": ("login", "status"),
    "claude": ("auth", "status"),
}

_STATUS_TIMEOUT_SECONDS: Final = 30.0
_INSTALL_TIMEOUT_SECONDS: Final = 600.0


class SetupAgentError(RuntimeError):
    """A runner could not be detected, installed or logged in."""


Stream: TypeAlias = int | IO[Any] | str | None


class Run(Protocol):
    """Injectable command runner: ``(argv, *, stdin, stdout, stderr) -> exit code``."""

    def __call__(
        self,
        argv: list[str],
        *,
        stdin: Stream = None,
        stdout: Stream = None,
        stderr: Stream = None,
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class AgentChoice:
    """The selected runner and the executable that implements it."""

    runner: RunnerKind
    executable: Path


def is_windows_shim_path(real: Path) -> bool:
    """True when a resolved path is a Windows shim, not a native Linux binary.

    Two shapes are rejected: a path reached through the WSL Windows mount
    (``/mnt/<letter>/...``) and a Windows script suffix (``.cmd``/``.exe``/``.bat``).
    """
    parts = real.parts
    if (
        len(parts) >= 3
        and parts[0] == "/"
        and parts[1] == "mnt"
        and len(parts[2]) == 1
        and parts[2].isalpha()
    ):
        return True
    return real.suffix.lower() in {".cmd", ".exe", ".bat"}


def has_native_format(head: bytes) -> bool:
    """True when the first bytes mark a shebang script or an ELF executable."""
    return head.startswith(b"#!") or head.startswith(b"\x7fELF")


def _is_native_executable(real: Path) -> bool:
    if is_windows_shim_path(real):
        return False
    try:
        with real.open("rb") as handle:
            head = handle.read(4)
    except OSError:
        return False
    return has_native_format(head)


def detect_agents(env_path: str | None = None) -> dict[RunnerKind, Path]:
    """Find native ``codex``, ``claude`` and ``omo`` executables on ``env_path``."""
    found: dict[RunnerKind, Path] = {}
    for runner in _RUNNERS:
        which = shutil.which(runner, path=env_path)
        if which is None:
            continue
        real = Path(which).resolve()
        if _is_native_executable(real):
            found[runner] = real
    return found


def choose_agent(requested: str, detected: Mapping[RunnerKind, Path]) -> RunnerKind:
    """Resolve a request to a runner; ``auto`` prefers codex, then claude, then omo."""
    if requested == "auto":
        for candidate in _RUNNERS:
            if candidate in detected:
                return candidate
        return "codex"
    if requested in _RUNNERS:
        return requested
    raise SetupAgentError(
        f"Unknown runner {requested!r}; expected auto, codex, claude or omo"
    )


def _subprocess_runner(timeout: float | None) -> Run:
    """Build a runner that shells out with a bounded timeout, ``None`` meaning open."""

    def run(
        argv: list[str],
        *,
        stdin: Stream = None,
        stdout: Stream = None,
        stderr: Stream = None,
    ) -> int:
        opened: list[IO[Any]] = []

        def resolve(target: Stream) -> int | IO[Any] | None:
            if isinstance(target, str):
                handle = open(target, "r+", buffering=1)
                opened.append(handle)
                return handle
            return target

        try:
            completed = subprocess.run(
                argv,
                stdin=resolve(stdin),
                stdout=resolve(stdout),
                stderr=resolve(stderr),
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return 124
        finally:
            for handle in opened:
                handle.close()
        return completed.returncode

    return run


_STATUS_RUN: Final[Run] = _subprocess_runner(_STATUS_TIMEOUT_SECONDS)
_INSTALL_RUN: Final[Run] = _subprocess_runner(_INSTALL_TIMEOUT_SECONDS)
_LOGIN_RUN: Final[Run] = _subprocess_runner(None)


def _post_install_path(env: Mapping[str, str] | None) -> str:
    source = env if env is not None else os.environ
    path = source.get("PATH", "")
    home = source.get("HOME", str(Path.home()))
    local_bin = str(Path(home) / ".local" / "bin")
    return f"{path}{os.pathsep}{local_bin}" if path else local_bin


def install_agent(
    runner: RunnerKind,
    *,
    run: Run = _INSTALL_RUN,
    env: Mapping[str, str] | None = None,
) -> Path:
    """Install a missing runner through its official installer and return its path."""
    if runner == "omo":
        raise SetupAgentError(
            "omo cannot be installed by setup; install omo first, then re-run setup"
        )
    pipeline = _INSTALL_PIPELINES[runner]
    code = run(
        ["bash", "-c", pipeline],
        stdin=subprocess.DEVNULL,
        stdout=None,
        stderr=None,
    )
    if code != 0:
        raise SetupAgentError(f"Installing {runner} failed with exit code {code}")
    detected = detect_agents(_post_install_path(env))
    if runner not in detected:
        raise SetupAgentError(
            f"{runner} was not found after installation; check the installer output"
        )
    return detected[runner]


def login_status(choice: AgentChoice, *, run: Run = _STATUS_RUN) -> bool:
    """Whether the provider CLI reports an active session; output is ignored.

    omo exposes no documented status command, so its status is reported as logged
    in and setup must not block on it.
    """
    if choice.runner == "omo":
        return True
    argv = [str(choice.executable), *_STATUS_ARGV[choice.runner]]
    return (
        run(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        == 0
    )


def _login_argv(choice: AgentChoice) -> list[str]:
    return [str(choice.executable), *_LOGIN_ARGV[choice.runner]]


def ensure_login(choice: AgentChoice, *, interactive: bool, run: Run = _LOGIN_RUN) -> None:
    """Ensure an active session, driving the provider login flow when interactive."""
    if login_status(choice, run=run):
        return
    command = " ".join(_login_argv(choice))
    if not interactive:
        raise SetupAgentError(f"Not logged in to {choice.runner}; run: {command}")
    run(_login_argv(choice), stdin=TTY, stdout=TTY, stderr=TTY)
    if not login_status(choice, run=run):
        raise SetupAgentError(f"Login for {choice.runner} did not complete; run: {command}")


__all__ = [
    "AgentChoice",
    "Run",
    "SetupAgentError",
    "choose_agent",
    "detect_agents",
    "ensure_login",
    "has_native_format",
    "install_agent",
    "is_windows_shim_path",
    "login_status",
]
