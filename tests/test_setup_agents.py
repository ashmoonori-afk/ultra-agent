"""Setup agent detection, installation and login contracts, with fake CLIs."""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

from ultra_agent.models import RunnerKind
from ultra_agent.setup_agents import (
    AgentChoice,
    SetupAgentError,
    choose_agent,
    detect_agents,
    ensure_login,
    has_native_format,
    install_agent,
    is_windows_shim_path,
    login_status,
)

Stream = int | object | None
FakeRun = Callable[..., int]


def _write_executable(path: Path, shebang: str = "#!/bin/sh\nexit 0\n") -> Path:
    path.write_text(shebang, encoding="utf-8")
    path.chmod(0o755)
    return path


def test_detect_agents_finds_native_scripts(tmp_path: Path) -> None:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for name in ("codex", "claude", "omo"):
        _write_executable(bindir / name)
    detected = detect_agents(str(bindir))
    assert set(detected) == {"codex", "claude", "omo"}
    assert detected["codex"] == (bindir / "codex").resolve()


def test_detect_agents_ignores_absent_runner(tmp_path: Path) -> None:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    _write_executable(bindir / "codex")
    detected = detect_agents(str(bindir))
    assert set(detected) == {"codex"}
    assert detect_agents(str(tmp_path / "missing")) == {}


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (Path("/mnt/c/tools/codex"), True),
        (Path("/mnt/d/tools/claude"), True),
        (Path("/usr/local/bin/codex.cmd"), True),
        (Path("/usr/local/bin/codex.CMD"), True),
        (Path("/usr/local/bin/claude.exe"), True),
        (Path("/usr/local/bin/omo.bat"), True),
        (Path("/usr/local/bin/codex"), False),
        (Path("/mnt/tool/codex"), False),
        (Path("/mnt"), False),
    ],
)
def test_is_windows_shim_path(path: Path, expected: bool) -> None:
    assert is_windows_shim_path(path) is expected


@pytest.mark.parametrize(
    ("head", "expected"),
    [(b"#!", True), (b"\x7fELF", True), (b"MZ", False), (b"\x00\x00", False), (b"", False)],
)
def test_has_native_format(head: bytes, expected: bool) -> None:
    assert has_native_format(head) is expected


def test_detect_agents_rejects_cmd_suffix_and_non_executable_format(tmp_path: Path) -> None:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    shim = bindir / "codex.cmd"
    shim.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    shim.chmod(0o755)
    _write_executable(bindir / "claude", "MZ\x90\x00binary\n")
    detected = detect_agents(str(bindir))
    assert detected == {}


def test_choose_agent_auto_prefers_codex_then_claude_then_omo() -> None:
    codex = Path("/x/codex")
    assert choose_agent("auto", {"omo": Path("/x/omo")}) == "omo"
    assert choose_agent("auto", {"claude": codex, "omo": Path("/x/omo")}) == "claude"
    assert choose_agent("auto", {"codex": codex, "claude": Path("/x/claude")}) == "codex"


def test_choose_agent_auto_defaults_to_codex_when_none_detected() -> None:
    assert choose_agent("auto", {}) == "codex"


@pytest.mark.parametrize("runner", ["codex", "claude", "omo"])
def test_choose_agent_explicit_passes_through(runner: RunnerKind) -> None:
    assert choose_agent(runner, {}) == runner


def test_choose_agent_unknown_raises() -> None:
    with pytest.raises(SetupAgentError, match="Unknown runner"):
        choose_agent("gemini", {})


def _recording_run(calls: list[list[str]], code: int = 0) -> FakeRun:
    def run(
        argv: list[str],
        *,
        stdin: Stream = None,
        stdout: Stream = None,
        stderr: Stream = None,
    ) -> int:
        calls.append(argv)
        return code

    return run


@pytest.mark.parametrize(
    ("runner", "pipeline"),
    [
        ("codex", "curl -fsSL https://chatgpt.com/codex/install.sh | sh"),
        ("claude", "curl -fsSL https://claude.ai/install.sh | bash"),
    ],
)
def test_install_runs_exact_official_pipeline(
    tmp_path: Path, runner: RunnerKind, pipeline: str
) -> None:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    calls: list[list[str]] = []
    streams: list[object] = []

    def run(
        argv: list[str],
        *,
        stdin: Stream = None,
        stdout: Stream = None,
        stderr: Stream = None,
    ) -> int:
        calls.append(argv)
        streams.append(stdin)
        _write_executable(bindir / runner)
        return 0

    installed = install_agent(
        runner, run=run, env={"PATH": str(bindir), "HOME": str(tmp_path / "home")}
    )
    assert calls == [["bash", "-c", pipeline]]
    assert streams == [subprocess.DEVNULL]
    assert installed == (bindir / runner).resolve()


def test_install_raises_when_executable_still_missing(tmp_path: Path) -> None:
    calls: list[list[str]] = []
    with pytest.raises(SetupAgentError, match="was not found after installation"):
        install_agent(
            "codex",
            run=_recording_run(calls),
            env={"PATH": str(tmp_path / "empty"), "HOME": str(tmp_path / "home")},
        )
    assert len(calls) == 1


def test_install_omo_is_refused() -> None:
    calls: list[list[str]] = []
    with pytest.raises(SetupAgentError, match="omo cannot be installed"):
        install_agent("omo", run=_recording_run(calls))
    assert calls == []


def test_install_raises_when_installer_fails(tmp_path: Path) -> None:
    with pytest.raises(SetupAgentError, match="failed with exit code 3"):
        install_agent("codex", run=_recording_run([], code=3), env={"PATH": str(tmp_path)})


def test_login_status_reports_exit_code(tmp_path: Path) -> None:
    exe = _write_executable(tmp_path / "codex")
    choice = AgentChoice("codex", exe)
    ok_calls: list[list[str]] = []
    assert login_status(choice, run=_recording_run(ok_calls)) is True
    assert ok_calls == [[str(exe), "login", "status"]]
    bad_calls: list[list[str]] = []
    assert login_status(choice, run=_recording_run(bad_calls, code=1)) is False
    assert bad_calls == [[str(exe), "login", "status"]]


def test_login_status_claude_uses_auth_status(tmp_path: Path) -> None:
    exe = _write_executable(tmp_path / "claude")
    calls: list[list[str]] = []
    assert login_status(AgentChoice("claude", exe), run=_recording_run(calls)) is True
    assert calls == [[str(exe), "auth", "status"]]


def test_login_status_omo_is_always_logged_in(tmp_path: Path) -> None:
    exe = _write_executable(tmp_path / "omo")
    calls: list[list[str]] = []
    assert login_status(AgentChoice("omo", exe), run=_recording_run(calls)) is True
    assert calls == []


def test_ensure_login_noninteractive_raises_with_exact_command(tmp_path: Path) -> None:
    exe = _write_executable(tmp_path / "codex")
    with pytest.raises(SetupAgentError, match=f"run: {exe} login"):
        ensure_login(AgentChoice("codex", exe), interactive=False, run=_recording_run([], code=1))


class _LoginSession:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.logged_in = False

    def __call__(
        self,
        argv: list[str],
        *,
        stdin: Stream = None,
        stdout: Stream = None,
        stderr: Stream = None,
    ) -> int:
        self.calls.append(argv)
        if argv[-1] == "status":
            return 0 if self.logged_in else 1
        self.logged_in = True
        return 0


def test_ensure_login_interactive_logs_in_then_rechecks(tmp_path: Path) -> None:
    exe = _write_executable(tmp_path / "codex")
    run = _LoginSession()
    ensure_login(AgentChoice("codex", exe), interactive=True, run=run)
    assert run.logged_in is True
    assert run.calls == [
        [str(exe), "login", "status"],
        [str(exe), "login"],
        [str(exe), "login", "status"],
    ]


def test_ensure_login_returns_when_already_logged_in(tmp_path: Path) -> None:
    exe = _write_executable(tmp_path / "claude")
    calls: list[list[str]] = []
    ensure_login(AgentChoice("claude", exe), interactive=False, run=_recording_run(calls))
    assert calls == [[str(exe), "auth", "status"]]


def test_ensure_login_raises_when_interactive_login_incomplete(tmp_path: Path) -> None:
    exe = _write_executable(tmp_path / "codex")
    run = _recording_run([], code=1)
    with pytest.raises(SetupAgentError, match="did not complete"):
        ensure_login(AgentChoice("codex", exe), interactive=True, run=run)
