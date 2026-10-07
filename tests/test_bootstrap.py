"""Focused subprocess tests for the repo-root install.sh bootstrap.

They prove dependency selection, the owned installation and the installed setup
invocation without the network, a real provider or any system change: every PATH
command is a fixture process that records its arguments and materialises the
artefacts install.sh expects.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

INSTALL = Path(__file__).resolve().parents[1] / "install.sh"
DEFAULT_SOURCE = "https://github.com/ashmoonori-afk/ultra-agent/archive/refs/heads/main.zip"
_CANDIDATES = ("python3.14", "python3.13", "python3.12", "python3.11", "python3", "python")

TOOL = """\
import json
import os
import shutil
import sys
from pathlib import Path

args = sys.argv[1:]
log_path = os.environ.get("FAKE_LOG")
if log_path:
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({"tool": os.path.basename(sys.argv[0]), "args": args}) + "\\n")


def _install_launcher(directory: Path) -> None:
    target = directory / "bin" / "ultra-agent"
    shutil.copyfile(os.environ["FAKE_LAUNCHER"], target)
    target.chmod(0o755)


def _make_venv(directory: Path, with_pip: bool) -> None:
    binary = directory / "bin"
    binary.mkdir(parents=True, exist_ok=True)
    python = binary / "python"
    shutil.copyfile(os.path.realpath(__file__), python)
    python.chmod(0o755)
    if with_pip:
        pip = binary / "pip"
        pip.write_text("#!/bin/sh\\nexit 0\\n", encoding="utf-8")
        pip.chmod(0o755)


if "-c" in args:
    sys.exit(0 if os.environ.get("FAKE_PY_OK", "1") == "1" else 1)
if args[:3] == ["-m", "pip", "install"]:
    _install_launcher(Path(sys.argv[0]).resolve().parents[1])
    sys.exit(0)
if args[:2] == ["-m", "venv"]:
    if os.environ.get("FAKE_VENV_FAIL") == "1":
        sys.exit(1)
    _make_venv(Path(args[2]), with_pip=True)
    sys.exit(0)
if args[:2] == ["pip", "install"]:
    _install_launcher(Path(args[args.index("--python") + 1]).resolve().parents[1])
    sys.exit(0)
if args[:1] == ["venv"]:
    _make_venv(Path(args[-1]), with_pip=False)
    sys.exit(0)
sys.exit(0)
"""

LAUNCHER = """\
import json
import os
import sys

payload = {
    "argv": sys.argv[1:],
    "env": {
        name: os.environ.get(name)
        for name in (
            "ULTRA_AGENT_RUNNER",
            "ULTRA_AGENT_PLATFORM",
            "ULTRA_AGENT_BOT_TOKEN",
            "ULTRA_AGENT_CHANNEL",
            "ULTRA_AGENT_USER",
            "ULTRA_AGENT_NONINTERACTIVE",
            "ULTRA_AGENT_SERVICE",
        )
    },
    "stdin_is_tty": sys.stdin.isatty(),
}
with open(os.environ["UA_RECORD"], "a", encoding="utf-8") as handle:
    handle.write(json.dumps(payload) + "\\n")
print("fixture-setup-ok")
sys.exit(int(os.environ.get("UA_SETUP_EXIT", "0")))
"""


def _write_script(path: Path, body: str) -> Path:
    path.write_text(f"#!{sys.executable}\n{body}", encoding="utf-8")
    path.chmod(0o755)
    return path


class Fake:
    def __init__(self, tmp_path: Path) -> None:
        self.home = tmp_path / "home"
        self.home.mkdir()
        self.install_dir = tmp_path / "install"
        self.workspace = tmp_path / "workspace"
        self.log = tmp_path / "calls.jsonl"
        self.record = tmp_path / "setup-record.jsonl"
        bin_dir = tmp_path / "fakebin"
        bin_dir.mkdir()
        for name in (*_CANDIDATES, "uv"):
            _write_script(bin_dir / name, TOOL)
        launcher = _write_script(tmp_path / "launcher.py", LAUNCHER)
        self.env = {
            "HOME": str(self.home),
            "PATH": f"{bin_dir}{os.pathsep}/usr/bin{os.pathsep}/bin",
            "FAKE_LOG": str(self.log),
            "FAKE_LAUNCHER": str(launcher),
            "UA_RECORD": str(self.record),
            "ULTRA_AGENT_INSTALL_DIR": str(self.install_dir),
            "UA_WORKSPACE": str(self.workspace),
        }

    def environ(self, **overrides: str) -> dict[str, str]:
        return {**self.env, **overrides}

    def calls(self) -> list[dict[str, object]]:
        if not self.log.exists():
            return []
        return [json.loads(line) for line in self.log.read_text(encoding="utf-8").splitlines()]

    def records(self) -> list[dict[str, object]]:
        if not self.record.exists():
            return []
        return [json.loads(line) for line in self.record.read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def fake(tmp_path: Path) -> Iterator[Fake]:
    yield Fake(tmp_path)


def _run(env: dict[str, str], *argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", *argv, str(INSTALL)],
        env=env,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
    )


def test_install_sh_passes_bash_syntax_check() -> None:
    result = subprocess.run(["bash", "-n", str(INSTALL)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_installs_into_owned_venv_and_invokes_setup(fake: Fake) -> None:
    result = _run(fake.environ())
    assert result.returncode == 0, result.stderr
    assert (fake.install_dir / "venv" / "bin" / "ultra-agent").is_file()
    assert "setup complete" in result.stdout
    pip_calls = [c for c in fake.calls() if c["args"][:3] == ["-m", "pip", "install"]]
    assert pip_calls, "the package was never installed"
    assert pip_calls[-1]["args"][-1] == DEFAULT_SOURCE
    assert fake.records()[-1]["argv"] == ["--workspace", str(fake.workspace), "setup"]


def test_local_source_override_is_used(fake: Fake, tmp_path: Path) -> None:
    archive = tmp_path / "ultra-agent-main.zip"
    archive.write_bytes(b"fixture")
    result = _run(fake.environ(ULTRA_AGENT_SOURCE=str(archive)))
    assert result.returncode == 0, result.stderr
    pip_calls = [c for c in fake.calls() if c["args"][:3] == ["-m", "pip", "install"]]
    assert pip_calls[-1]["args"][-1] == str(archive)


def test_falls_back_to_uv_when_no_supported_python(fake: Fake) -> None:
    result = _run(fake.environ(FAKE_PY_OK="0"))
    assert result.returncode == 0, result.stderr
    assert any(c["tool"] == "uv" and c["args"][:1] == ["venv"] for c in fake.calls())
    assert any(c["tool"] == "uv" and c["args"][:2] == ["pip", "install"] for c in fake.calls())
    assert not any(c["args"][:3] == ["-m", "pip", "install"] for c in fake.calls())
    assert fake.records()[-1]["argv"][-1] == "setup"


def test_missing_venv_support_uses_uv_instead(fake: Fake) -> None:
    result = _run(fake.environ(FAKE_VENV_FAIL="1"))
    assert result.returncode == 0, result.stderr
    assert any(c["tool"] == "uv" and c["args"][:1] == ["venv"] for c in fake.calls())
    assert fake.records()[-1]["argv"][-1] == "setup"


def test_environment_contract_passed_and_token_never_leaks(fake: Fake) -> None:
    token = "super-secret-token-123"
    expected = {
        "ULTRA_AGENT_RUNNER": "codex",
        "ULTRA_AGENT_PLATFORM": "slack",
        "ULTRA_AGENT_BOT_TOKEN": token,
        "ULTRA_AGENT_CHANNEL": "C123",
        "ULTRA_AGENT_USER": "U456",
        "ULTRA_AGENT_NONINTERACTIVE": "1",
        "ULTRA_AGENT_SERVICE": "process",
    }
    result = _run(fake.environ(**expected))
    assert result.returncode == 0, result.stderr
    record = fake.records()[-1]
    assert record["env"] == expected
    assert record["stdin_is_tty"] is False
    assert token not in result.stdout
    assert token not in result.stderr
    assert token not in json.dumps([c["args"] for c in fake.calls()])
    assert token not in json.dumps(record["argv"])


def test_setup_failure_exits_nonzero_without_success(fake: Fake) -> None:
    result = _run(fake.environ(UA_SETUP_EXIT="7"))
    assert result.returncode == 7
    assert "setup complete" not in result.stdout
    assert "failed" in result.stderr


def test_inherited_xtrace_keeps_the_token_hidden(fake: Fake) -> None:
    token = "trace-secret-456"
    result = _run(fake.environ(ULTRA_AGENT_BOT_TOKEN=token), "-x")
    assert result.returncode == 0, result.stderr
    assert token not in result.stderr


def test_default_location_is_owned_under_home(fake: Fake) -> None:
    env = fake.environ()
    del env["ULTRA_AGENT_INSTALL_DIR"]
    del env["UA_WORKSPACE"]
    result = _run(env)
    assert result.returncode == 0, result.stderr
    root = fake.home / ".local" / "share" / "ultra-agent"
    assert (root / "venv" / "bin" / "ultra-agent").is_file()
    assert fake.records()[-1]["argv"] == ["--workspace", str(root / "workspace"), "setup"]


def test_repeat_run_reuses_the_installation(fake: Fake) -> None:
    assert _run(fake.environ()).returncode == 0
    assert _run(fake.environ()).returncode == 0
    assert len(fake.records()) == 2
    venv_calls = [c for c in fake.calls() if c["args"][:2] == ["-m", "venv"]]
    assert len(venv_calls) == 1
