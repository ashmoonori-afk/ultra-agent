"""Focused end-to-end tests for the ultra-agent CLI.

Every test drives the real :func:`ultra_agent.cli.main` against a real
workspace: a real SQLite :class:`~ultra_agent.store.Store`, a real Mnemosyne
:class:`~ultra_agent.memory.Memory` and a real :class:`~ultra_agent.runtime.Runtime`.
The only injection is a loopback transport at the messenger network boundary,
supplied through the ``runtime_factory`` seam; no runtime is faked.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from ultra_agent import service
from ultra_agent.cli import _signal_stop, main
from ultra_agent.config import load_config
from ultra_agent.http import TransportError
from ultra_agent.messenger import Messenger
from ultra_agent.models import Config, Delivery, Submission
from ultra_agent.runner_lock import RunnerLock
from ultra_agent.runtime import Runtime
from ultra_agent.store import Store

_REPO = Path(__file__).resolve().parents[1]

_TOML = """command = ["omo"]
tools = []
verification_command = []

[host]
min_memory_percent = 0.1
min_disk_gib = 0.1
max_lanes = 20
slot_dir = "{slot}"
"""

_TOML_HOLD = """command = ["omo"]
tools = []

[host]
min_disk_gib = 1000000000
slot_dir = "{slot}"
"""

_MESSENGERS = """
[[messengers]]
platform = "slack"
token_env = "UA_TEST_SLACK_TOKEN"
users = ["U1"]
channels = ["C1"]

[[messengers]]
platform = "telegram"
token_env = "UA_TEST_TELEGRAM_TOKEN"
users = ["42"]
channels = ["99"]

[[messengers]]
platform = "discord"
token_env = "UA_TEST_DISCORD_TOKEN"
users = ["U9"]
channels = ["C1"]
"""


class _LoopbackTransport:
    """A real messenger transport double at the network boundary."""

    def __init__(self, error: TransportError | None = None) -> None:
        self._error = error
        self.sent: list[Delivery] = []

    def identity(self) -> str:
        return "self"

    def poll(self) -> list[str]:
        return ["inbound-1"]

    def send(self, delivery: Delivery, /) -> str:
        if self._error is not None:
            raise self._error
        self.sent.append(delivery)
        return "remote-1"

    def edit(self, channel: str, message_id: str, text: str, /) -> str:
        return message_id

    def typing(self, channel: str, /, thread_id: str | None = None) -> None:
        return None


def _write_workspace(tmp_path: Path, toml: str = _TOML, extra: str = "") -> Path:
    ws = tmp_path / "ws"
    assert main(["--workspace", str(ws), "init"]) == 0
    text = toml.format(slot=tmp_path / "slots") + extra
    (ws / "ultra-agent.toml").write_text(text, encoding="utf-8")
    return ws


def _cli(ws: Path, *args: str) -> int:
    return main(["--workspace", str(ws), *args])


def _cli_with(ws: Path, factory: object, *args: str) -> int:
    assert callable(factory)
    return main(["--workspace", str(ws), *args], runtime_factory=factory)


def _last_json(captured: str) -> dict[str, object]:
    parsed: object = json.loads(captured.strip().splitlines()[-1])
    assert isinstance(parsed, dict)
    return parsed


def _out(capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    return _last_json(capsys.readouterr().out)


def _factory(transport: _LoopbackTransport):
    def build(config: Config) -> Runtime:
        messenger = Messenger(config, Store(config.db_path), transports={"discord": transport})
        return Runtime(config, messenger=messenger)

    return build


def test_failed_live_check_returns_nonzero_closure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _write_workspace(tmp_path)
    runtime = Runtime(load_config(ws), clock=lambda: 300)
    runtime.registry.launch("worker", "worker", "command")
    runtime.completion.claim("worker", "claimed", 0)
    code = main(
        ["--workspace", str(ws), "lane", "close", "worker"],
        runtime_factory=lambda config: runtime,
    )
    assert code == 1
    assert _out(capsys)["ok"] is False


def test_host_can_launch_reports_occupied_slot(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from ultra_agent.host import HeavySlot

    ws = _write_workspace(tmp_path)
    with HeavySlot(load_config(ws).host):
        assert _cli(ws, "host", "can-launch") == 1
    assert _out(capsys)["decision"] == "HOLD"


def test_init_memory_and_status_roundtrip(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _write_workspace(tmp_path)
    assert _cli(ws, "skill-path") == 0
    assert Path(str(_out(capsys)["skill_file"])).is_file()

    assert _cli(ws, "memory", "write", "greeting", "hello world", "--source", "cli") == 0
    assert _out(capsys)["ok"] is True

    assert _cli(ws, "memory", "search", "hello") == 0
    assert "hello world" in str(_out(capsys)["result"])

    assert _cli(ws, "memory", "get", "greeting") == 0
    assert "hello world" in str(_out(capsys)["content"])

    assert _cli(ws, "memory", "get", "absent") == 1
    assert _out(capsys)["error"]

    assert _cli(ws, "status") == 0
    status = _out(capsys)
    assert status["jobs"] == [] and status["lanes"]


def test_submit_approve_then_status(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    ws = _write_workspace(tmp_path)
    assert _cli(ws, "submit", "do the work") == 0
    job = _out(capsys)["job"]
    assert isinstance(job, dict) and job["status"] == "awaiting_approval"
    job_id = str(job["id"])

    assert _cli(ws, "status") == 0
    assert job_id in _out(capsys)["pending_approval"]

    assert _cli(ws, "approve", job_id) == 0
    assert _out(capsys)["job"]["status"] == "queued"


def test_resolve_retry_and_resend_require_acknowledgements(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _write_workspace(tmp_path)
    store = Store(load_config(ws).db_path)
    job_id = store.enqueue(Submission(source_key="t1", prompt="risky", approved=True))
    claimed = store.claim()
    assert claimed is not None and claimed.id == job_id
    assert store.recover_interrupted() == 1

    assert _cli(ws, "retry", job_id) == 1
    assert "acknowledge-uncertain-effects" in str(_out(capsys)["error"])

    assert _cli(ws, "resolve", job_id) == 1
    assert _out(capsys)["error"]

    assert _cli(ws, "retry", job_id, "--acknowledge-uncertain-effects") == 0
    result = _out(capsys)
    assert result["resolved_original"] is True
    assert result["job"]["status"] == "awaiting_approval"
    assert result["job"]["retry_of"] == job_id

    assert _cli(ws, "resend", job_id) == 1
    assert "acknowledge-possible-duplicate" in str(_out(capsys)["error"])


def test_once_zero_without_work_and_nonzero_on_hold(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _write_workspace(tmp_path)
    assert _cli(ws, "once") == 0
    assert _out(capsys)["ok"] is True

    held = _write_workspace(tmp_path / "held", toml=_TOML_HOLD)
    assert _cli(held, "once") == 1
    result = _out(capsys)
    assert result["ok"] is False
    assert result["tick"]["hold"]


def test_lane_launch_rejects_unknown_done_and_respects_window(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _write_workspace(tmp_path)

    assert _cli(ws, "lane", "launch", "alpha", "--role", "dev", "--done", "bogus:thing") == 1
    assert _out(capsys)["error"]

    assert _cli(ws, "lane", "list") == 0
    keys = [lane["key"] for lane in _out(capsys)["lanes"]]
    assert "alpha" not in keys

    assert _cli(ws, "lane", "launch", "alpha", "--role", "dev", "--done", "pr:OWNER/REPO#1") == 0
    assert _out(capsys)["lane"]["status"] == "working"

    assert _cli(ws, "lane", "claim", "alpha", "--evidence", "tests pass") == 0
    claim = _out(capsys)["lane"]
    assert claim["status"] == "done-claimed"
    assert claim["deadline"] == pytest.approx(claim["claim_at"] + 300.0)

    assert _cli(ws, "lane", "close", "alpha") == 1
    assert "objection window" in str(_out(capsys)["error"])

    assert _cli(ws, "lane", "object", "alpha", "--reason", "not merged") == 0
    assert _out(capsys)["lane"]["status"] == "not-done"

    assert _cli(ws, "lane", "sync") == 0
    assert "report" in _out(capsys)

    assert _cli(ws, "lane", "watch", "alpha") == 0
    assert _out(capsys)["snapshot"]["history"]


def test_host_and_doctor_report_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    ws = _write_workspace(tmp_path)

    assert _cli(ws, "host", "status") == 0
    assert _out(capsys)["sample"]

    code = _cli(ws, "host", "can-launch")
    decision = _out(capsys)
    assert code == (0 if decision["ok"] else 1)
    assert decision["decision"] in ("READY", "HOLD")

    assert _cli(ws, "doctor") == 0
    doctor = _out(capsys)
    assert doctor["live"] is None
    assert "not checked" in str(doctor["channel_access"])


def test_messenger_unsupported_capabilities_are_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _write_workspace(tmp_path, extra=_MESSENGERS)

    assert _cli(ws, "messenger", "typing", "slack", "--to", "C1") == 1
    assert "typing" in str(_out(capsys)["error"])

    assert _cli(ws, "messenger", "presence", "telegram", "--seconds", "0") == 1
    assert "presence" in str(_out(capsys)["error"])


def test_messenger_send_uses_loopback_and_reports_truthful_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _write_workspace(tmp_path, extra=_MESSENGERS)
    transport = _LoopbackTransport()

    send = ("messenger", "send", "discord", "--to", "C1", "--text", "hi")
    assert _cli_with(ws, _factory(transport), *send) == 0
    sent = _out(capsys)
    assert sent["state"] == "sent" and sent["runner"] == "acquired"
    assert sent["statuses"][sent["delivery_ids"][0]] == "sent"
    assert len(transport.sent) == 1

    inbound = ("messenger", "inbound", "discord", "--once")
    assert _cli_with(ws, _factory(_LoopbackTransport()), *inbound) == 0
    assert _out(capsys)["job_ids"] == ["inbound-1"]

    assert _cli(ws, "messenger", "inbound", "discord") == 1
    assert "one of --once or --loop" in str(_out(capsys)["error"])


def test_messenger_send_reports_uncertain_delivery(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _write_workspace(tmp_path, extra=_MESSENGERS)
    transport = _LoopbackTransport(error=TransportError("ambiguous"))
    send = ("messenger", "send", "discord", "--to", "C1", "--text", "hi")

    assert _cli_with(ws, _factory(transport), *send) == 1
    result = _out(capsys)
    assert result["ok"] is False
    assert set(result["statuses"].values()) == {"uncertain"}


def test_messenger_send_reports_queued_when_runner_lock_is_held(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _write_workspace(tmp_path, extra=_MESSENGERS)
    transport = _LoopbackTransport()
    held = RunnerLock(load_config(ws).state_dir / "runner.lock")
    with held:
        assert _cli_with(
            ws, _factory(transport), "messenger", "send", "discord", "--to", "C1", "--text", "hi"
        ) == 0
    result = _out(capsys)
    assert result["state"] == "queued" and result["runner"] == "busy"
    assert transport.sent == []


def test_relay_push_unknown_peer_is_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _write_workspace(tmp_path)
    assert _cli(ws, "relay", "push", "nope", "--file", "status.log") == 1
    assert "unknown relay peer" in str(_out(capsys)["error"])


def test_service_render_and_install(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _write_workspace(tmp_path)

    assert _cli(ws, "service", "render") == 0
    rendered = capsys.readouterr().out
    assert "ExecStart=" in rendered and "Restart=on-failure" in rendered

    console_dir = tmp_path / "console"
    console_dir.mkdir()
    console = console_dir / "ultra-agent"
    console.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    console.chmod(0o755)
    monkeypatch.setenv("PATH", f"{console_dir}{os.pathsep}{os.environ['PATH']}")

    assert _cli(ws, "service", "install", "--unit-dir", str(tmp_path / "units")) == 0
    installed = _out(capsys)
    assert Path(str(installed["unit_path"])).is_file()
    assert service.UNIT_NAME in str(installed["unit_path"])


def test_keeper_runs_fixed_argv(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    ws = _write_workspace(tmp_path)
    assert _cli(ws, "keeper", "--cwd", str(ws), sys.executable, "-c", "raise SystemExit(0)") == 0
    assert _out(capsys)["exit_code"] == 0


def test_signal_guard_restores_handlers_and_sets_stop() -> None:
    stop = threading.Event()
    before = signal.getsignal(signal.SIGINT)
    with pytest.raises(KeyboardInterrupt):
        with _signal_stop(stop):
            assert signal.getsignal(signal.SIGINT) is not before
            signal.raise_signal(signal.SIGINT)
    assert stop.is_set()
    assert signal.getsignal(signal.SIGINT) == before


def test_module_entry_point_invokes_real_cli(tmp_path: Path) -> None:
    ws = _write_workspace(tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "ultra_agent", "--workspace", str(ws), "status"],
        cwd=_REPO,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0
    assert json.loads(result.stdout)["ok"] is True
