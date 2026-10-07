"""Behavior tests for persistent automation ticks.

Every test drives the real :class:`~ultra_agent.automation` functions with an
injected clock and a real SQLite store reopened where persistence matters, plus
real files on disk. The command-monitor and probe tests run real subprocesses to
prove bounded output, credential isolation and full process-group cleanup; their
process trees end on their own once the group is killed, synchronized through
socket closure rather than a sleep.
"""

from __future__ import annotations

import os
import socket
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ultra_agent.automation import (
    ProbeResult,
    run_probe,
    tick_automation,
    tick_monitors,
    tick_relays,
    tick_schedules,
)
from ultra_agent.models import Config, MessengerConfig, Monitor, Relay, Schedule
from ultra_agent.store import Store


def _store(tmp_path: Path) -> Store:
    return Store(tmp_path / "runtime.db")


def _config(tmp_path: Path, **kwargs: object) -> Config:
    return Config(workspace=tmp_path, **kwargs)


def _complete(store: Store, job_id: str) -> None:
    job = store.get_job(job_id)
    if job.status == "awaiting_approval":
        store.approve(job_id)
    store.claim()
    store.finish(job_id, "completed")


def test_interval_schedule_coalesces_and_never_repeats(tmp_path: Path) -> None:
    schedule = Schedule(name="s", prompt="tick", every_seconds=100.0, trusted=True)
    store = _store(tmp_path)
    config = _config(tmp_path, schedules=(schedule,))

    first = tick_schedules(config, store, 250.0)
    assert len(first) == 1
    job = store.get_job(first[0])
    assert job.source_key == "schedule:s:2"
    assert job.status == "queued" and job.approved

    assert tick_schedules(config, store, 260.0) == []

    _complete(store, first[0])
    third = tick_schedules(config, store, 350.0)
    assert len(third) == 1 and store.get_job(third[0]).source_key == "schedule:s:3"

    _complete(store, third[0])
    jumped = tick_schedules(config, store, 10000.0)
    assert len(jumped) == 1 and store.get_job(jumped[0]).source_key == "schedule:s:100"
    assert tick_schedules(config, store, 10050.0) == []

    assert tick_schedules(config, store, 50.0) == []

    reopened = Store(tmp_path / "runtime.db")
    assert tick_schedules(config, reopened, 10050.0) == []
    assert len(reopened.list_jobs()) == 3


def test_untrusted_schedule_awaits_approval(tmp_path: Path) -> None:
    schedule = Schedule(name="s", prompt="tick", every_seconds=10.0)
    store = _store(tmp_path)
    created = tick_schedules(_config(tmp_path, schedules=(schedule,)), store, 10.0)
    assert len(created) == 1
    job = store.get_job(created[0])
    assert job.status == "awaiting_approval" and not job.approved


def test_schedule_overlap_skips_but_keeps_pending_work(tmp_path: Path) -> None:
    schedule = Schedule(name="s", prompt="tick", every_seconds=10.0)
    store = _store(tmp_path)
    config = _config(tmp_path, schedules=(schedule,))

    first = tick_schedules(config, store, 10.0)
    assert len(first) == 1

    assert tick_schedules(config, store, 20.0) == []
    assert store.get_checkpoint("schedule:s:slot") == "1"

    _complete(store, first[0])
    second = tick_schedules(config, store, 20.0)
    assert len(second) == 1
    assert store.get_job(second[0]).source_key == "schedule:s:2"


def test_daily_schedule_runs_once_per_local_date(tmp_path: Path) -> None:
    schedule = Schedule(
        name="d", prompt="daily", at="09:00", timezone="UTC", trusted=True
    )
    store = _store(tmp_path)
    config = _config(tmp_path, schedules=(schedule,))

    def at(year: int, month: int, day: int, hour: int, minute: int) -> float:
        return datetime(year, month, day, hour, minute, tzinfo=UTC).timestamp()

    first = tick_schedules(config, store, at(2026, 10, 7, 10, 0))
    assert len(first) == 1
    assert store.get_job(first[0]).source_key == "schedule:d:2026-10-07"
    assert tick_schedules(config, store, at(2026, 10, 7, 11, 0)) == []

    _complete(store, first[0])
    assert tick_schedules(config, store, at(2026, 10, 8, 8, 0)) == []

    second = tick_schedules(config, store, at(2026, 10, 8, 9, 30))
    assert len(second) == 1
    assert store.get_job(second[0]).source_key == "schedule:d:2026-10-08"

    _complete(store, second[0])
    assert tick_schedules(config, store, at(2026, 10, 6, 12, 0)) == []


def test_file_monitor_baseline_change_and_missing(tmp_path: Path) -> None:
    watched = tmp_path / "watched.txt"
    watched.write_text("one\n", encoding="utf-8")
    monitor = Monitor(name="m", path=watched)
    store = _store(tmp_path)
    config = _config(tmp_path, monitors=(monitor,))

    assert tick_monitors(config, store, 0.0) == []
    assert tick_monitors(config, store, 1.0) == []

    watched.write_text("two\n", encoding="utf-8")
    changed = tick_monitors(config, store, 2.0)
    assert len(changed) == 1
    assert store.get_job(changed[0]).status == "awaiting_approval"
    assert tick_monitors(config, store, 3.0) == []

    watched.unlink()
    missing = tick_monitors(config, store, 4.0)
    assert len(missing) == 1
    assert tick_monitors(config, store, 5.0) == []

    reopened = Store(tmp_path / "runtime.db")
    assert tick_monitors(config, reopened, 6.0) == []
    assert len(reopened.list_jobs()) == 2


def test_file_monitor_emit_initial(tmp_path: Path) -> None:
    watched = tmp_path / "watched.txt"
    watched.write_text("initial\n", encoding="utf-8")
    monitor = Monitor(name="m", path=watched, emit_initial=True)
    store = _store(tmp_path)
    created = tick_monitors(_config(tmp_path, monitors=(monitor,)), store, 0.0)
    assert len(created) == 1


def test_command_monitor_strips_credentials_and_detects_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ULTRA_AGENT_TEST_TOKEN", "topsecret")
    messenger = MessengerConfig(
        platform="discord", token_env="ULTRA_AGENT_TEST_TOKEN", channels=("1",), users=("2",)
    )
    monitor = Monitor(
        name="m",
        command=(
            sys.executable,
            "-c",
            "import os; print(os.environ.get('ULTRA_AGENT_TEST_TOKEN', 'ABSENT'))",
        ),
        timeout_seconds=30.0,
        emit_initial=True,
    )
    store = _store(tmp_path)
    config = _config(tmp_path, monitors=(monitor,), messengers=(messenger,))

    created = tick_monitors(config, store, 0.0)
    assert len(created) == 1
    prompt = store.get_job(created[0]).prompt
    assert "ABSENT" in prompt
    assert "topsecret" not in prompt
    assert tick_monitors(config, store, 1.0) == []


def test_command_monitor_change_on_exit_status(tmp_path: Path) -> None:
    data = tmp_path / "data.txt"
    data.write_text("ok\n", encoding="utf-8")
    monitor = Monitor(
        name="m",
        command=(
            sys.executable,
            "-c",
            "import sys; t = open(sys.argv[1]).read(); print(t); sys.exit(1 if 'fail' in t else 0)",
            str(data),
        ),
        timeout_seconds=30.0,
    )
    store = _store(tmp_path)
    config = _config(tmp_path, monitors=(monitor,))

    assert tick_monitors(config, store, 0.0) == []

    data.write_text("fail\n", encoding="utf-8")
    failed = tick_monitors(config, store, 1.0)
    assert len(failed) == 1
    assert tick_monitors(config, store, 2.0) == []

    data.write_text("ok\n", encoding="utf-8")
    recovered = tick_monitors(config, store, 3.0)
    assert len(recovered) == 1
    assert recovered[0] != failed[0]


_PROBE_SOURCE = """\
import socket
import subprocess
import sys
import time

host, port = sys.argv[1], int(sys.argv[2])
if len(sys.argv) < 4:
    subprocess.Popen([sys.executable, __file__, host, str(port), "child"])
connection = socket.create_connection((host, port))
time.sleep(300)
"""


def test_probe_timeout_kills_whole_process_group(tmp_path: Path) -> None:
    script = tmp_path / "probe.py"
    script.write_text(_PROBE_SOURCE, encoding="utf-8")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(4)
    server.settimeout(20.0)
    port = server.getsockname()[1]

    results: list[ProbeResult] = []

    def run() -> None:
        results.append(
            run_probe([sys.executable, str(script), "127.0.0.1", str(port)], 2.0, {})
        )

    worker = threading.Thread(target=run)
    worker.start()
    try:
        connections = [server.accept()[0] for _ in range(2)]
        worker.join(timeout=30.0)
        assert not worker.is_alive()
        result = results[0]
        assert result.timed_out
        assert result.exit_code is None
        for connection in connections:
            connection.settimeout(5.0)
            try:
                assert connection.recv(1) == b""
            finally:
                connection.close()
    finally:
        server.close()


def test_relay_complete_lines_partial_and_duplicate_offsets(tmp_path: Path) -> None:
    path = tmp_path / "status.log"
    path.write_text("header\n", encoding="utf-8")
    relay = Relay(name="r", path=path, platform="discord", channel="chan")
    store = _store(tmp_path)
    config = _config(tmp_path, relays=(relay,))

    assert tick_relays(config, store, 0.0) == []

    with path.open("a", encoding="utf-8") as handle:
        handle.write("PROGRESS alpha\n")
    first = tick_relays(config, store, 1.0)
    assert [d.content for d in store.deliveries()] == ["PROGRESS alpha"]
    assert len(first) == 1

    with path.open("a", encoding="utf-8") as handle:
        handle.write("PROGRESS alpha\n")
    assert len(tick_relays(config, store, 2.0)) == 1
    assert [d.content for d in store.deliveries()] == ["PROGRESS alpha", "PROGRESS alpha"]

    with path.open("a", encoding="utf-8") as handle:
        handle.write("PROGRESS partial")
    assert tick_relays(config, store, 3.0) == []
    assert len(store.deliveries()) == 2

    with path.open("a", encoding="utf-8") as handle:
        handle.write("\nQUESTION beta\nNOTE ignored\n")
    tail = tick_relays(config, store, 4.0)
    assert len(tail) == 2
    assert [d.content for d in store.deliveries()] == [
        "PROGRESS alpha",
        "PROGRESS alpha",
        "PROGRESS partial",
        "QUESTION beta",
    ]
    assert all(d.platform == "discord" and d.channel_id == "chan" for d in store.deliveries())

    reopened = Store(tmp_path / "runtime.db")
    assert tick_relays(config, reopened, 5.0) == []
    assert len(reopened.deliveries()) == 4


def test_relay_rotation_and_truncate_reset(tmp_path: Path) -> None:
    path = tmp_path / "status.log"
    path.write_text("", encoding="utf-8")
    relay = Relay(name="r", path=path, platform="discord", channel="chan")
    store = _store(tmp_path)
    config = _config(tmp_path, relays=(relay,))

    assert tick_relays(config, store, 0.0) == []

    with path.open("a", encoding="utf-8") as handle:
        handle.write("PROGRESS one\n")
    assert len(tick_relays(config, store, 1.0)) == 1

    replacement = tmp_path / "replacement.log"
    replacement.write_text("PROGRESS one\n", encoding="utf-8")
    os.replace(replacement, path)
    assert len(tick_relays(config, store, 2.0)) == 1
    assert [d.content for d in store.deliveries()] == ["PROGRESS one", "PROGRESS one"]

    path.write_text("PROGRESS b\n", encoding="utf-8")
    assert len(tick_relays(config, store, 3.0)) == 1
    assert [d.content for d in store.deliveries()] == [
        "PROGRESS one",
        "PROGRESS one",
        "PROGRESS b",
    ]


def test_relay_write_rolls_back_atomically_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "status.log"
    path.write_text("seed\n", encoding="utf-8")
    relay = Relay(name="r", path=path, platform="discord", channel="chan")
    store = _store(tmp_path)
    config = _config(tmp_path, relays=(relay,))

    assert tick_relays(config, store, 0.0) == []
    baseline = store.get_checkpoint("relay:r")
    assert baseline is not None

    with path.open("a", encoding="utf-8") as handle:
        handle.write("PROGRESS a\nQUESTION b\n")

    original: Callable[..., str] = store.add_delivery
    calls = {"count": 0}

    def flaky(*args: object, **kwargs: object) -> str:
        calls["count"] += 1
        if calls["count"] == 2:
            raise RuntimeError("probe failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(store, "add_delivery", flaky)
    with pytest.raises(RuntimeError):
        tick_relays(config, store, 1.0)

    assert store.deliveries() == []
    assert store.get_checkpoint("relay:r") == baseline

    monkeypatch.setattr(store, "add_delivery", original)
    assert len(tick_relays(config, store, 2.0)) == 2
    assert [d.content for d in store.deliveries()] == ["PROGRESS a", "QUESTION b"]
    assert store.get_checkpoint("relay:r") != baseline


def test_relative_paths_resolve_against_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    watched = tmp_path / "watched.txt"
    watched.write_text("one\n", encoding="utf-8")
    status = tmp_path / "status.log"
    status.write_text("PROGRESS seed\n", encoding="utf-8")
    (elsewhere / "watched.txt").write_text("decoy\n", encoding="utf-8")
    (elsewhere / "status.log").write_text("PROGRESS decoy\n", encoding="utf-8")

    monitor = Monitor(name="m", path=Path("watched.txt"))
    relay = Relay(name="r", path=Path("status.log"), platform="discord", channel="chan")
    store = _store(tmp_path)
    config = _config(tmp_path, monitors=(monitor,), relays=(relay,))

    monkeypatch.chdir(elsewhere)
    assert tick_monitors(config, store, 0.0) == []
    watched.write_text("two\n", encoding="utf-8")
    assert len(tick_monitors(config, store, 1.0)) == 1

    assert tick_relays(config, store, 2.0) == []
    with status.open("a", encoding="utf-8") as handle:
        handle.write("PROGRESS go\n")
    assert len(tick_relays(config, store, 3.0)) == 1
    assert [d.content for d in store.deliveries()] == ["PROGRESS go"]


def test_tick_automation_dispatches_every_lane(tmp_path: Path) -> None:
    schedule = Schedule(name="s", prompt="tick", every_seconds=100.0, trusted=True)
    watched = tmp_path / "watched.txt"
    watched.write_text("initial\n", encoding="utf-8")
    monitor = Monitor(name="m", path=watched, emit_initial=True)
    status = tmp_path / "status.log"
    status.write_text("PROGRESS seed\n", encoding="utf-8")
    relay = Relay(name="r", path=status, platform="discord", channel="chan")
    store = _store(tmp_path)
    config = _config(
        tmp_path, schedules=(schedule,), monitors=(monitor,), relays=(relay,)
    )

    created = tick_automation(config, store, 250.0)
    assert len(created) == 2
    assert len(store.list_jobs()) == 2
    assert store.deliveries() == []
