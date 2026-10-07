"""Real subprocess, SQLite and Mnemosyne integration, without remote credentials."""

import json
import sys
from dataclasses import replace
from pathlib import Path
from threading import Event

import pytest

from ultra_agent.config import init_workspace
from ultra_agent.host import HeavySlot
from ultra_agent.models import HostConfig, Schedule, Submission
from ultra_agent.runner_lock import RunnerBusy, RunnerLock
from ultra_agent.runtime import Runtime

FIXTURE = Path(__file__).parent / "fixtures" / "omo.py"


def _runtime(tmp_path: Path, mode: str = "runtime") -> Runtime:
    config = replace(
        init_workspace(tmp_path),
        command=(sys.executable, str(FIXTURE.resolve()), mode),
        host=HostConfig(0, 0, 0, tmp_path / "slots"),
    )
    return Runtime(config)


def test_approval_then_real_turn_and_reopened_session(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    runtime.memory.write("aurora", "aurora-marker-8214", "fixture")
    identifier = runtime.store.enqueue(Submission("one", "aurora"))
    assert runtime.once().job is None
    runtime.store.approve(identifier)
    first = runtime.once().job
    assert first is not None and first.status == "completed"
    assert first.verified is False
    payload = json.loads(first.result)
    assert "aurora-marker-8214" in payload["prompt"]
    reopened = Runtime(runtime.config)
    reopened.store.enqueue(Submission("two", "second turn", approved=True))
    second = reopened.once().job
    assert second is not None and second.status == "completed"
    assert first.session_id == second.session_id
    assert reopened.registry.get("default").session_file.is_file()
    assert reopened.once().job is None
    assert len(reopened.store.list_jobs()) == 2


def test_status_construction_does_not_recover_live_job(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    identifier = runtime.store.enqueue(Submission("one", "work", approved=True))
    runtime.store.claim()
    with runtime.lock:
        inspector = Runtime(runtime.config)
        assert inspector.store.get_job(identifier).status == "running"
        with pytest.raises(RunnerBusy):
            inspector.once()
    result = runtime.once()
    assert result.hold
    assert runtime.store.get_job(identifier).status == "uncertain"


def test_busy_host_keeps_approved_work_queued(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    identifier = runtime.store.enqueue(Submission("one", "work", approved=True))
    with HeavySlot(runtime.config.host):
        assert runtime.once().hold
        assert runtime.store.get_job(identifier).status == "queued"
    assert runtime.once().job.status == "completed"


def test_compaction_failure_persists_handoff_before_fresh_session(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path, "compaction_failed")
    old = runtime.registry.get("default").session_id
    identifier = runtime.store.enqueue(Submission("one", "work", approved=True))
    result = runtime.once()
    assert result.job.status == "uncertain"
    assert result.handoff.is_file()
    assert runtime.registry.get("default").session_id != old
    assert runtime.store.get_job(identifier).session_id == old
    assert runtime.once().job is None
    assert len(runtime.store.list_jobs()) == 1


def test_normal_compaction_does_not_rotate_session(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path, "normal")
    old = runtime.registry.get("default").session_id
    runtime.store.enqueue(Submission("one", "work", approved=True))
    result = runtime.once()
    assert result.job.status == "completed"
    assert result.handoff is None
    assert runtime.registry.get("default").session_id == old


def test_failed_handoff_never_rotates_session(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    old = runtime.registry.get("default").session_id
    runtime.config.handoffs_dir.rmdir()
    runtime.config.handoffs_dir.write_text("not a directory")
    with pytest.raises(OSError):
        runtime.rotate("default", "operator rotation")
    assert runtime.registry.get("default").session_id == old
    assert runtime.store.get_checkpoint("handoff:default") is None


@pytest.mark.parametrize("code,expected", [(0, "completed"), (3, "failed")])
def test_verification_gates_success(tmp_path: Path, code: int, expected: str) -> None:
    original = _runtime(tmp_path)
    runtime = Runtime(replace(
        original.config, verification_command=(sys.executable, "-c", f"raise SystemExit({code})"),
    ))
    runtime.store.enqueue(Submission("one", "work", approved=True))
    result = runtime.once().job
    assert result.status == expected
    assert result.verified is (code == 0)


def test_custom_lane_uses_its_cwd_and_configured_style(tmp_path: Path) -> None:
    original = _runtime(tmp_path)
    style = "operator-supplied-style-marker"
    runtime = Runtime(replace(original.config, writing_style=style))
    lane = runtime.registry.launch("worker", "worker", "manual", cwd=tmp_path / "project")
    runtime.store.enqueue(Submission("one", "work", approved=True, lane="worker"))
    result = runtime.once().job
    payload = json.loads(result.result)
    assert payload["cwd"] == str(lane.cwd)
    assert payload["session_id"] == lane.session_id
    assert payload["prompt"].startswith(style)


def test_restart_does_not_repeat_schedule_occurrence(tmp_path: Path) -> None:
    original = _runtime(tmp_path)
    config = replace(original.config, schedules=(Schedule("daily", "work", every_seconds=60),))
    first = Runtime(config, clock=lambda: 120.0)
    assert len(first.once().enqueued) == 1
    second = Runtime(config, clock=lambda: 120.0)
    assert second.once().enqueued == ()
    assert len(second.store.list_jobs()) == 1


def test_daemon_stop_event_releases_runner_lock(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    stop = Event()
    runtime.run(stop, lambda result: stop.set())
    with RunnerLock(runtime.config.state_dir / "runner.lock"):
        pass


def test_lane_cap_does_not_prevent_existing_lane_from_finishing(tmp_path: Path) -> None:
    original = _runtime(tmp_path)
    runtime = Runtime(replace(
        original.config, host=replace(original.config.host, max_lanes=1),
    ))
    runtime.store.enqueue(Submission("one", "finish", approved=True))
    assert runtime.once().job.status == "completed"
