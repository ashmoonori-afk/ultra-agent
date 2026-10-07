"""Behavior tests for the fail-closed host guard and the shared heavy slot.

The guard tests inject samples directly to pin the ready/HOLD boundaries,
including the unknown-measurement case that must never fall back to healthy.
The slot tests take real advisory locks: contention inside one process, across
two workspace configurations that share one host slot directory, and against a
real child process that holds the lock until a pipe line releases it. No test
sleeps; every wait is a blocking pipe read or a bounded process wait.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

import ultra_agent.host as host
from ultra_agent.host import (
    HeavySlot,
    HostSample,
    LaunchDecision,
    SlotUnavailable,
    can_launch,
    sample_host,
)
from ultra_agent.models import Config, HostConfig

_CHILD_HOLDS = """\
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)
fcntl.flock(fd, fcntl.LOCK_EX)
sys.stdout.write("locked\\n")
sys.stdout.flush()
sys.stdin.readline()
"""

_CHILD_PROBES = """\
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)
try:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except OSError:
    sys.stdout.write("busy\\n")
else:
    sys.stdout.write("free\\n")
"""


def _limits(tmp_path: Path, *, heavy_slots: int = 1) -> HostConfig:
    return HostConfig(slot_dir=tmp_path / "slots", heavy_slots=heavy_slots)


def _sample(
    *, memory_percent: float | None = 50, disk_gib: float | None = 10,
    load: float | None = 12, active_lanes: int = 0,
) -> HostSample:
    return HostSample(memory_percent, disk_gib, load, active_lanes)


def test_ready_when_every_resource_is_healthy() -> None:
    assert can_launch(_sample(), HostConfig()) == LaunchDecision(ok=True, reasons=())


def test_unknown_measurement_holds() -> None:
    one = can_launch(_sample(memory_percent=None), HostConfig())
    assert one.ok is False
    assert len(one.reasons) == 1

    other = can_launch(_sample(disk_gib=None), HostConfig())
    assert other.ok is False
    assert len(other.reasons) == 1

    both = can_launch(_sample(memory_percent=None, disk_gib=None), HostConfig())
    assert both.ok is False
    assert len(both.reasons) == 2


@pytest.mark.parametrize(("memory", "expected"), [(20.0, True), (19.999, False)])
def test_memory_threshold_boundary(memory: float, expected: bool) -> None:
    assert can_launch(_sample(memory_percent=memory), HostConfig()).ok is expected


@pytest.mark.parametrize(("disk", "expected"), [(1.0, True), (0.999, False)])
def test_disk_threshold_boundary(disk: float, expected: bool) -> None:
    assert can_launch(_sample(disk_gib=disk), HostConfig()).ok is expected


def test_lane_cap_boundary() -> None:
    assert can_launch(_sample(active_lanes=19), HostConfig(max_lanes=20)).ok is True
    assert can_launch(_sample(active_lanes=20), HostConfig(max_lanes=20)).ok is False


def test_zero_lane_cap_disables_the_cap() -> None:
    assert can_launch(_sample(active_lanes=10**6), HostConfig(max_lanes=0)).ok is True


def test_cpu_load_never_vetoes() -> None:
    assert can_launch(_sample(load=999.0), HostConfig()).ok is True
    assert can_launch(_sample(load=None), HostConfig()).ok is True


def test_sample_host_reads_the_real_host(tmp_path: Path) -> None:
    sample = sample_host(Config(workspace=tmp_path), active_lanes=2)
    assert sample.active_lanes == 2
    assert sample.memory_percent is None or 0.0 <= sample.memory_percent <= 100.0
    assert sample.disk_gib is None or sample.disk_gib >= 0.0
    assert sample.load is None or sample.load >= 0.0


def test_missing_meminfo_is_unknown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(host, "_MEMINFO_PATH", tmp_path / "absent")
    assert sample_host(Config(workspace=tmp_path), 0).memory_percent is None


def test_meminfo_without_available_is_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "meminfo"
    source.write_text("MemTotal:       1000000 kB\n")
    monkeypatch.setattr(host, "_MEMINFO_PATH", source)
    assert sample_host(Config(workspace=tmp_path), 0).memory_percent is None


def test_meminfo_percent_is_computed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "meminfo"
    source.write_text("MemTotal:       1000 kB\nMemAvailable:    250 kB\n")
    monkeypatch.setattr(host, "_MEMINFO_PATH", source)
    sample = sample_host(Config(workspace=tmp_path), 0)
    assert sample.memory_percent == pytest.approx(25.0)


def test_missing_workspace_disk_is_unknown(tmp_path: Path) -> None:
    sample = sample_host(Config(workspace=tmp_path / "absent"), 0)
    assert sample.disk_gib is None
    assert can_launch(sample, HostConfig()).ok is False


def test_constructor_takes_no_slot(tmp_path: Path) -> None:
    slot = HeavySlot(_limits(tmp_path))
    assert slot.acquired is False
    assert not (tmp_path / "slots").exists()


def test_single_slot_contention_in_one_process(tmp_path: Path) -> None:
    limits = _limits(tmp_path)
    first = HeavySlot(limits)
    second = HeavySlot(limits)
    assert first.acquire() is True
    assert second.acquire() is False
    first.release()
    assert second.acquire() is True
    second.release()


def test_two_workspaces_share_one_host_slot(tmp_path: Path) -> None:
    slot_dir = tmp_path / "slots"
    workspace_a = Config(workspace=tmp_path / "a", host=HostConfig(slot_dir=slot_dir))
    workspace_b = Config(workspace=tmp_path / "b", host=HostConfig(slot_dir=slot_dir))
    slot_a = HeavySlot(workspace_a.host)
    slot_b = HeavySlot(workspace_b.host)
    assert slot_a.acquire() is True
    assert slot_b.acquire() is False
    slot_a.release()
    assert slot_b.acquire() is True
    slot_b.release()


def test_capacity_grants_every_configured_slot(tmp_path: Path) -> None:
    limits = _limits(tmp_path, heavy_slots=2)
    slots = [HeavySlot(limits) for _ in range(3)]
    assert slots[0].acquire() is True
    assert slots[1].acquire() is True
    assert slots[2].acquire() is False
    slots[0].release()
    assert slots[2].acquire() is True
    for slot in slots:
        slot.release()


def test_zero_capacity_never_acquires(tmp_path: Path) -> None:
    assert HeavySlot(_limits(tmp_path, heavy_slots=0)).acquire() is False


def test_release_is_idempotent_and_safe_when_unheld(tmp_path: Path) -> None:
    limits = _limits(tmp_path)
    slot = HeavySlot(limits)
    slot.release()
    assert slot.acquire() is True
    slot.release()
    slot.release()
    assert slot.acquired is False
    again = HeavySlot(limits)
    assert again.acquire() is True
    again.release()


def test_context_manager_releases_on_exit(tmp_path: Path) -> None:
    limits = _limits(tmp_path)
    with HeavySlot(limits) as held:
        assert held.acquired is True
    after = HeavySlot(limits)
    assert after.acquire() is True
    after.release()


def test_context_manager_raises_when_busy(tmp_path: Path) -> None:
    limits = _limits(tmp_path)
    holder = HeavySlot(limits)
    assert holder.acquire() is True
    try:
        with pytest.raises(SlotUnavailable), HeavySlot(limits):
            pass
    finally:
        holder.release()


def test_external_process_holds_the_slot_until_released(tmp_path: Path) -> None:
    slot_dir = tmp_path / "slots"
    slot_dir.mkdir()
    limits = HostConfig(slot_dir=slot_dir)
    lock_path = slot_dir / "heavy-0.lock"

    child = subprocess.Popen(
        [sys.executable, "-c", _CHILD_HOLDS, str(lock_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline() == "locked\n"
        assert HeavySlot(limits).acquire() is False
    finally:
        if child.poll() is None and child.stdin is not None:
            try:
                child.stdin.write("release\n")
                child.stdin.flush()
            except BrokenPipeError:
                pass
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=10)

    assert child.returncode == 0
    freed = HeavySlot(limits)
    assert freed.acquire() is True
    freed.release()


def test_parent_slot_is_visible_to_another_process(tmp_path: Path) -> None:
    slot_dir = tmp_path / "slots"
    limits = HostConfig(slot_dir=slot_dir)
    held = HeavySlot(limits)
    assert held.acquire() is True
    try:
        probe = subprocess.run(
            [sys.executable, "-c", _CHILD_PROBES, str(slot_dir / "heavy-0.lock")],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        assert probe.stdout == "busy\n"
    finally:
        held.release()
