"""Fail-closed host guard and the host-wide heavy launch slot.

This module owns the boundary between one configured workspace and the shared
machine it runs on. It answers one question -- may another launch start right
now? -- and it answers with HOLD whenever it cannot measure the host, never with
a fabricated healthy value. A separate, host-wide advisory lock lets different
workspaces coexist without overloading one machine: every heavy process takes
one slot file under an operator-local directory, and a slot file that stays
locked means the work waits instead of overcommitting the host.

The lock is an ``fcntl`` advisory lock on a per-slot file, so the held file
descriptor *is* the lock. A crashed holder frees its slot in the kernel, and no
process id is ever read, trusted or killed.
"""

from __future__ import annotations

import fcntl
import math
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from ultra_agent.models import Config, HostConfig

GIB = 1024**3
"""Bytes in one gibibyte, the unit disk headroom is reported in."""

_MEMINFO_PATH = Path("/proc/meminfo")
"""Linux memory source; an absent or unreadable file means an unknown sample."""

_SLOT_PREFIX = "heavy"
"""Stem of the per-slot lock files inside the shared slot directory."""


@dataclass(frozen=True, slots=True)
class HostSample:
    """One point-in-time view of the host; ``None`` means "could not measure".

    ``load`` is reported for the operator and never decides a launch. Callers
    pass the lane count they already know from durable state.
    """

    memory_percent: float | None
    disk_gib: float | None
    load: float | None
    active_lanes: int


@dataclass(frozen=True, slots=True)
class LaunchDecision:
    """Whether a launch may proceed, plus the reasons that forced HOLD."""

    ok: bool
    reasons: tuple[str, ...] = ()


def sample_host(config: Config, active_lanes: int) -> HostSample:
    """Measure the host for one workspace, inventing no missing value."""

    return HostSample(
        memory_percent=_memory_percent(),
        disk_gib=_disk_gib(config.workspace),
        load=_load(),
        active_lanes=active_lanes,
    )


def can_launch(sample: HostSample, limits: HostConfig) -> LaunchDecision:
    """Decide whether to launch, failing closed on any unmeasured resource.

    A resource below its configured minimum holds the launch; so does a missing
    measurement. CPU load is never a veto, and a zero lane cap disables only the
    lane check.
    """

    reasons: list[str] = []

    memory = sample.memory_percent
    if memory is None or not math.isfinite(memory) or not 0 <= memory <= 100:
        reasons.append("available memory could not be measured")
    elif memory < limits.min_memory_percent:
        reasons.append(
            f"available memory {memory:.1f} percent is below the "
            f"{limits.min_memory_percent:.1f} percent minimum"
        )

    disk = sample.disk_gib
    if disk is None or not math.isfinite(disk) or disk < 0:
        reasons.append("available disk could not be measured")
    elif disk < limits.min_disk_gib:
        reasons.append(
            f"available disk {disk:.2f} GiB is below the "
            f"{limits.min_disk_gib:.2f} GiB minimum"
        )

    if limits.max_lanes > 0 and sample.active_lanes >= limits.max_lanes:
        reasons.append(
            f"{sample.active_lanes} active lanes have reached the "
            f"{limits.max_lanes} lane cap"
        )

    return LaunchDecision(ok=not reasons, reasons=tuple(reasons))


def _memory_percent() -> float | None:
    """Linux MemAvailable as a percentage of MemTotal, or ``None``."""

    try:
        text = _MEMINFO_PATH.read_text(encoding="utf-8")
    except OSError:
        return None
    available = _meminfo_kib(text, "MemAvailable")
    total = _meminfo_kib(text, "MemTotal")
    if available is None or total is None or total <= 0:
        return None
    return 100.0 * available / total


def _meminfo_kib(text: str, key: str) -> float | None:
    """First numeric field of one ``/proc/meminfo`` line, or ``None``."""

    prefix = f"{key}:"
    for line in text.splitlines():
        if line.startswith(prefix):
            fields = line[len(prefix) :].split()
            if not fields:
                return None
            try:
                return float(fields[0])
            except ValueError:
                return None
    return None


def _disk_gib(workspace: Path) -> float | None:
    """Free space on the filesystem holding ``workspace``, in GiB, or ``None``."""

    try:
        free = shutil.disk_usage(workspace).free
    except OSError:
        return None
    return free / GIB


def _load() -> float | None:
    """One-minute load average, or ``None`` where the platform has none."""

    try:
        return os.getloadavg()[0]
    except OSError:
        return None


class SlotUnavailable(RuntimeError):
    """Raised by the context-manager form when every heavy slot is busy."""


class HeavySlot:
    """One host-wide heavy-launch slot, shared across separate workspaces.

    Construction is inert: it only resolves the operator-local slot directory.
    ``acquire`` takes the first free slot file without blocking, keeps that file
    descriptor open for the whole process lifetime, and returns ``True``; when
    every slot is held it returns ``False`` so the caller can leave the work
    queued. The descriptor is the lock holder, so nothing reads, trusts or kills
    a process id and a crashed holder frees its slot in the kernel.
    """

    def __init__(self, limits: HostConfig) -> None:
        self._slot_dir = Path(limits.slot_dir).expanduser().resolve()
        self._capacity = limits.heavy_slots
        self._fd: int | None = None

    @property
    def acquired(self) -> bool:
        """Whether this instance currently holds a slot."""

        return self._fd is not None

    def fileno(self) -> int:
        """Descriptor inherited by the child so a parent crash cannot free its slot."""
        if self._fd is None:
            raise SlotUnavailable("no heavy slot is held")
        return self._fd

    def acquire(self) -> bool:
        """Take one slot without blocking; ``False`` means all of them are busy."""

        if self._fd is not None:
            return True
        if self._capacity < 1:
            return False
        try:
            self._slot_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            return False
        for index in range(self._capacity):
            path = self._slot_dir / f"{_SLOT_PREFIX}-{index}.lock"
            try:
                fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
            except OSError:
                return False
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                os.close(fd)
                continue
            self._fd = fd
            return True
        return False

    def release(self) -> None:
        """Drop the held slot; safe to call twice or when nothing was taken."""

        fd = self._fd
        if fd is None:
            return
        self._fd = None
        # A surviving child inherits this open-file description. Closing only
        # our reference preserves its slot until the child really exits.
        os.close(fd)

    def __enter__(self) -> HeavySlot:
        if not self.acquire():
            raise SlotUnavailable("no heavy launch slot is free")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
