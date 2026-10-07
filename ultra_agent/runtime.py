"""One locked controller connects durable input, agent turns and delivery."""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from threading import Event

from ultra_agent.automation import tick_monitors, tick_relays, tick_schedules
from ultra_agent.completion import Completion
from ultra_agent.driver import run_turn, run_verification
from ultra_agent.handoff import write_handoff
from ultra_agent.host import HeavySlot, SlotUnavailable, can_launch, sample_host
from ultra_agent.memory import Memory
from ultra_agent.messenger import Messenger
from ultra_agent.models import DEFAULT_LANE, Config, Job
from ultra_agent.registry import Lane, Registry
from ultra_agent.runner_lock import RunnerLock
from ultra_agent.store import Store


@dataclass(frozen=True, slots=True)
class TickResult:
    job: Job | None = None
    enqueued: tuple[str, ...] = ()
    delivered: tuple[str, ...] = ()
    lanes: tuple[Lane, ...] = ()
    hold: tuple[str, ...] = ()
    handoff: Path | None = None


class Runtime:
    """Recovery runs only while owning the workspace, never on a status read."""

    def __init__(
        self, config: Config, *, messenger: Messenger | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.config = config
        self.store = Store(config.db_path)
        self.registry = Registry(self.store, config)
        self.memory = Memory(config.vault_dir)
        self.messenger = messenger if messenger is not None else Messenger(config, self.store)
        self.completion = Completion(config, self.store, self.registry)
        self.clock = clock
        self.lock = RunnerLock(config.state_dir / "runner.lock")
        if not any(lane.key == DEFAULT_LANE for lane in self.registry.list()):
            self.registry.launch(
                DEFAULT_LANE, "agent",
                "command" if config.verification_command else "manual",
                cwd=config.work_dir,
            )

    def once(self) -> TickResult:
        with self.lock:
            self.store.recover_interrupted()
            return self._tick()

    def run(self, stop: Event, emit: Callable[[TickResult], None]) -> None:
        """Run until the caller's stop event; signals are owned by the CLI."""
        with self.lock:
            self.store.recover_interrupted()
            while not stop.is_set():
                emit(self._tick())
                stop.wait(self.config.poll_seconds)

    def rotate(self, key: str, reason: str) -> Path:
        """An explicit restart still requires a durable handoff and no live child."""
        with self.lock:
            self.store.recover_interrupted()
            return self._rotate(self.registry.get(key), reason)

    def close_lane(self, key: str) -> Lane:
        """Completion commands use the same ownership policy as agent turns."""
        with self.lock:
            decision = can_launch(
                sample_host(self.config, len(self.registry.list(open_only=True))),
                replace(self.config.host, max_lanes=0),
            )
            if not decision.ok:
                raise SlotUnavailable("host HOLD: " + "; ".join(decision.reasons))
            with HeavySlot(self.config.host) as slot:
                completion = Completion(
                    self.config, self.store, self.registry,
                    pass_fds=(self.lock.fileno(), slot.fileno()),
                )
                return completion.close(key, self.clock())

    def _rotate(self, lane: Lane, reason: str) -> Path:
        path = write_handoff(self.config, self.store, lane, reason)
        self.registry.rotate(lane.key, str(uuid.uuid4()), self.clock(), f"handoff: {path}")
        return path

    def _tick(self) -> TickResult:
        now = self.clock()
        enqueued = self.messenger.poll()
        enqueued.extend(tick_schedules(self.config, self.store, now))
        enqueued.extend(tick_relays(self.config, self.store, now))
        delivered = self.messenger.drain()
        decision = can_launch(
            sample_host(self.config, len(self.registry.list(open_only=True))),
            # The registry already admitted these lanes. The cap applies to
            # adding a lane, not to finishing work in an existing one.
            replace(self.config.host, max_lanes=0),
        )
        if not decision.ok:
            return TickResult(
                enqueued=tuple(enqueued), delivered=tuple(delivered), hold=decision.reasons,
            )
        slot = HeavySlot(self.config.host)
        if not slot.acquire():
            return TickResult(
                enqueued=tuple(enqueued), delivered=tuple(delivered),
                hold=("host heavy slot is busy or unavailable",),
            )
        try:
            descriptors = (self.lock.fileno(), slot.fileno())
            enqueued.extend(tick_monitors(
                self.config, self.store, now, pass_fds=descriptors,
            ))
            completion = Completion(
                self.config, self.store, self.registry, pass_fds=descriptors,
            )
            lanes = tuple(completion.tick(now))
            job = self.store.claim()
            handoff: Path | None = None
            if job is not None:
                handoff = self._execute(job, slot)
                job = self.store.get_job(job.id)
            delivered.extend(self.messenger.drain())
            blocked = (
                ("unresolved execution requires explicit reconciliation",)
                if job is None and any(
                    entry.status == "uncertain" for entry in self.store.list_jobs()
                ) else ()
            )
            return TickResult(
                job, tuple(enqueued), tuple(delivered), lanes, blocked, handoff,
            )
        finally:
            slot.release()

    def _execute(self, job: Job, slot: HeavySlot) -> Path | None:
        finished = False
        try:
            lane = next(
                (entry for entry in self.registry.list() if entry.key == job.lane), None,
            )
            if lane is None or lane.status == "closed":
                self.store.finish(job.id, "failed", error="lane is missing or closed")
                finished = True
                return None
            if lane.host != "local":
                self.store.finish(job.id, "failed", error="execution requires a local lane")
                finished = True
                return None
            lane.cwd.mkdir(parents=True, exist_ok=True)
            prompt = self.memory.recall(job.prompt) + "\n\nRequest:\n" + job.prompt
            if self.config.writing_style:
                prompt = self.config.writing_style + "\n\n" + prompt
            prior = self.store.get_checkpoint(f"handoff:{lane.key}")
            if prior is not None:
                with Path(prior).open(encoding="utf-8") as handle:
                    context = handle.read(1024 * 1024 + 1)
                if len(context) > 1024 * 1024:
                    raise ValueError("handoff exceeds the agent input bound")
                prompt = context + "\n\n" + prompt
            descriptors = (self.lock.fileno(), slot.fileno())
            result = run_turn(
                self.config, lane.session_id, prompt, cwd=lane.cwd, pass_fds=descriptors,
            )
            if not result.success:
                self.store.finish(job.id, "uncertain", result.text, result.error or "turn failed")
                finished = True
                if result.compaction_failed:
                    return self._rotate(lane, "automatic compaction failed")
                return None
            verified = False
            if self.config.verification_command:
                check = run_verification(self.config, cwd=lane.cwd, pass_fds=descriptors)
                if not check.success:
                    self.store.finish(
                        job.id, "failed", result.text, check.error or "verification failed",
                    )
                    finished = True
                    return None
                verified = True
            self.memory.write(f"job {job.id}", result.text, f"job:{job.id}")
            self.store.finish(job.id, "completed", result.text, verified=verified)
            finished = True
            self.registry.sync(write=True)
            return None
        finally:
            if not finished:
                self.store.finish(job.id, "uncertain", error="execution interrupted")
