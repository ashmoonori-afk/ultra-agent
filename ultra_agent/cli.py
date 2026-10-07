"""argparse command surface for ultra-agent.

Every data result is JSON on stdout and every failure exits non-zero; the one
documented exception is ``service render``, which prints the plain systemd unit
text (machine configuration, not data). ``--workspace PATH`` is a global option
and must come before the command.

:func:`main` accepts an injected ``runtime_factory`` so a caller can supply a
real :class:`~ultra_agent.runtime.Runtime` built with a loopback messenger at
the network boundary; the parser, handlers and results stay real. A status read
never runs recovery, a manual send drains only while holding the workspace
runner lock, the Done-grammar and host-hold checks run before a lane is added,
and lane closure always goes through
:meth:`~ultra_agent.completion.Completion.close`, which owns the objection
window. Tokens and cookies are referenced only by environment-variable name and
never appear in output or errors.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import signal
import threading
import uuid
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import FrameType
from typing import Final, TypeAlias, TypeGuard

from ultra_agent import service
from ultra_agent.completion import SignalError, parse_signals
from ultra_agent.config import ConfigError, init_workspace, load_config
from ultra_agent.host import HeavySlot, HostSample, SlotUnavailable, can_launch, sample_host
from ultra_agent.http import TransportError
from ultra_agent.messenger import CapabilityError, MessengerError
from ultra_agent.models import (
    DEFAULT_LANE,
    Config,
    Delivery,
    Job,
    JSONValue,
    Platform,
    RelayPeer,
    Submission,
)
from ultra_agent.presence import PresenceError
from ultra_agent.registry import Lane, LaneEvent, RegistryError, SyncFinding, SyncReport
from ultra_agent.relay import RelayError, create_relay_server, push_status_file
from ultra_agent.runner_lock import RunnerBusy
from ultra_agent.runtime import Runtime, TickResult
from ultra_agent.service import ServiceError
from ultra_agent.slack import SlackError, UnsupportedCapabilityError
from ultra_agent.store import StoreError
from ultra_agent.telegram import TelegramError

__all__ = ["CliError", "main", "build_parser"]

RuntimeFactory: TypeAlias = Callable[[Config], Runtime]
CommandHandler: TypeAlias = Callable[[argparse.Namespace, RuntimeFactory], int]
SignalHandlerType: TypeAlias = Callable[[int, FrameType | None], object] | int | None

_PLATFORMS: Final = ("discord", "telegram", "slack")


class CliError(RuntimeError):
    """A command could not proceed; the message is reported as JSON and exits 1."""


def _emit(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


def _emit_error(message: str) -> None:
    _emit({"ok": False, "error": message})


@contextmanager
def _signal_stop(stop: threading.Event) -> Generator[None]:
    """Set ``stop`` and raise ``KeyboardInterrupt`` on SIGINT/SIGTERM.

    A synchronous agent turn is therefore interrupted promptly: the driver reaps
    its process group and the runtime marks the job ``uncertain``. Previous
    handlers are restored in ``finally``, and handlers are installed only in the
    main thread because :func:`signal.signal` is only legal there.
    """
    previous: list[tuple[int, SignalHandlerType]] = []

    def _handle(_signum: int, _frame: FrameType | None) -> None:
        stop.set()
        raise KeyboardInterrupt

    if threading.current_thread() is threading.main_thread():
        for number in (signal.SIGINT, signal.SIGTERM):
            previous.append((number, signal.getsignal(number)))
            signal.signal(number, _handle)
    try:
        yield
    finally:
        for number, handler in previous:
            signal.signal(number, handler)


def _workspace(args: argparse.Namespace) -> Path:
    raw: object = args.workspace
    if not isinstance(raw, str) or not raw.strip():
        raise CliError("--workspace PATH is required (or set UA_WORKSPACE)")
    return Path(raw).expanduser()


def _load(args: argparse.Namespace) -> Config:
    return load_config(_workspace(args))


def _runtime(args: argparse.Namespace, factory: RuntimeFactory) -> Runtime:
    return factory(_load(args))


def _platform(value: object) -> Platform:
    match value:
        case "discord":
            return "discord"
        case "telegram":
            return "telegram"
        case "slack":
            return "slack"
        case _:
            raise CliError(f"platform must be one of {', '.join(_PLATFORMS)}")


def _is_object_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _str_list(value: object) -> list[str]:
    if not _is_object_list(value):
        return []
    return [item for item in value if isinstance(item, str)]


def _opt_str(args: argparse.Namespace, name: str) -> str | None:
    value: object = getattr(args, name)
    return value if isinstance(value, str) else None


def _job_json(job: Job) -> dict[str, JSONValue]:
    return {
        "id": job.id,
        "source_key": job.source_key,
        "prompt": job.prompt,
        "approved": job.approved,
        "channel_id": job.channel_id,
        "reply_to": job.reply_to,
        "thread_id": job.thread_id,
        "lane": job.lane,
        "platform": job.platform,
        "heavy": job.heavy,
        "status": job.status,
        "result": job.result,
        "error": job.error,
        "session_id": job.session_id,
        "retry_of": job.retry_of,
        "verified": job.verified,
    }


def _delivery_json(delivery: Delivery) -> dict[str, JSONValue]:
    return {
        "id": delivery.id,
        "platform": delivery.platform,
        "channel_id": delivery.channel_id,
        "content": delivery.content,
        "job_id": delivery.job_id,
        "reply_to": delivery.reply_to,
        "thread_id": delivery.thread_id,
        "status": delivery.status,
        "remote_id": delivery.remote_id,
        "ordinal": delivery.ordinal,
    }


def _lane_json(lane: Lane) -> dict[str, JSONValue]:
    return {
        "key": lane.key,
        "role": lane.role,
        "cwd": str(lane.cwd),
        "host": lane.host,
        "session_id": lane.session_id,
        "session_file": str(lane.session_file) if lane.session_file is not None else None,
        "done": lane.done,
        "worktrees": [str(item) for item in lane.worktrees],
        "status": lane.status,
        "revision": lane.revision,
        "claim_at": lane.claim_at,
        "deadline": lane.deadline,
        "evidence": lane.evidence,
        "claim_emitted": lane.claim_emitted,
    }


def _event_json(event: LaneEvent) -> dict[str, JSONValue]:
    return {
        "key": event.key,
        "revision": event.revision,
        "at": event.at,
        "status": event.status,
        "note": event.note,
        "source": event.source,
    }


def _finding_json(finding: SyncFinding) -> dict[str, JSONValue]:
    return {
        "kind": finding.kind,
        "key": finding.key,
        "path": str(finding.path) if finding.path is not None else None,
        "detail": finding.detail,
    }


def _sync_json(report: SyncReport) -> dict[str, JSONValue]:
    return {
        "scanned": [str(item) for item in report.scanned],
        "findings": [_finding_json(item) for item in report.findings],
        "repairs": [
            {
                "key": repair.key,
                "revision": repair.revision,
                "source": repair.source,
                "session_id": repair.session_id,
            }
            for repair in report.repairs
        ],
        "written": report.written,
    }


def _tick_json(result: TickResult) -> dict[str, JSONValue]:
    return {
        "job": _job_json(result.job) if result.job is not None else None,
        "enqueued": list(result.enqueued),
        "delivered": list(result.delivered),
        "lanes": [_lane_json(lane) for lane in result.lanes],
        "hold": list(result.hold),
        "handoff": str(result.handoff) if result.handoff is not None else None,
    }


def _sample_json(sample: HostSample) -> dict[str, JSONValue]:
    return {
        "memory_percent": sample.memory_percent,
        "disk_gib": sample.disk_gib,
        "load": sample.load,
        "active_lanes": sample.active_lanes,
    }


def _peer(config: Config, name: str) -> RelayPeer:
    for peer in config.peers:
        if peer.name == name:
            return peer
    raise CliError(f"unknown relay peer: {name!r}")


def _cmd_init(args: argparse.Namespace, _factory: RuntimeFactory) -> int:
    config = init_workspace(_workspace(args))
    _emit(
        {
            "ok": True,
            "workspace": str(config.workspace),
            "config_path": str(config.config_path),
            "work_dir": str(config.work_dir),
            "vault_dir": str(config.vault_dir),
            "state_dir": str(config.state_dir),
        }
    )
    return 0


def _cmd_doctor(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    config = _load(args)
    runtime = factory(config)
    statuses = runtime.messenger.doctor()
    command0 = config.command[0] if config.command else ""
    result: dict[str, JSONValue] = {
        "ok": True,
        "workspace": str(config.workspace),
        "config_path": str(config.config_path),
        "command": list(config.command),
        "command_available": bool(command0) and shutil.which(command0) is not None,
        "messengers": [
            {
                "platform": status.platform,
                "enabled": status.enabled,
                "typing": status.typing,
                "presence": status.presence,
                "token_env": status.token_env,
                "cookie_env": status.cookie_env,
                "users": list(status.users),
                "channels": list(status.channels),
            }
            for status in statuses
        ],
        "live": None,
        "channel_access": "not checked (use --live; this build validates identity only)",
    }
    exit_code = 0
    if bool(args.live):
        identities: list[JSONValue] = []
        for status in statuses:
            if not status.enabled:
                continue
            entry: dict[str, JSONValue] = {
                "platform": status.platform,
                "ok": True,
                "identity": None,
                "error": None,
            }
            try:
                entry["identity"] = runtime.messenger.identity(status.platform)
            except (MessengerError, TransportError, OSError, ValueError) as exc:
                entry["ok"] = False
                entry["error"] = str(exc)
                exit_code = 1
            identities.append(entry)
        result["live"] = {
            "identities": identities,
            "channel_access": (
                "identity-only: no channel-validation API is available in this "
                "build, so channel access was NOT checked"
            ),
        }
        result["ok"] = exit_code == 0
    _emit(result)
    return exit_code


def _cmd_submit(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    lane = _opt_str(args, "lane") or DEFAULT_LANE
    if runtime.registry.get(lane).status == "closed":
        raise CliError("cannot submit work to a closed lane")
    submission = Submission(
        source_key=f"cli:{uuid.uuid4().hex}",
        prompt=str(args.text),
        approved=bool(args.approve),
        lane=lane,
    )
    job_id = runtime.store.enqueue(submission)
    _emit({"ok": True, "job": _job_json(runtime.store.get_job(job_id))})
    return 0


def _cmd_approve(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    runtime.store.approve(str(args.id))
    _emit({"ok": True, "job": _job_json(runtime.store.get_job(str(args.id)))})
    return 0


def _cmd_status(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    jobs = runtime.store.list_jobs()
    deliveries = runtime.store.deliveries()
    lanes = runtime.registry.list()
    _emit(
        {
            "ok": True,
            "workspace": str(runtime.config.workspace),
            "jobs": [_job_json(job) for job in jobs],
            "lanes": [_lane_json(lane) for lane in lanes],
            "deliveries": [_delivery_json(delivery) for delivery in deliveries],
            "pending_approval": [job.id for job in jobs if job.status == "awaiting_approval"],
            "uncertain_jobs": [job.id for job in jobs if job.status == "uncertain"],
            "uncertain_deliveries": [
                delivery.id for delivery in deliveries if delivery.status == "uncertain"
            ],
        }
    )
    return 0


def _cmd_once(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    stop = threading.Event()
    with _signal_stop(stop):
        result = runtime.once()
    failed = result.job is not None and result.job.status in ("failed", "uncertain")
    _emit({"ok": not result.hold and not failed, "tick": _tick_json(result)})
    if result.hold:
        return 1
    job = result.job
    if job is not None and job.status in ("failed", "uncertain"):
        return 1
    return 0


def _cmd_run(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    stop = threading.Event()
    with _signal_stop(stop):
        try:
            runtime.run(stop, lambda tick: _emit({"ok": True, "tick": _tick_json(tick)}))
        except KeyboardInterrupt:
            pass
    _emit({"ok": True, "stopped": True})
    return 0


def _cmd_resolve(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    if not bool(args.acknowledge_uncertain_effects):
        raise CliError("resolve requires --acknowledge-uncertain-effects")
    runtime = _runtime(args, factory)
    runtime.store.resolve(str(args.id))
    _emit({"ok": True, "resolved": str(args.id)})
    return 0


def _cmd_retry(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    if not bool(args.acknowledge_uncertain_effects):
        raise CliError("retry requires --acknowledge-uncertain-effects")
    runtime = _runtime(args, factory)
    original = runtime.store.get_job(str(args.id))
    resolved_original = original.status == "uncertain"
    if resolved_original:
        runtime.store.resolve(original.id)
    new_id = runtime.store.retry(original.id)
    _emit(
        {
            "ok": True,
            "retry_of": original.id,
            "resolved_original": resolved_original,
            "job": _job_json(runtime.store.get_job(new_id)),
        }
    )
    return 0


def _cmd_resend(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    if not bool(args.acknowledge_possible_duplicate):
        raise CliError("resend requires --acknowledge-possible-duplicate")
    runtime = _runtime(args, factory)
    new_id = runtime.store.resend(str(args.id))
    found = [item for item in runtime.store.deliveries() if item.id == new_id]
    if not found:
        raise CliError("resend did not produce a delivery")
    _emit({"ok": True, "resend_of": str(args.id), "delivery": _delivery_json(found[0])})
    return 0


def _cmd_rotate(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    lane = _opt_str(args, "lane") or DEFAULT_LANE
    path = runtime.rotate(lane, str(args.reason))
    _emit({"ok": True, "lane": lane, "handoff": str(path)})
    return 0


def _cmd_memory_write(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    path = runtime.memory.write(str(args.title), str(args.body), str(args.source))
    _emit({"ok": True, "title": str(args.title), "path": str(path)})
    return 0


def _cmd_memory_search(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    text = runtime.memory.recall(str(args.query))
    _emit({"ok": True, "query": str(args.query), "result": text})
    return 0


def _cmd_memory_get(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    content = runtime.memory.get(str(args.title))
    if content is None:
        raise CliError(f"memory note not found: {args.title!r}")
    _emit({"ok": True, "title": str(args.title), "content": content})
    return 0


def _cmd_lane_launch(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    done = str(args.done)
    _ = parse_signals(done, runtime.config)
    sample = sample_host(runtime.config, len(runtime.registry.list(open_only=True)))
    decision = can_launch(sample, runtime.config.host)
    if not decision.ok:
        raise CliError("host HOLD before launch: " + "; ".join(decision.reasons))
    cwd = Path(str(args.cwd)).expanduser() if _opt_str(args, "cwd") else None
    worktrees = tuple(Path(item) for item in _str_list(args.worktree))
    lane = runtime.registry.launch(
        str(args.key),
        str(args.role),
        done,
        cwd=cwd,
        host=str(args.host),
        worktrees=worktrees,
    )
    _emit({"ok": True, "lane": _lane_json(lane)})
    return 0


def _cmd_lane_adopt(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    lane = runtime.registry.adopt(str(args.key), Path(str(args.session_file)))
    _emit({"ok": True, "lane": _lane_json(lane)})
    return 0


def _cmd_lane_list(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    _emit({"ok": True, "lanes": [_lane_json(lane) for lane in runtime.registry.list()]})
    return 0


def _cmd_lane_sync(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    report = runtime.registry.sync(write=bool(args.write))
    _emit({"ok": True, "report": _sync_json(report)})
    return 0


def _cmd_lane_claim(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    lane = runtime.completion.claim(str(args.key), str(args.evidence), runtime.clock())
    _emit({"ok": True, "lane": _lane_json(lane)})
    return 0


def _cmd_lane_object(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    reasons = _str_list(args.reason)
    lane = runtime.registry.object(str(args.key), reasons, runtime.clock())
    _emit({"ok": True, "lane": _lane_json(lane)})
    return 0


def _cmd_lane_watch(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    key = _opt_str(args, "key")
    follow = bool(args.follow)
    stop = threading.Event()
    with _signal_stop(stop):
        while True:
            _emit({"ok": True, "snapshot": _watch_snapshot(runtime, key)})
            if not follow or stop.wait(runtime.config.poll_seconds):
                break
    return 0


def _watch_snapshot(runtime: Runtime, key: str | None) -> dict[str, JSONValue]:
    if key is None:
        return {"lanes": [_lane_json(lane) for lane in runtime.registry.list()]}
    lane = runtime.registry.get(key)
    return {
        "lane": _lane_json(lane),
        "history": [_event_json(event) for event in runtime.registry.history(key)],
    }


def _cmd_lane_close(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    with _signal_stop(threading.Event()):
        lane = runtime.close_lane(str(args.key))
    closed = lane.status == "closed"
    _emit({"ok": closed, "lane": _lane_json(lane)})
    return 0 if closed else 1


def _cmd_host_can_launch(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    sample = sample_host(runtime.config, len(runtime.registry.list(open_only=True)))
    decision = can_launch(sample, runtime.config.host)
    reasons = list(decision.reasons)
    slot = HeavySlot(runtime.config.host)
    if not slot.acquire():
        reasons.append("host heavy slot is busy or unavailable")
    slot.release()
    _emit(
        {
            "ok": not reasons,
            "decision": "HOLD" if reasons else "READY",
            "reasons": reasons,
            "sample": _sample_json(sample),
        }
    )
    return 1 if reasons else 0


def _cmd_host_status(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    sample = sample_host(runtime.config, len(runtime.registry.list(open_only=True)))
    _emit({"ok": True, "sample": _sample_json(sample)})
    return 0


def _cmd_messenger_send(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    platform = _platform(args.platform)
    target = str(args.to)
    text = str(args.text)
    edit = _opt_str(args, "edit")
    if edit is not None:
        remote_id = runtime.messenger.edit(platform, target, edit, text)
        _emit(
            {
                "ok": True,
                "platform": platform,
                "channel": target,
                "message_id": edit,
                "remote_id": remote_id,
            }
        )
        return 0
    thread = _opt_str(args, "thread")
    ids = runtime.messenger.send(platform, target, text, thread_id=thread)
    drained: list[str] = []
    runner = "acquired"
    try:
        with runtime.lock:
            drained = runtime.messenger.drain()
    except RunnerBusy:
        runner = "busy"
    wanted = set(ids)
    statuses = {
        delivery.id: delivery.status
        for delivery in runtime.store.deliveries()
        if delivery.id in wanted
    }
    uncertain = sorted(key for key, value in statuses.items() if value == "uncertain")
    sent = bool(statuses) and all(value == "sent" for value in statuses.values())
    _emit(
        {
            "ok": not uncertain,
            "platform": platform,
            "channel": target,
            "delivery_ids": ids,
            "drained": drained,
            "statuses": statuses,
            "state": "sent" if sent else "queued",
            "runner": runner,
        }
    )
    return 1 if uncertain else 0


def _cmd_messenger_inbound(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    platform = _platform(args.platform)
    once = bool(args.once)
    loop = bool(args.loop)
    if once == loop:
        raise CliError("messenger inbound requires exactly one of --once or --loop")
    stop = threading.Event()
    with _signal_stop(stop):
        while True:
            try:
                with runtime.lock:
                    job_ids = runtime.messenger.poll(platform)
            except RunnerBusy as exc:
                raise CliError(f"runner lock is held; retry later: {exc}") from exc
            _emit({"ok": True, "platform": platform, "job_ids": job_ids})
            if not loop or stop.wait(runtime.config.poll_seconds):
                break
    return 0


def _cmd_messenger_typing(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    platform = _platform(args.platform)
    runtime.messenger.typing(platform, str(args.to), _opt_str(args, "thread"))
    _emit({"ok": True, "platform": platform, "to": str(args.to)})
    return 0


def _cmd_messenger_presence(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    runtime = _runtime(args, factory)
    platform = _platform(args.platform)
    session = runtime.messenger.presence(platform)
    duration: float | None = args.seconds
    if duration is not None and (not math.isfinite(duration) or duration <= 0):
        raise CliError("--seconds must be positive and finite")
    stop = threading.Event()
    with _signal_stop(stop):
        with session:
            if not session.wait_ready(45):
                raise PresenceError(session.error or "presence connection was not established")
            _emit({
                "ok": True, "platform": platform, "connected": True,
                "available": session.available, "phase": session.phase,
            })
            try:
                session.wait_finished(duration)
            except KeyboardInterrupt:
                pass
    error = session.error
    _emit(
        {
            "ok": error is None,
            "platform": platform,
            "connected": session.connected,
            "available": session.available,
            "phase": session.phase,
            "error": error,
        }
    )
    return 1 if error is not None else 0


def _cmd_relay_serve(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    config = _load(args)
    runtime = factory(config)
    stop = threading.Event()
    with _signal_stop(stop):
        with create_relay_server(config, runtime.store) as server:
            _emit({
                "ok": True, "serving": True,
                "bind": server.server_address[0], "port": server.server_address[1],
            })
            try:
                server.serve_forever(poll_interval=0.1)
            except KeyboardInterrupt:
                pass
    _emit({"ok": True, "serving": False, "stopped": True})
    return 0


def _cmd_relay_push(args: argparse.Namespace, factory: RuntimeFactory) -> int:
    config = _load(args)
    runtime = factory(config)
    peer = _peer(config, str(args.peer))
    receipts = push_status_file(config, runtime.store, peer, Path(str(args.file)))
    _emit({"ok": True, "peer": peer.name, "receipts": receipts})
    return 0


def _cmd_service_render(args: argparse.Namespace, _factory: RuntimeFactory) -> int:
    config = _load(args)
    print(service.render_service(config.workspace), flush=True)
    return 0


def _cmd_service_install(args: argparse.Namespace, _factory: RuntimeFactory) -> int:
    config = _load(args)
    unit_dir = Path(str(args.unit_dir)).expanduser() if _opt_str(args, "unit_dir") else None
    path = service.install_service(config, unit_dir=unit_dir, enable=bool(args.enable))
    _emit({"ok": True, "unit_path": str(path), "enabled": bool(args.enable)})
    return 0


def _cmd_keeper(args: argparse.Namespace, _factory: RuntimeFactory) -> int:
    config = _load(args)
    argv = _str_list(args.command_argv)
    if argv and argv[0] == "--":
        argv = argv[1:]
    if not argv:
        raise CliError("keeper requires a command after its options")
    cwd = Path(str(args.cwd)).expanduser() if _opt_str(args, "cwd") else config.work_dir
    stop = threading.Event()
    with _signal_stop(stop):
        try:
            code = service.keeper(tuple(argv), cwd, stop)
        except KeyboardInterrupt:
            code = 0
    _emit({"ok": code == 0, "exit_code": code})
    return 0 if code == 0 else 1


def _cmd_skill_path(_args: argparse.Namespace, _factory: RuntimeFactory) -> int:
    base = Path(__file__).resolve().parent / "skills" / "ultra-agent"
    _emit(
        {
            "ok": True,
            "skill_path": str(base),
            "skill_file": str(base / "SKILL.md"),
        }
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Return the complete argparse tree for the ultra-agent CLI."""
    parser = argparse.ArgumentParser(
        prog="ultra-agent",
        description="A locally operated omo agent runtime (data results are JSON).",
    )
    parser.add_argument(
        "--workspace",
        metavar="PATH",
        default=os.environ.get("UA_WORKSPACE"),
        help="workspace directory; must come before the command (env UA_WORKSPACE)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    _ = sub.add_parser("init", help="create the workspace, private directories and config")

    doctor = sub.add_parser("doctor", help="report configuration and provider status")
    doctor.add_argument(
        "--live",
        action="store_true",
        help="check live provider identities (identity-only; channel access is not checked)",
    )

    submit = sub.add_parser("submit", help="enqueue work; without --approve it waits")
    submit.add_argument("text")
    submit.add_argument("--approve", action="store_true")
    submit.add_argument("--lane", default=DEFAULT_LANE)

    approve = sub.add_parser("approve", help="approve one awaiting job")
    approve.add_argument("id")

    _ = sub.add_parser("status", help="print durable jobs, lanes and deliveries as JSON")
    _ = sub.add_parser("once", help="run at most one approved job and exit")
    _ = sub.add_parser("run", help="run the daemon loop until SIGINT/SIGTERM")

    resolve = sub.add_parser("resolve", help="reconcile one uncertain job or delivery")
    resolve.add_argument("id")
    resolve.add_argument(
        "--acknowledge-uncertain-effects",
        action="store_true",
        dest="acknowledge_uncertain_effects",
    )

    retry = sub.add_parser("retry", help="create a linked new job for a finished/uncertain job")
    retry.add_argument("id")
    retry.add_argument(
        "--acknowledge-uncertain-effects",
        action="store_true",
        dest="acknowledge_uncertain_effects",
    )

    resend = sub.add_parser("resend", help="queue a fresh copy of one delivery")
    resend.add_argument("id")
    resend.add_argument(
        "--acknowledge-possible-duplicate",
        action="store_true",
        dest="acknowledge_possible_duplicate",
    )

    rotate = sub.add_parser("rotate", help="write a handoff and install a fresh session")
    rotate.add_argument("--reason", required=True)
    rotate.add_argument("--lane", default=DEFAULT_LANE)

    memory = sub.add_parser("memory", help="durable Mnemosyne memory")
    memory_sub = memory.add_subparsers(dest="memory_command", required=True)
    memory_write = memory_sub.add_parser("write")
    memory_write.add_argument("title")
    memory_write.add_argument("body")
    memory_write.add_argument("--source", required=True)
    memory_search = memory_sub.add_parser("search")
    memory_search.add_argument("query")
    memory_get = memory_sub.add_parser("get")
    memory_get.add_argument("title")

    lane = sub.add_parser("lane", help="tracked work lanes")
    lane_sub = lane.add_subparsers(dest="lane_command", required=True)
    lane_launch = lane_sub.add_parser("launch")
    lane_launch.add_argument("key")
    lane_launch.add_argument("--role", required=True)
    lane_launch.add_argument(
        "--done",
        required=True,
        help="Done grammar: command;pr:OWNER/REPO#N;issue:OWNER/REPO#N;npm:PACKAGE@VERSION",
    )
    lane_launch.add_argument("--cwd")
    lane_launch.add_argument("--host", default="local")
    lane_launch.add_argument("--worktree", action="append", default=None)
    lane_adopt = lane_sub.add_parser("adopt")
    lane_adopt.add_argument("key")
    lane_adopt.add_argument("--session-file", required=True, dest="session_file")
    _ = lane_sub.add_parser("list")
    lane_sync = lane_sub.add_parser("sync")
    lane_sync.add_argument("--write", action="store_true")
    lane_claim = lane_sub.add_parser("claim")
    lane_claim.add_argument("key")
    lane_claim.add_argument("--evidence", required=True)
    lane_object = lane_sub.add_parser("object")
    lane_object.add_argument("key")
    lane_object.add_argument("--reason", action="append", required=True, default=None)
    lane_watch = lane_sub.add_parser("watch")
    lane_watch.add_argument("key", nargs="?", default=None)
    lane_watch.add_argument("--follow", action="store_true")
    lane_close = lane_sub.add_parser("close")
    lane_close.add_argument("key")

    host = sub.add_parser("host", help="host guard")
    host_sub = host.add_subparsers(dest="host_command", required=True)
    _ = host_sub.add_parser("can-launch")
    _ = host_sub.add_parser("status")

    messenger = sub.add_parser("messenger", help="unified Discord/Telegram/Slack surface")
    messenger_sub = messenger.add_subparsers(dest="messenger_command", required=True)
    messenger_send = messenger_sub.add_parser("send")
    messenger_send.add_argument("platform", choices=_PLATFORMS)
    messenger_send.add_argument("--to", required=True)
    messenger_send.add_argument("--text", required=True)
    messenger_send.add_argument("--thread", default=None)
    messenger_send.add_argument("--edit", default=None)
    messenger_inbound = messenger_sub.add_parser("inbound")
    messenger_inbound.add_argument("platform", choices=_PLATFORMS)
    messenger_inbound.add_argument(
        "--once", action="store_true", help="poll once and exit (required unless --loop)"
    )
    messenger_inbound.add_argument(
        "--loop", action="store_true", help="poll until interrupted (explicit alternative)"
    )
    messenger_typing = messenger_sub.add_parser("typing")
    messenger_typing.add_argument("platform", choices=_PLATFORMS)
    messenger_typing.add_argument("--to", required=True)
    messenger_typing.add_argument("--thread", default=None)
    messenger_presence = messenger_sub.add_parser("presence")
    messenger_presence.add_argument("platform", choices=_PLATFORMS)
    messenger_presence.add_argument(
        "--seconds", type=float, default=None,
        help="hold duration after connection; default stays online until interrupted",
    )

    relay = sub.add_parser("relay", help="authenticated multi-machine status relay")
    relay_sub = relay.add_subparsers(dest="relay_command", required=True)
    _ = relay_sub.add_parser("serve")
    relay_push = relay_sub.add_parser("push")
    relay_push.add_argument("peer")
    relay_push.add_argument("--file", required=True)

    service_parser = sub.add_parser(
        "service",
        help="systemd user service; render prints plain unit text (not JSON)",
    )
    service_sub = service_parser.add_subparsers(dest="service_command", required=True)
    _ = service_sub.add_parser("render", help="print the plain systemd unit text (not JSON)")
    service_install = service_sub.add_parser("install")
    service_install.add_argument("--enable", action="store_true")
    service_install.add_argument("--unit-dir", default=None, dest="unit_dir")

    keeper = sub.add_parser("keeper", help="supervise a child process without systemd")
    keeper.add_argument("--cwd", default=None)
    keeper.add_argument("command_argv", nargs=argparse.REMAINDER)

    _ = sub.add_parser("skill-path", help="print the packaged operator skill path")

    return parser


_SIMPLE_HANDLERS: Final[dict[str, CommandHandler]] = {
    "init": _cmd_init,
    "doctor": _cmd_doctor,
    "submit": _cmd_submit,
    "approve": _cmd_approve,
    "status": _cmd_status,
    "once": _cmd_once,
    "run": _cmd_run,
    "resolve": _cmd_resolve,
    "retry": _cmd_retry,
    "resend": _cmd_resend,
    "rotate": _cmd_rotate,
    "keeper": _cmd_keeper,
    "skill-path": _cmd_skill_path,
}

_GROUP_HANDLERS: Final[dict[str, dict[str, CommandHandler]]] = {
    "memory": {
        "write": _cmd_memory_write,
        "search": _cmd_memory_search,
        "get": _cmd_memory_get,
    },
    "lane": {
        "launch": _cmd_lane_launch,
        "adopt": _cmd_lane_adopt,
        "list": _cmd_lane_list,
        "sync": _cmd_lane_sync,
        "claim": _cmd_lane_claim,
        "object": _cmd_lane_object,
        "watch": _cmd_lane_watch,
        "close": _cmd_lane_close,
    },
    "host": {
        "can-launch": _cmd_host_can_launch,
        "status": _cmd_host_status,
    },
    "messenger": {
        "send": _cmd_messenger_send,
        "inbound": _cmd_messenger_inbound,
        "typing": _cmd_messenger_typing,
        "presence": _cmd_messenger_presence,
    },
    "relay": {
        "serve": _cmd_relay_serve,
        "push": _cmd_relay_push,
    },
    "service": {
        "render": _cmd_service_render,
        "install": _cmd_service_install,
    },
}


def _dispatch(args: argparse.Namespace) -> CommandHandler:
    command: object = args.command
    if not isinstance(command, str):
        raise CliError("no command given")
    handler = _SIMPLE_HANDLERS.get(command)
    if handler is not None:
        return handler
    group = _GROUP_HANDLERS.get(command)
    if group is None:
        raise CliError(f"unknown command: {command!r}")
    nested_name: object = getattr(args, f"{command}_command", None)
    nested = group.get(str(nested_name))
    if nested is None:
        raise CliError(f"unknown {command} subcommand: {nested_name!r}")
    return nested


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_factory: RuntimeFactory = Runtime,
) -> int:
    """Run one CLI invocation and return its process exit code.

    ``runtime_factory`` builds a real :class:`~ultra_agent.runtime.Runtime` from
    a parsed config. It is the only injection seam and exists so a caller can
    supply a runtime whose messenger uses a loopback transport; no handler
    substitutes a fake runtime for real behavior.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = _dispatch(args)
    try:
        return handler(args, runtime_factory)
    except KeyboardInterrupt:
        _emit({"ok": False, "interrupted": True})
        return 130
    except (
        CliError,
        ConfigError,
        StoreError,
        RegistryError,
        SignalError,
        MessengerError,
        CapabilityError,
        SlackError,
        UnsupportedCapabilityError,
        TelegramError,
        PresenceError,
        RelayError,
        ServiceError,
        RunnerBusy,
        SlotUnavailable,
        TransportError,
        OSError,
        ValueError,
    ) as exc:
        _emit_error(str(exc))
        return 1
