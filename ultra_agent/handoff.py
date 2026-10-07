"""Durable context handoff snapshots written before a lane's session is reset.

A terminal compaction failure or an explicit operator rotation must not lose
what a lane was doing. :func:`write_handoff` captures one lane's exact identity,
session, unfinished jobs, standing no-replay rules, operator-configured command
surface and filesystem references into a unique Markdown file that carries a
machine-readable JSON snapshot, and mirrors a provenance-backed note into the
real Mnemosyne vault.

The ordering is deliberate and fail-closed:

1. The provenance note is written first, through the real Mnemosyne vault.
2. The handoff file is persisted with an atomic replace, then the file and its
   directory are fsynced and the file is made private.
3. Only after both succeed is the ``handoff:<lane>`` checkpoint advanced to the
   complete file path.

A disk or memory failure therefore raises before the checkpoint moves, leaving
the previous pointer untouched and the lane session unchanged. This module never
rotates a session, never launches a process and never emits a readiness marker;
the caller owns rotation and readiness after this function returns a path.

No secret value is read here: credentials live only in the environment, and the
snapshot records configuration fields and credential *names* only.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from ultra_agent.memory import Memory
from ultra_agent.models import Config, JobStatus, JSONValue
from ultra_agent.registry import Lane
from ultra_agent.store import Store

HANDOFF_VERSION: Final = 1
"""Schema version of the machine-readable snapshot embedded in the file."""

HANDOFF_KIND: Final = "ultra-agent-handoff"
"""Discriminator written into every snapshot."""

CHECKPOINT_PREFIX: Final = "handoff:"
"""Checkpoint key prefix: ``handoff:<lane key>`` points at the newest file."""

MEMORY_SOURCE_PREFIX: Final = "ultra-agent handoff"
"""Mnemosyne source recorded for the provenance note of every handoff."""

OPEN_JOB_STATUSES: Final[tuple[JobStatus, ...]] = (
    "awaiting_approval",
    "queued",
    "running",
    "uncertain",
)
"""Job states that are still open or queued and must survive a handoff."""

NEXT_ACTIONS: Final[dict[JobStatus, str]] = {
    "awaiting_approval": "await local approval before it can run",
    "queued": "queued; runs when this lane's session is used",
    "running": (
        "was running when the handoff was written; reconcile to uncertain "
        "before any retry and never replay it automatically"
    ),
    "uncertain": (
        "outcome is unknown; resolve or retry only with an explicit "
        "acknowledgement of possible prior effects"
    ),
    "completed": "completed; no further action",
    "failed": "failed; retry only with acknowledgement or abandon it",
}
"""Human next-step text per job state (data, not asserted prose)."""

_SAFE_KEY = re.compile(r"[^a-z0-9._-]+")


def handoff_checkpoint_key(lane_key: str) -> str:
    """Return the durable checkpoint key that points at a lane's handoff."""
    return f"{CHECKPOINT_PREFIX}{lane_key}"


def _safe_key(lane_key: str) -> str:
    """Reduce a lane key to a filename-safe token (defensive, never absolute)."""
    cleaned = _SAFE_KEY.sub("_", lane_key).strip("._-")
    return cleaned or "lane"


def _memory_title(lane: Lane) -> str:
    return f"handoff {lane.key}"


def _memory_source(lane: Lane) -> str:
    return f"{MEMORY_SOURCE_PREFIX} {lane.key}"


def _human_explanation(lane: Lane, reason: str, open_jobs: int) -> str:
    """Concise operator-facing explanation placed above the JSON snapshot."""
    return (
        f"Handoff for lane {lane.key!r} (role {lane.role!r}, host {lane.host!r}) "
        f"at {lane.cwd}.\n\n"
        f"Reason: {reason}\n\n"
        f"The lane keeps session {lane.session_id} and {open_jobs} unfinished "
        f"job(s). The JSON snapshot below is the authoritative machine-readable "
        f"record: it lists every open job with its exact state and next action, "
        f"the standing approval and no-replay rules, the configured command "
        f"surface and the paths needed to resume. No secret values are included."
    )


def _lane_snapshot(lane: Lane) -> dict[str, JSONValue]:
    return {
        "key": lane.key,
        "role": lane.role,
        "session_id": lane.session_id,
        "cwd": str(lane.cwd),
        "host": lane.host,
        "done": lane.done,
        "worktrees": [str(item) for item in lane.worktrees],
        "status": lane.status,
        "revision": lane.revision,
        "session_file": str(lane.session_file) if lane.session_file is not None else None,
        "claim": {
            "claim_at": lane.claim_at,
            "deadline": lane.deadline,
            "evidence": lane.evidence,
            "claim_emitted": lane.claim_emitted,
        },
    }


def _jobs_snapshot(store: Store, lane: Lane) -> list[JSONValue]:
    jobs: list[JSONValue] = []
    for job in store.list_jobs():
        if job.lane != lane.key or job.status not in OPEN_JOB_STATUSES:
            continue
        jobs.append(
            {
                "id": job.id,
                "source_key": job.source_key,
                "status": job.status,
                "prompt": job.prompt,
                "approved": job.approved,
                "lane": job.lane,
                "platform": job.platform,
                "heavy": job.heavy,
                "session_id": job.session_id,
                "retry_of": job.retry_of,
                "error": job.error,
                "next_action": NEXT_ACTIONS[job.status],
            }
        )
    return jobs


def _lane_history(store: Store, lane_key: str) -> list[JSONValue]:
    """Read the lane's immutable revision history, when the registry exists."""
    with store.connection() as connection:
        rows = connection.execute(
            "SELECT revision, at, status, note, source FROM lane_history "
            "WHERE key = ? ORDER BY revision",
            (lane_key,),
        ).fetchall()
    history: list[JSONValue] = []
    for row in rows:
        history.append(
            {
                "revision": row.get("revision"),
                "at": row.get("at"),
                "status": row.get("status"),
                "note": row.get("note"),
                "source": row.get("source"),
            }
        )
    return history


def _snapshot(config: Config, store: Store, lane: Lane, reason: str) -> dict[str, JSONValue]:
    return {
        "version": HANDOFF_VERSION,
        "kind": HANDOFF_KIND,
        "reason": reason,
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "lane": _lane_snapshot(lane),
        "jobs": _jobs_snapshot(store, lane),
        "rules": {
            "remote_work_requires_local_approval": True,
            "tools_disabled_unless_configured": not config.tools,
            "uncertain_jobs_are_not_replayed": True,
            "reconciliation_requires_explicit_acknowledgement": True,
            "completion_claims_require_live_verification": True,
            "worktrees_are_never_deleted_or_pushed": True,
        },
        "config": {
            "command": list(config.command),
            "model": config.model,
            "timeout_seconds": config.timeout_seconds,
            "poll_seconds": config.poll_seconds,
            "tools": list(config.tools),
            "verification_command": list(config.verification_command),
            "writing_style": config.writing_style,
        },
        "paths": {
            "workspace": str(config.workspace),
            "work_dir": str(config.work_dir),
            "state_dir": str(config.state_dir),
            "vault_dir": str(config.vault_dir),
            "sessions_dir": str(config.sessions_dir),
            "handoffs_dir": str(config.handoffs_dir),
            "db_path": str(config.db_path),
            "config_path": str(config.config_path),
        },
        "sessions": {
            "current_session_id": lane.session_id,
            "session_file": str(lane.session_file) if lane.session_file is not None else None,
            "sessions_dir": str(config.sessions_dir),
            "lane_revision": lane.revision,
            "history": _lane_history(store, lane.key),
        },
    }


def _render(document: dict[str, JSONValue], human: str) -> str:
    payload = json.dumps(document, indent=2, ensure_ascii=False, sort_keys=False)
    return (
        "# ultra-agent handoff\n\n"
        f"{human}\n\n"
        "## Snapshot\n\n"
        "```json\n"
        f"{payload}\n"
        "```\n"
    )


def _fsync_directory(directory: Path) -> None:
    """Fsync a directory so a rename survives a crash."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(directory, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` via a private temp file and atomic replace."""
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temp_name = tempfile.mkstemp(dir=directory, prefix=f".{path.name}.", suffix=".tmp")
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_path, 0o600)
        os.replace(temp_path, path)
        _fsync_directory(directory)
    except BaseException:
        try:
            temp_path.unlink()
        except OSError:
            pass
        raise


def write_handoff(config: Config, store: Store, lane: Lane, reason: str) -> Path:
    """Persist one durable handoff for ``lane`` and return the snapshot path.

    The provenance note is written to the real Mnemosyne vault, then the
    machine-readable Markdown file is written atomically and privately, then the
    ``handoff:<lane>`` checkpoint is advanced to that file. Any failure raises
    before the checkpoint moves, so the previous pointer and the lane session
    are never replaced by an incomplete handoff.
    """
    if not reason.strip():
        raise ValueError("handoff requires a non-empty reason")

    open_jobs = [
        job
        for job in store.list_jobs()
        if job.lane == lane.key and job.status in OPEN_JOB_STATUSES
    ]
    human = _human_explanation(lane, reason, len(open_jobs))

    memory = Memory(config.vault_dir)
    note_path = memory.write(_memory_title(lane), human, _memory_source(lane))

    snapshot = _snapshot(config, store, lane, reason)
    snapshot["memory"] = {
        "title": _memory_title(lane),
        "source": _memory_source(lane),
        "vault": str(config.vault_dir),
        "note_path": str(note_path),
    }

    name = f"handoff-{_safe_key(lane.key)}-{uuid.uuid4().hex}.md"
    path = config.handoffs_dir / name
    _atomic_write(path, _render(snapshot, human))

    store.set_checkpoint(handoff_checkpoint_key(lane.key), str(path))
    return path


__all__ = [
    "CHECKPOINT_PREFIX",
    "HANDOFF_KIND",
    "HANDOFF_VERSION",
    "MEMORY_SOURCE_PREFIX",
    "OPEN_JOB_STATUSES",
    "handoff_checkpoint_key",
    "write_handoff",
]
