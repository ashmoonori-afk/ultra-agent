"""Durable per-lane registry: exact sessions, status history and sync drift.

A lane is one tracked unit of longer work: a validated key, a role, a host, a
working directory, one exact persisted session UUID, an optional authoritative
session file, declared worktrees, Done criteria and a status history. Lane state
is deliberately separate from job state.

Two invariants make the registry trustworthy:

* The per-lane session UUID is written into the same ``sessions`` table the
  :class:`~ultra_agent.store.Store` uses, inside the same transaction as the
  lane row, so a lane and its session can never disagree.
* Every status change appends an immutable ``lane_history`` revision. A caller
  that captured a revision passes it back as ``expected_revision`` and a stale
  transition is refused (compare-and-swap), so an objection or re-claim can
  never be closed by a checker that read an older state.

``sync`` performs the authoritative drift check: it reads only the configured
sessions directory and each lane's explicitly adopted file, reports missing,
changed-header, missing-Done and unregistered files, and never adopts a file it
merely guesses belongs to a lane. ``sync(write=True)`` repairs only an
unambiguous authoritative header and refuses while the lane has running or
uncertain execution.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, TypeAlias, TypeGuard

from ultra_agent.models import Config, Job
from ultra_agent.store import Store

LaneStatus: TypeAlias = Literal[
    "working", "done-claimed", "verified-done", "not-done", "closed"
]
"""Durable lane lifecycle states; ``closed`` is terminal."""

SyncKind: TypeAlias = Literal[
    "pending", "missing", "changed-header", "missing-done", "unregistered-file"
]
"""Categories of sync observation."""

LANE_KEY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{1,60}$")
"""Validated lane keys: lowercase alphanumeric start, then a bounded suffix."""

CLAIM_SECONDS = 300.0
"""Exact objection window opened by one completion claim."""

_LANE_STATUSES: tuple[LaneStatus, ...] = (
    "working",
    "done-claimed",
    "verified-done",
    "not-done",
    "closed",
)

_ACTIVE_JOB_STATUSES: tuple[str, ...] = ("running", "uncertain")

_LANE_SCHEMA: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS lanes (
        key TEXT PRIMARY KEY,
        role TEXT NOT NULL,
        cwd TEXT NOT NULL,
        host TEXT NOT NULL,
        session_id TEXT NOT NULL,
        session_file TEXT,
        done TEXT NOT NULL,
        worktrees TEXT NOT NULL,
        status TEXT NOT NULL,
        revision INTEGER NOT NULL,
        claim_at REAL,
        deadline REAL,
        evidence TEXT NOT NULL DEFAULT '',
        claim_emitted INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS lane_history (
        key TEXT NOT NULL,
        revision INTEGER NOT NULL,
        at REAL NOT NULL,
        status TEXT NOT NULL,
        note TEXT NOT NULL,
        source TEXT NOT NULL,
        PRIMARY KEY (key, revision)
    )
    """,
)


class RegistryError(RuntimeError):
    """A lane operation was refused: unknown lane, conflict or stale revision."""


@dataclass(frozen=True, slots=True)
class LaneEvent:
    """One immutable, revision-numbered lane status record."""

    key: str
    revision: int
    at: float
    status: LaneStatus
    note: str
    source: str


@dataclass(frozen=True, slots=True)
class Lane:
    """One tracked lane and its current, revisioned state."""

    key: str
    role: str
    cwd: Path
    host: str
    session_id: str
    session_file: Path | None
    done: str
    worktrees: tuple[Path, ...]
    status: LaneStatus
    revision: int
    claim_at: float | None = None
    deadline: float | None = None
    evidence: str = ""
    claim_emitted: bool = False


@dataclass(frozen=True, slots=True)
class SyncFinding:
    """One observation made by :meth:`Registry.sync`."""

    kind: SyncKind
    key: str | None
    path: Path | None
    detail: str


@dataclass(frozen=True, slots=True)
class SyncRepair:
    """One authoritative header repair written by ``sync(write=True)``."""

    key: str
    revision: int
    source: str
    session_id: str


@dataclass(frozen=True, slots=True)
class SyncReport:
    """Typed result of one authoritative sync scan."""

    scanned: tuple[Path, ...]
    findings: tuple[SyncFinding, ...]
    repairs: tuple[SyncRepair, ...]
    written: bool


def _text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _int(value: object) -> int:
    return value if isinstance(value, int) else 0


def _flag(value: object) -> bool:
    return value if isinstance(value, bool) else bool(value)


def _opt_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _lane_status(value: object) -> LaneStatus:
    if isinstance(value, str):
        for status in _LANE_STATUSES:
            if value == status:
                return status
    raise RegistryError(f"unknown lane status stored in database: {value!r}")


def _worktrees(value: object) -> tuple[Path, ...]:
    if not isinstance(value, str):
        raise RegistryError("lane worktrees column is not JSON text")
    try:
        parsed: object = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RegistryError("lane worktrees column is not valid JSON") from exc
    if not _is_list(parsed):
        raise RegistryError("lane worktrees column is not a JSON array")
    paths: list[Path] = []
    for item in parsed:
        if not isinstance(item, str):
            raise RegistryError("lane worktree entry is not a string")
        paths.append(Path(item))
    return tuple(paths)


def _is_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _is_dict(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict)


def _row_to_lane(row: Mapping[str, object]) -> Lane:
    return Lane(
        key=_text(row.get("key")),
        role=_text(row.get("role")),
        cwd=Path(_text(row.get("cwd"))),
        host=_text(row.get("host")),
        session_id=_text(row.get("session_id")),
        session_file=_opt_path(row.get("session_file")),
        done=_text(row.get("done")),
        worktrees=_worktrees(row.get("worktrees")),
        status=_lane_status(row.get("status")),
        revision=_int(row.get("revision")),
        claim_at=_opt_float(row.get("claim_at")),
        deadline=_opt_float(row.get("deadline")),
        evidence=_text(row.get("evidence")),
        claim_emitted=_flag(row.get("claim_emitted")),
    )


def _opt_path(value: object) -> Path | None:
    if isinstance(value, str) and value:
        return Path(value)
    return None


def _row_to_event(row: Mapping[str, object]) -> LaneEvent:
    return LaneEvent(
        key=_text(row.get("key")),
        revision=_int(row.get("revision")),
        at=_opt_float(row.get("at")) or 0.0,
        status=_lane_status(row.get("status")),
        note=_text(row.get("note")),
        source=_text(row.get("source")),
    )


def _read_header(path: Path) -> str:
    """Return the session id from a JSONL file's first (header) line."""
    if not path.is_file():
        raise RegistryError(f"session file does not exist: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            first = handle.readline(65537)
    except (OSError, UnicodeError) as exc:
        raise RegistryError(f"cannot read session file {path}: {exc}") from exc
    if not first.strip():
        raise RegistryError(f"session file has no header line: {path}")
    if len(first) > 65536:
        raise RegistryError("session header exceeds size limit")
    try:
        parsed: object = json.loads(first)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise RegistryError(f"session file header is not JSON: {path}") from exc
    if not _is_dict(parsed):
        raise RegistryError(f"session file header is not an object: {path}")
    header = parsed
    if header.get("type") != "session":
        raise RegistryError(f"session file header is not a session header: {path}")
    session_id = header.get("id")
    if not isinstance(session_id, str) or not session_id:
        raise RegistryError(f"session file header has no id: {path}")
    return session_id


def _read_header_or_none(path: Path) -> str | None:
    try:
        return _read_header(path)
    except RegistryError:
        return None


def _finite(now: float) -> None:
    if not math.isfinite(now):
        raise ValueError("timestamp must be finite")


def _insert_history(
    connection: sqlite3.Connection,
    key: str,
    revision: int,
    at: float,
    status: LaneStatus,
    note: str,
    source: str,
) -> None:
    connection.execute(
        """
        INSERT INTO lane_history (key, revision, at, status, note, source)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (key, revision, at, status, note, source),
    )


class Registry:
    """Durable, revisioned lane registry over the shared SQLite store."""

    def __init__(self, store: Store, config: Config) -> None:
        self._store = store
        self._config = config
        with store.connection() as connection:
            for statement in _LANE_SCHEMA:
                connection.execute(statement)

    def launch(
        self,
        key: str,
        role: str,
        done: str,
        cwd: Path | None = None,
        host: str = "local",
        worktrees: Sequence[Path] = (),
    ) -> Lane:
        """Register one lane and persist its exact session UUID.

        This records lane identity only; it never spawns a process. The lane
        row, its first history revision and its ``sessions`` row are written in
        one transaction, so the lane never exists without its session.
        """
        self._validate_key(key)
        role = role.strip()
        done = done.strip()
        host = host.strip()
        if not role:
            raise ValueError("lane role must not be empty")
        if not done:
            raise ValueError("lane Done criteria must not be empty")
        if not host:
            raise ValueError("lane host must not be empty")
        cwd_path = Path(cwd) if cwd is not None else self._config.work_dir / key
        trees = tuple(Path(item) for item in worktrees)
        session_id = str(uuid.uuid4())
        now = time.time()
        with self._store.connection() as connection:
            existing: dict[str, object] | None = connection.execute(
                "SELECT key FROM lanes WHERE key = ?", (key,)
            ).fetchone()
            if existing is not None:
                raise RegistryError(f"lane {key!r} is already registered")
            lane = Lane(
                key=key,
                role=role,
                cwd=cwd_path,
                host=host,
                session_id=session_id,
                session_file=None,
                done=done,
                worktrees=trees,
                status="working",
                revision=1,
            )
            self._write_lane(connection, lane)
            _insert_history(connection, key, 1, now, "working", "launched", "launch")
            return lane

    def get(self, key: str) -> Lane:
        """Return one lane or raise :class:`RegistryError` when absent."""
        with self._store.connection() as connection:
            return self._require(connection, key)

    def list(self, open_only: bool = False) -> list[Lane]:
        """Return every lane in registration order, optionally excluding closed."""
        with self._store.connection() as connection:
            rows = connection.execute("SELECT * FROM lanes ORDER BY rowid").fetchall()
        lanes = [_row_to_lane(row) for row in rows]
        if open_only:
            return [lane for lane in lanes if lane.status != "closed"]
        return lanes

    def history(self, key: str) -> tuple[LaneEvent, ...]:
        """Return the immutable status history for ``key`` in revision order."""
        with self._store.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM lane_history WHERE key = ? ORDER BY revision", (key,)
            ).fetchall()
        return tuple(_row_to_event(row) for row in rows)

    def adopt(self, key: str, session_file: Path) -> Lane:
        """Bind ``key`` to an authoritative session file and its header id.

        The file must exist and carry a valid JSONL session header; its id must
        not be bound to another lane. This is an explicit operator action, so it
        never guesses a file and refuses only a closed lane or an id conflict.
        """
        path = Path(session_file).expanduser().resolve()
        header = _read_header(path)
        with self._store.connection() as connection:
            lane = self._require(connection, key)
            if lane.status == "closed":
                raise RegistryError(f"lane {key!r} is closed")
            if connection.execute(
                "SELECT id FROM jobs WHERE lane = ? "
                "AND status IN ('running', 'uncertain') LIMIT 1", (key,)
            ).fetchone() is not None:
                raise RegistryError("lane execution must be reconciled before adoption")
            other: dict[str, object] | None = connection.execute(
                "SELECT key FROM lanes WHERE session_id = ? AND key <> ?", (header, key)
            ).fetchone()
            if other is not None:
                raise RegistryError(
                    f"session {header!r} is already bound to lane {_text(other.get('key'))!r}"
                )
            updated = replace(
                lane, session_id=header, session_file=path, revision=lane.revision + 1
            )
            self._write_lane(connection, updated)
            _insert_history(
                connection,
                key,
                updated.revision,
                time.time(),
                updated.status,
                f"adopted {path}",
                "adopt",
            )
            return updated

    def rotate(self, key: str, session_id: str, now: float, note: str) -> Lane:
        """Install a fresh session after a durable handoff and a reaped turn."""
        _finite(now)
        if not session_id.strip():
            raise ValueError("rotation requires a session id")
        with self._store.connection() as connection:
            lane = self._require(connection, key)
            if lane.status == "closed" or session_id == lane.session_id:
                raise RegistryError("rotation requires an open lane and a fresh session")
            if connection.execute(
                "SELECT id FROM jobs WHERE lane = ? AND status = 'running' LIMIT 1",
                (key,),
            ).fetchone() is not None:
                raise RegistryError("running execution must stop before rotation")
            if connection.execute(
                "SELECT key FROM lanes WHERE session_id = ? AND key <> ?",
                (session_id, key),
            ).fetchone() is not None:
                raise RegistryError("session is already owned by another lane")
            updated = replace(
                lane, session_id=session_id, session_file=None, status="working",
                claim_at=None, deadline=None, claim_emitted=False, evidence="",
                revision=lane.revision + 1,
            )
            self._write_lane(connection, updated)
            _insert_history(
                connection, key, updated.revision, now, "working", note, "handoff"
            )
            return updated

    def claim(self, key: str, evidence: str, now: float) -> Lane:
        """Open a fixed 300-second objection window on a completion claim.

        The deadline is exactly ``now + 300``. Repeating a claim while the same
        window is still open returns the lane unchanged and never extends it.
        """
        _finite(now)
        with self._store.connection() as connection:
            lane = self._require(connection, key)
            if lane.status == "closed":
                raise RegistryError(f"lane {key!r} is closed")
            if (
                lane.claim_at is not None
                and lane.deadline is not None
                and now < lane.deadline
            ):
                return lane
            updated = replace(
                lane,
                status="done-claimed",
                claim_at=now,
                deadline=now + CLAIM_SECONDS,
                evidence=evidence.strip(),
                claim_emitted=False,
                revision=lane.revision + 1,
            )
            self._write_lane(connection, updated)
            _insert_history(
                connection,
                key,
                updated.revision,
                now,
                "done-claimed",
                evidence.strip() or "claimed done",
                "claim",
            )
            return updated

    def object(self, key: str, reasons: Sequence[str], now: float) -> Lane:
        """Object to a claim: record ``not-done`` and clear the active claim."""
        _finite(now)
        note = "; ".join(reason.strip() for reason in reasons if reason.strip())
        if not note:
            raise ValueError("objection requires at least one reason")
        with self._store.connection() as connection:
            lane = self._require(connection, key)
            if lane.status == "closed":
                raise RegistryError(f"lane {key!r} is closed")
            updated = replace(
                lane,
                status="not-done",
                claim_at=None,
                deadline=None,
                evidence="",
                claim_emitted=False,
                revision=lane.revision + 1,
            )
            self._write_lane(connection, updated)
            _insert_history(
                connection, key, updated.revision, now, "not-done", note, "object"
            )
            return updated

    def transition(
        self,
        key: str,
        status: LaneStatus,
        note: str,
        now: float,
        expected_revision: int | None = None,
    ) -> Lane:
        """Move a lane to a new status under compare-and-swap protection.

        ``closed`` is terminal: a closed lane refuses every further change. When
        ``expected_revision`` is supplied it must match the current revision, so
        a checker holding a stale read cannot close a lane that a newer
        objection or re-claim has already changed.
        """
        _finite(now)
        if status not in _LANE_STATUSES:
            raise ValueError(f"unknown lane status: {status!r}")
        if status == "done-claimed":
            raise RegistryError("use claim() to open an objection window")
        with self._store.connection() as connection:
            lane = self._require(connection, key)
            if lane.status == "closed":
                raise RegistryError(f"lane {key!r} is closed (terminal)")
            if expected_revision is not None and expected_revision != lane.revision:
                raise RegistryError(
                    f"stale revision for lane {key!r}: expected {expected_revision}, "
                    f"current {lane.revision}"
                )
            updated = replace(
                lane,
                status=status,
                claim_at=None,
                deadline=None,
                claim_emitted=False,
                revision=lane.revision + 1,
            )
            self._write_lane(connection, updated)
            _insert_history(
                connection,
                key,
                updated.revision,
                now,
                status,
                note.strip(),
                "transition",
            )
            return updated

    def settle(self, key: str, revision: int, now: float, note: str) -> Lane:
        """Atomically close a checked claim, excluding late work and objections."""
        _finite(now)
        with self._store.connection() as connection:
            lane = self._require(connection, key)
            if (
                lane.revision != revision or lane.status != "done-claimed"
                or lane.deadline is None or now < lane.deadline
            ):
                raise RegistryError("claim changed or its objection window is open")
            if connection.execute(
                "SELECT id FROM jobs WHERE lane = ? AND status IN "
                "('awaiting_approval', 'queued', 'running', 'uncertain') LIMIT 1",
                (key,),
            ).fetchone() is not None:
                raise RegistryError("unfinished work prevents closure")
            verified = replace(
                lane, status="verified-done", claim_at=None, deadline=None,
                claim_emitted=False, revision=lane.revision + 1,
            )
            self._write_lane(connection, verified)
            _insert_history(
                connection, key, verified.revision, now, "verified-done", note, "verification"
            )
            closed = replace(verified, status="closed", revision=verified.revision + 1)
            self._write_lane(connection, closed)
            _insert_history(
                connection, key, closed.revision, now, "closed", note, "verification"
            )
            return closed

    def mark_claim_emitted(self, key: str) -> Lane:
        """Record that the one allowed claim notification has been sent."""
        with self._store.connection() as connection:
            lane = self._require(connection, key)
            if lane.status != "done-claimed":
                raise RegistryError("lane has no active claim")
            if lane.claim_emitted:
                return lane
            updated = replace(lane, claim_emitted=True, revision=lane.revision + 1)
            self._write_lane(connection, updated)
            _insert_history(
                connection,
                key,
                updated.revision,
                time.time(),
                updated.status,
                "claim notification emitted",
                "claim-emitted",
            )
            return updated

    def sync(self, write: bool = False) -> SyncReport:
        """Scan authoritative session files and report drift.

        Only the configured sessions directory and each lane's explicitly
        adopted file are read; no environment or process heuristic is used and
        no unregistered file is ever adopted on a guess. With ``write=True`` an
        unambiguous authoritative header may be applied to its own lane, but
        never while that lane has running or uncertain execution.
        """
        jobs = self._store.list_jobs()
        lanes = self.list()
        sessions_dir = self._config.sessions_dir
        scanned = tuple(sorted(set(_scan(sessions_dir)) | {
            lane.session_file for lane in lanes if lane.session_file is not None
        }))
        headers: dict[Path, str | None] = {path: _read_header_or_none(path) for path in scanned}
        counts: dict[str, int] = {}
        for session_id in headers.values():
            if session_id is not None:
                counts[session_id] = counts.get(session_id, 0) + 1
        lane_ids = {lane.session_id for lane in lanes}

        findings: list[SyncFinding] = []
        repairs: list[SyncRepair] = []
        registered: set[Path] = set()

        for lane in lanes:
            if not lane.done.strip():
                findings.append(
                    SyncFinding("missing-done", lane.key, None, "lane has no Done criteria")
                )
            matches = [path for path, value in headers.items() if value == lane.session_id]
            if lane.session_file is None and len(matches) > 1:
                findings.append(SyncFinding(
                    "changed-header", lane.key, None, "multiple files claim this session id"
                ))
                continue
            resolved = lane.session_file or (
                matches[0] if matches else sessions_dir / f"{lane.session_id}.jsonl"
            )
            explicit = lane.session_file is not None
            registered.add(resolved)
            if not resolved.exists():
                executed = any(
                    job.lane == lane.key and job.session_id is not None for job in jobs
                )
                if explicit or executed or lane.status != "working":
                    findings.append(
                        SyncFinding(
                            "missing", lane.key, resolved, "authoritative session file is missing"
                        )
                    )
                else:
                    findings.append(
                        SyncFinding(
                            "pending",
                            lane.key,
                            resolved,
                            "lane has not executed yet; no session file",
                        )
                    )
                continue
            session_id = headers.get(resolved)
            if session_id is None:
                session_id = _read_header_or_none(resolved)
            if session_id is None:
                findings.append(
                    SyncFinding(
                        "changed-header",
                        lane.key,
                        resolved,
                        "session header is missing or unreadable",
                    )
                )
                continue
            if session_id != lane.session_id:
                findings.append(
                    SyncFinding(
                        "changed-header",
                        lane.key,
                        resolved,
                        f"header id {session_id!r} differs from lane session {lane.session_id!r}",
                    )
                )
            if write and (session_id != lane.session_id or lane.session_file is None):
                repair = self._repair(lane, resolved, session_id, counts, lane_ids, jobs)
                if repair is not None:
                    repairs.append(repair)
                    lane_ids.discard(lane.session_id)
                    lane_ids.add(repair.session_id)

        for path in scanned:
            if path in registered:
                continue
            session_id = headers.get(path)
            if session_id is not None and session_id in lane_ids:
                continue
            findings.append(
                SyncFinding(
                    "unregistered-file",
                    None,
                    path,
                    "session file is not registered to any lane",
                )
            )

        return SyncReport(
            scanned=scanned,
            findings=tuple(findings),
            repairs=tuple(repairs),
            written=write,
        )

    def _repair(
        self,
        lane: Lane,
        path: Path,
        session_id: str,
        counts: Mapping[str, int],
        lane_ids: set[str],
        jobs: Sequence[Job],
    ) -> SyncRepair | None:
        if lane.session_file is not None and Path(lane.session_file) != path:
            return None
        if counts.get(session_id, 0) != 1:
            return None
        if session_id in lane_ids and session_id != lane.session_id:
            return None
        if any(
            job.lane == lane.key and job.status in _ACTIVE_JOB_STATUSES
            for job in jobs
        ):
            return None
        now = time.time()
        with self._store.connection() as connection:
            current = self._require(connection, lane.key)
            if current.revision != lane.revision:
                return None
            if connection.execute(
                "SELECT id FROM jobs WHERE lane = ? "
                "AND status IN ('running', 'uncertain') LIMIT 1", (lane.key,),
            ).fetchone() is not None:
                return None
            updated = replace(
                current, session_id=session_id, session_file=path,
                revision=current.revision + 1,
            )
            self._write_lane(connection, updated)
            _insert_history(
                connection,
                lane.key,
                updated.revision,
                now,
                updated.status,
                f"sync repaired session header to {session_id}",
                "sync",
            )
        return SyncRepair(
            key=lane.key,
            revision=updated.revision,
            source="sync",
            session_id=session_id,
        )

    @staticmethod
    def _validate_key(key: str) -> None:
        if not LANE_KEY_PATTERN.match(key):
            raise ValueError(f"invalid lane key: {key!r}")

    def _require(self, connection: sqlite3.Connection, key: str) -> Lane:
        row: dict[str, object] | None = connection.execute(
            "SELECT * FROM lanes WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            raise RegistryError(f"unknown lane: {key!r}")
        return _row_to_lane(row)

    @staticmethod
    def _write_lane(connection: sqlite3.Connection, lane: Lane) -> None:
        connection.execute(
            "INSERT INTO sessions (lane, session_id) VALUES (?, ?) "
            "ON CONFLICT(lane) DO UPDATE SET session_id = excluded.session_id",
            (lane.key, lane.session_id),
        )
        connection.execute(
            """
            INSERT INTO lanes (
                key, role, cwd, host, session_id, session_file, done, worktrees,
                status, revision, claim_at, deadline, evidence, claim_emitted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                role = excluded.role,
                cwd = excluded.cwd,
                host = excluded.host,
                session_id = excluded.session_id,
                session_file = excluded.session_file,
                done = excluded.done,
                worktrees = excluded.worktrees,
                status = excluded.status,
                revision = excluded.revision,
                claim_at = excluded.claim_at,
                deadline = excluded.deadline,
                evidence = excluded.evidence,
                claim_emitted = excluded.claim_emitted
            """,
            (
                lane.key,
                lane.role,
                str(lane.cwd),
                lane.host,
                lane.session_id,
                str(lane.session_file) if lane.session_file is not None else None,
                lane.done,
                json.dumps([str(item) for item in lane.worktrees]),
                lane.status,
                lane.revision,
                lane.claim_at,
                lane.deadline,
                lane.evidence,
                int(lane.claim_emitted),
            ),
        )


def _scan(directory: Path) -> tuple[Path, ...]:
    if not directory.is_dir():
        return ()
    return tuple(sorted(directory.glob("*.jsonl")))


__all__ = [
    "CLAIM_SECONDS",
    "LANE_KEY_PATTERN",
    "Lane",
    "LaneEvent",
    "LaneStatus",
    "Registry",
    "RegistryError",
    "SyncFinding",
    "SyncKind",
    "SyncRepair",
    "SyncReport",
]
