"""Durable SQLite state for jobs, approvals, the outbox and cursors.

This module owns the crash-honest core of ultra-agent: immutable job input,
durable approval state, one transactional claim, explicit unresolved-outcome
recovery, deterministic reply chunking and a small outbox state machine. Every
operation uses its own SQLite connection inside an explicit transaction, so a
process that dies between ticks leaves a state the next process can read and
reconcile rather than silently replay.

Exactly-once external effects are not promised. A job found in ``running`` or a
delivery found in ``sending`` after a restart is reported as ``uncertain`` and
never retried automatically; only an explicit operator action resolves it.

Other modules (registry, relay) share this database through ``connection()``
instead of opening a second engine, so their inserts join the same transaction
boundary.

Typed parsing is deliberate: rows are read as ``dict[str, object]`` and every
field is narrowed before it enters a frozen model, and a state change that does
not affect exactly one expected row raises ``StoreError`` instead of passing as
a silent no-op.
"""

from __future__ import annotations

import math
import sqlite3
import time
import uuid
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import TypeAlias

from ultra_agent.models import (
    DEFAULT_LANE,
    Delivery,
    DeliveryStatus,
    Job,
    JobStatus,
    Platform,
    Submission,
)

BUSY_TIMEOUT_MS = 5000
"""Milliseconds SQLite waits on a locked database before failing."""

CHUNK_SIZE = 1900
"""Maximum characters in one outbound reply chunk."""

_JOB_STATUSES: tuple[JobStatus, ...] = (
    "awaiting_approval",
    "queued",
    "running",
    "completed",
    "failed",
    "uncertain",
)

_DELIVERY_STATUSES: tuple[DeliveryStatus, ...] = (
    "pending",
    "sending",
    "sent",
    "uncertain",
    "resolved",
)

_FINISH_JOB_STATUSES: tuple[JobStatus, ...] = ("completed", "failed", "uncertain")

_RETRYABLE_JOB_STATUSES: tuple[JobStatus, ...] = ("completed", "failed", "uncertain")

_Row: TypeAlias = dict[str, object]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    source_key TEXT NOT NULL UNIQUE,
    prompt TEXT NOT NULL,
    approved INTEGER NOT NULL DEFAULT 0,
    channel_id TEXT,
    reply_to TEXT,
    lane TEXT NOT NULL DEFAULT 'default',
    platform TEXT,
    heavy INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    result TEXT,
    error TEXT,
    session_id TEXT,
    retry_of TEXT,
    verified INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    platform TEXT,
    channel_id TEXT NOT NULL,
    content TEXT NOT NULL,
    reply_to TEXT,
    job_id TEXT,
    source_key TEXT,
    status TEXT NOT NULL,
    remote_id TEXT,
    ordinal INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS deliveries_source_key
    ON deliveries(source_key) WHERE source_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS checkpoints (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    lane TEXT PRIMARY KEY,
    session_id TEXT NOT NULL
);
"""


class StoreError(RuntimeError):
    """A durable state change was refused: wrong state, unknown id or conflict."""


def _dict_row(cursor: sqlite3.Cursor, row: tuple[object, ...]) -> _Row:
    """Row factory that yields plain ``dict[str, object]`` rows."""
    description = cursor.description
    if description is None:
        return {}
    return {column[0]: value for column, value in zip(description, row, strict=False)}


def _as_str(value: object) -> str:
    return value if isinstance(value, str) else ""


def _as_opt_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_bool(value: object) -> bool:
    return bool(value)


def _as_int(value: object) -> int:
    return value if isinstance(value, int) else 0


def _job_status(value: object) -> JobStatus:
    if isinstance(value, str):
        for status in _JOB_STATUSES:
            if value == status:
                return status
    raise StoreError(f"unknown job status stored in database: {value!r}")


def _delivery_status(value: object) -> DeliveryStatus:
    if isinstance(value, str):
        for status in _DELIVERY_STATUSES:
            if value == status:
                return status
    raise StoreError(f"unknown delivery status stored in database: {value!r}")


def _platform(value: object) -> Platform | None:
    if value in ("discord", "telegram", "slack"):
        return value
    return None


def chunks(text: str, size: int = CHUNK_SIZE) -> list[str]:
    """Split without breaking code points, bounded in UTF-16 wire units."""
    if size < 1:
        raise ValueError("chunk size must be positive")
    if not text:
        return []
    parts: list[str] = []
    start = 0
    used = 0
    for index, character in enumerate(text):
        units = 2 if ord(character) > 0xFFFF else 1
        if units > size:
            raise ValueError("chunk size cannot hold a Unicode code point")
        if used + units > size:
            parts.append(text[start:index])
            start = index
            used = 0
        used += units
    parts.append(text[start:])
    return parts


class Store:
    """Crash-honest durable state backed by one SQLite database file."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        if self.path.parent != Path(""):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        try:
            connection.executescript(_SCHEMA)
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path, isolation_level=None, timeout=BUSY_TIMEOUT_MS / 1000
        )
        connection.row_factory = _dict_row
        connection.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection]:
        """Yield a connection inside one immediate write transaction.

        The transaction commits when the block returns and rolls back when it
        raises, so composite writes (for example a cursor plus its inbox rows)
        are atomic. The connection closes in every case.
        """
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            connection.execute("COMMIT")
        finally:
            connection.close()

    @contextmanager
    def _read(self) -> Generator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _one(
        connection: sqlite3.Connection, sql: str, params: Sequence[object] = ()
    ) -> _Row | None:
        raw = connection.execute(sql, params).fetchone()
        if raw is None:
            return None
        row: _Row = raw
        return row

    @staticmethod
    def _all(
        connection: sqlite3.Connection, sql: str, params: Sequence[object] = ()
    ) -> list[_Row]:
        rows: list[_Row] = []
        for raw in connection.execute(sql, params).fetchall():
            row: _Row = raw
            rows.append(row)
        return rows

    @staticmethod
    def _insert_job(
        connection: sqlite3.Connection,
        job_id: str,
        submission: Submission,
        status: JobStatus,
        retry_of: str | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO jobs (
                id, source_key, prompt, approved, channel_id, reply_to,
                lane, platform, heavy, status, retry_of
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                submission.source_key,
                submission.prompt,
                int(submission.approved),
                submission.channel_id,
                submission.reply_to,
                submission.lane,
                submission.platform,
                int(submission.heavy),
                status,
                retry_of,
            ),
        )

    def _next_ordinal(self, connection: sqlite3.Connection, job_id: str | None) -> int:
        if job_id is None:
            return 0
        row = self._one(
            connection,
            "SELECT COALESCE(MAX(ordinal), -1) + 1 AS n FROM deliveries WHERE job_id = ?",
            (job_id,),
        )
        return _as_int(row.get("n")) if row is not None else 0

    @staticmethod
    def _row_to_job(row: Mapping[str, object]) -> Job:
        return Job(
            id=_as_str(row.get("id")),
            source_key=_as_str(row.get("source_key")),
            prompt=_as_str(row.get("prompt")),
            approved=_as_bool(row.get("approved")),
            channel_id=_as_opt_str(row.get("channel_id")),
            reply_to=_as_opt_str(row.get("reply_to")),
            lane=_as_str(row.get("lane")) or DEFAULT_LANE,
            platform=_platform(row.get("platform")),
            heavy=_as_bool(row.get("heavy")),
            status=_job_status(row.get("status")),
            result=_as_opt_str(row.get("result")),
            error=_as_opt_str(row.get("error")),
            session_id=_as_opt_str(row.get("session_id")),
            retry_of=_as_opt_str(row.get("retry_of")),
            verified=_as_bool(row.get("verified")),
        )

    @staticmethod
    def _row_to_delivery(row: Mapping[str, object]) -> Delivery:
        return Delivery(
            id=_as_str(row.get("id")),
            channel_id=_as_str(row.get("channel_id")),
            content=_as_str(row.get("content")),
            job_id=_as_opt_str(row.get("job_id")),
            reply_to=_as_opt_str(row.get("reply_to")),
            status=_delivery_status(row.get("status")),
            remote_id=_as_opt_str(row.get("remote_id")),
            ordinal=_as_int(row.get("ordinal")),
            platform=_platform(row.get("platform")),
        )

    def enqueue(self, submission: Submission) -> str:
        """Insert one immutable job, or return the id of its existing twin.

        A repeated ``source_key`` with the same prompt returns the original
        job id. The same key with a different prompt is refused: job input is
        immutable once recorded.
        """
        with self.connection() as connection:
            return self._enqueue(connection, submission)

    def enqueue_batch(
        self, submissions: Sequence[Submission], checkpoint: tuple[str, str] | None
    ) -> list[str]:
        """Insert many jobs and advance one cursor in a single transaction.

        Returns the durable id for each submission in input order (the
        existing id when a source key already exists). If any submission
        conflicts, the whole batch, including the cursor, rolls back.
        """
        with self.connection() as connection:
            job_ids = [self._enqueue(connection, item) for item in submissions]
            if checkpoint is not None:
                self._write_checkpoint(connection, checkpoint[0], checkpoint[1])
            return job_ids

    def _enqueue(self, connection: sqlite3.Connection, submission: Submission) -> str:
        if not submission.source_key.strip():
            raise ValueError("submission requires a non-empty source_key")
        existing = self._one(
            connection,
            "SELECT * FROM jobs WHERE source_key = ?",
            (submission.source_key,),
        )
        if existing is not None:
            original = self._row_to_job(existing)
            if (
                original.prompt != submission.prompt
                or original.channel_id != submission.channel_id
                or original.reply_to != submission.reply_to
                or original.lane != submission.lane
                or original.platform != submission.platform
                or original.heavy != submission.heavy
            ):
                raise StoreError(
                    f"source_key {submission.source_key!r} is bound to a different prompt"
                )
            return _as_str(existing.get("id"))
        job_id = str(uuid.uuid4())
        status: JobStatus = "queued" if submission.approved else "awaiting_approval"
        self._insert_job(connection, job_id, submission, status, None)
        return job_id

    def get_job(self, job_id: str) -> Job:
        """Return one job by id, or raise ``StoreError`` when it is absent."""
        with self._read() as connection:
            row = self._one(connection, "SELECT * FROM jobs WHERE id = ?", (job_id,))
        if row is None:
            raise StoreError(f"unknown job: {job_id!r}")
        return self._row_to_job(row)

    def list_jobs(self) -> list[Job]:
        """Return every job in insertion order."""
        with self._read() as connection:
            rows = self._all(connection, "SELECT * FROM jobs ORDER BY rowid")
        return [self._row_to_job(row) for row in rows]

    def approve(self, job_id: str) -> None:
        """Authorize one job: ``awaiting_approval -> queued`` only."""
        with self.connection() as connection:
            cursor = connection.execute(
                "UPDATE jobs SET status = 'queued', approved = 1 "
                "WHERE id = ? AND status = 'awaiting_approval'",
                (job_id,),
            )
            if cursor.rowcount != 1:
                raise StoreError(f"job {job_id!r} is not awaiting approval")

    def claim(self) -> Job | None:
        """Reserve the oldest queued job by moving it to ``running``.

        Returns ``None`` when nothing is queued or when any job is ``running``
        or ``uncertain``: an unresolved execution blocks new execution until the
        operator reconciles it.
        """
        with self.connection() as connection:
            blocked = self._one(
                connection,
                "SELECT COUNT(*) AS n FROM jobs WHERE status IN ('running', 'uncertain')",
            )
            if blocked is not None and _as_int(blocked.get("n")) > 0:
                return None
            row = self._one(
                connection,
                "SELECT * FROM jobs WHERE status = 'queued' ORDER BY rowid LIMIT 1",
            )
            if row is None:
                return None
            job_id = _as_str(row.get("id"))
            lane = _as_str(row.get("lane"))
            session = self._one(
                connection, "SELECT session_id FROM sessions WHERE lane = ?", (lane,)
            )
            session_id = (
                _as_str(session.get("session_id")) if session else str(uuid.uuid4())
            )
            if session is None:
                connection.execute(
                    "INSERT INTO sessions (lane, session_id) VALUES (?, ?)",
                    (lane, session_id),
                )
            cursor = connection.execute(
                "UPDATE jobs SET status = 'running', session_id = ? "
                "WHERE id = ? AND status = 'queued'",
                (session_id, job_id),
            )
            if cursor.rowcount != 1:
                raise StoreError(f"job {job_id!r} could not be claimed")
            claimed = dict(row)
            claimed["status"] = "running"
            claimed["session_id"] = session_id
            return self._row_to_job(claimed)

    def finish(
        self,
        job_id: str,
        status: JobStatus,
        result: str = "",
        error: str = "",
        verified: bool = False,
    ) -> None:
        """Close one running job and insert its reply chunks atomically.

        Only a ``running`` job can finish, and only into ``completed``,
        ``failed`` or ``uncertain``. A completed job with a non-empty channel
        and non-empty result gets deterministic 1900-character pending chunks;
        a job with no channel produces no delivery. Both writes share one
        transaction.
        """
        if status not in _FINISH_JOB_STATUSES:
            raise ValueError(f"finish status must be one of {_FINISH_JOB_STATUSES}")
        with self.connection() as connection:
            cursor = connection.execute(
                "UPDATE jobs SET status = ?, result = ?, error = ?, verified = ? "
                "WHERE id = ? AND status = 'running'",
                (status, result or None, error or None, int(verified), job_id),
            )
            if cursor.rowcount != 1:
                raise StoreError(f"job {job_id!r} is not running")
            if status == "completed" and result:
                row = self._one(
                    connection,
                    "SELECT channel_id, platform, reply_to FROM jobs WHERE id = ?",
                    (job_id,),
                )
                channel_id = _as_opt_str(row.get("channel_id")) if row is not None else None
                platform = _platform(row.get("platform")) if row is not None else None
                if channel_id:
                    ordinal = self._next_ordinal(connection, job_id)
                    for index, chunk in enumerate(chunks(result)):
                        self._insert_delivery(
                            connection,
                            platform=platform,
                            channel_id=channel_id,
                            content=chunk,
                            reply_to=_as_opt_str(row.get("reply_to")) if row else None,
                            job_id=job_id,
                            source_key=None,
                            status="pending",
                            ordinal=ordinal + index,
                        )

    def recover_interrupted(self) -> int:
        """Mark interrupted work uncertain and return how many rows changed.

        A ``running`` job and a ``sending`` delivery both mean the process died
        with an outcome the next process cannot know. Neither is retried: they
        become ``uncertain`` until an operator resolves them.
        """
        with self.connection() as connection:
            jobs = connection.execute(
                "UPDATE jobs SET status = 'uncertain' WHERE status = 'running'"
            ).rowcount
            deliveries = connection.execute(
                "UPDATE deliveries SET status = 'uncertain' WHERE status = 'sending'"
            ).rowcount
            return max(jobs, 0) + max(deliveries, 0)

    def resolve(self, target_id: str) -> None:
        """Reconcile one uncertain record; anything else is refused.

        An uncertain job becomes ``failed`` (its effects are not replayed) and
        an uncertain delivery becomes ``resolved``, not falsely ``sent``.
        Resolution is explicit because
        the store cannot prove whether the lost action happened.
        """
        with self.connection() as connection:
            jobs = connection.execute(
                "UPDATE jobs SET status = 'failed' WHERE id = ? AND status = 'uncertain'",
                (target_id,),
            )
            if jobs.rowcount == 1:
                return
            deliveries = connection.execute(
                "UPDATE deliveries SET status = 'resolved' "
                "WHERE id = ? AND status = 'uncertain'",
                (target_id,),
            )
            if deliveries.rowcount == 1:
                return
        raise StoreError(f"no uncertain job or delivery to resolve: {target_id!r}")

    def retry(self, job_id: str) -> str:
        """Create a new linked job awaiting approval; never rerun the original."""
        with self.connection() as connection:
            row = self._one(connection, "SELECT * FROM jobs WHERE id = ?", (job_id,))
            if row is None:
                raise StoreError(f"unknown job: {job_id!r}")
            if _job_status(row.get("status")) not in _RETRYABLE_JOB_STATUSES:
                raise StoreError(f"job {job_id!r} is not in a retryable state")
            new_id = str(uuid.uuid4())
            submission = Submission(
                source_key=f"{_as_str(row.get('source_key'))}#retry:{new_id}",
                prompt=_as_str(row.get("prompt")),
                approved=False,
                channel_id=_as_opt_str(row.get("channel_id")),
                reply_to=_as_opt_str(row.get("reply_to")),
                lane=_as_str(row.get("lane")) or DEFAULT_LANE,
                platform=_platform(row.get("platform")),
                heavy=_as_bool(row.get("heavy")),
            )
            self._insert_job(connection, new_id, submission, "awaiting_approval", job_id)
            return new_id

    @staticmethod
    def _insert_delivery(
        connection: sqlite3.Connection,
        *,
        platform: Platform | None,
        channel_id: str,
        content: str,
        reply_to: str | None,
        job_id: str | None,
        source_key: str | None,
        status: DeliveryStatus,
        ordinal: int,
        remote_id: str | None = None,
    ) -> str:
        delivery_id = str(uuid.uuid4())
        connection.execute(
            """
            INSERT INTO deliveries (
                id, platform, channel_id, content, reply_to, job_id,
                source_key, status, remote_id, ordinal
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                delivery_id,
                platform,
                channel_id,
                content,
                reply_to,
                job_id,
                source_key,
                status,
                remote_id,
                ordinal,
            ),
        )
        return delivery_id

    def add_delivery(
        self,
        platform: Platform | None,
        channel: str,
        content: str,
        reply_to: str | None = None,
        job_id: str | None = None,
        source_key: str | None = None,
    ) -> str:
        """Append one pending outbound chunk, for the relay or an adapter.

        When ``source_key`` is supplied a repeat returns the existing delivery
        id, so a resent event does not duplicate output. Chunks attached to a
        job carry the next ordinal and are therefore ordered.
        """
        if not channel.strip():
            raise ValueError("delivery requires a non-empty channel")
        with self.connection() as connection:
            if source_key is not None:
                existing = self._one(
                    connection,
                    "SELECT id FROM deliveries WHERE source_key = ?",
                    (source_key,),
                )
                if existing is not None:
                    return _as_str(existing.get("id"))
            return self._insert_delivery(
                connection,
                platform=platform,
                channel_id=channel,
                content=content,
                reply_to=reply_to,
                job_id=job_id,
                source_key=source_key,
                status="pending",
                ordinal=self._next_ordinal(connection, job_id),
            )

    def deliveries(self) -> list[Delivery]:
        """Return every delivery in insertion order."""
        with self._read() as connection:
            rows = self._all(connection, "SELECT * FROM deliveries ORDER BY rowid")
        return [self._row_to_delivery(row) for row in rows]

    def start_delivery(self, delivery_id: str, now: float | None = None) -> bool:
        """Commit ``pending -> sending`` before one network POST.

        Returns ``False`` without changing anything when an earlier chunk of
        the same job is not yet ``sent``; an unresolved or unsent earlier chunk
        blocks all later chunks. Any other state is a refused transition.
        """
        with self.connection() as connection:
            row = self._one(
                connection,
                "SELECT * FROM deliveries WHERE id = ?",
                (delivery_id,),
            )
            if row is None:
                raise StoreError(f"unknown delivery: {delivery_id!r}")
            if _delivery_status(row.get("status")) != "pending":
                raise StoreError(f"delivery {delivery_id!r} is not pending")
            retry = self._one(
                connection,
                "SELECT value FROM checkpoints WHERE key = ?",
                (f"delivery-retry:{delivery_id}",),
            )
            current = time.time() if now is None else now
            if retry is not None and float(_as_str(retry.get("value"))) > current:
                return False
            job_id = _as_opt_str(row.get("job_id"))
            if job_id is not None:
                earlier = self._one(
                    connection,
                    "SELECT id FROM deliveries WHERE job_id = ? AND ordinal < ? "
                    "AND status NOT IN ('sent', 'resolved') LIMIT 1",
                    (job_id, _as_int(row.get("ordinal"))),
                )
                if earlier is not None:
                    return False
            cursor = connection.execute(
                "UPDATE deliveries SET status = 'sending' WHERE id = ? AND status = 'pending'",
                (delivery_id,),
            )
            if cursor.rowcount != 1:
                raise StoreError(f"delivery {delivery_id!r} could not be started")
            return True

    def defer_delivery(self, delivery_id: str, retry_at: float) -> None:
        """Reschedule only after a definite pre-effect rejection such as 429."""
        if not math.isfinite(retry_at) or retry_at < 0:
            raise ValueError("retry time must be finite and nonnegative")
        with self.connection() as connection:
            updated = connection.execute(
                "UPDATE deliveries SET status = 'pending' "
                "WHERE id = ? AND status = 'sending'",
                (delivery_id,),
            )
            if updated.rowcount != 1:
                raise StoreError("delivery is not sending")
            self._write_checkpoint(
                connection, f"delivery-retry:{delivery_id}", str(retry_at)
            )

    def finish_delivery(
        self, delivery_id: str, status: DeliveryStatus, remote_id: str | None = None
    ) -> None:
        """Close one sending delivery as ``sent`` or ``uncertain``."""
        if status not in ("sent", "uncertain"):
            raise ValueError("delivery finish status must be 'sent' or 'uncertain'")
        with self.connection() as connection:
            cursor = connection.execute(
                "UPDATE deliveries SET status = ?, remote_id = COALESCE(?, remote_id) "
                "WHERE id = ? AND status = 'sending'",
                (status, remote_id, delivery_id),
            )
            if cursor.rowcount != 1:
                raise StoreError(f"delivery {delivery_id!r} is not sending")

    def resend(self, delivery_id: str) -> str:
        """Queue a fresh copy of one delivery; the job is never re-executed."""
        with self.connection() as connection:
            row = self._one(
                connection, "SELECT * FROM deliveries WHERE id = ?", (delivery_id,)
            )
            if row is None:
                raise StoreError(f"unknown delivery: {delivery_id!r}")
            job_id = _as_opt_str(row.get("job_id"))
            ordinal = (
                self._next_ordinal(connection, job_id) if job_id is not None else 0
            )
            if _delivery_status(row.get("status")) == "uncertain":
                connection.execute(
                    "UPDATE deliveries SET status = 'resolved' WHERE id = ?",
                    (delivery_id,),
                )
                ordinal = _as_int(row.get("ordinal"))
            return self._insert_delivery(
                connection,
                platform=_platform(row.get("platform")),
                channel_id=_as_str(row.get("channel_id")),
                content=_as_str(row.get("content")),
                reply_to=_as_opt_str(row.get("reply_to")),
                job_id=job_id,
                source_key=None,
                status="pending",
                ordinal=ordinal,
            )

    def get_checkpoint(self, key: str) -> str | None:
        """Return the durable cursor value for ``key``, or ``None``."""
        with self._read() as connection:
            row = self._one(
                connection, "SELECT value FROM checkpoints WHERE key = ?", (key,)
            )
        return _as_opt_str(row.get("value")) if row is not None else None

    def set_checkpoint(self, key: str, value: str) -> None:
        """Persist a durable cursor value."""
        with self.connection() as connection:
            self._write_checkpoint(connection, key, value)

    @staticmethod
    def _write_checkpoint(connection: sqlite3.Connection, key: str, value: str) -> None:
        connection.execute(
            "INSERT INTO checkpoints (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def get_session(self, lane: str = DEFAULT_LANE) -> str | None:
        """Return the persisted session id for ``lane``, or ``None``."""
        with self._read() as connection:
            row = self._one(
                connection, "SELECT session_id FROM sessions WHERE lane = ?", (lane,)
            )
        return _as_opt_str(row.get("session_id")) if row is not None else None

    def set_session(self, session_id: str, lane: str = DEFAULT_LANE) -> None:
        """Persist the session id used for a lane."""
        with self.connection() as connection:
            connection.execute(
                "INSERT INTO sessions (lane, session_id) VALUES (?, ?) "
                "ON CONFLICT(lane) DO UPDATE SET session_id = excluded.session_id",
                (lane, session_id),
            )
