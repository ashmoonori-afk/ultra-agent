"""Atomic native-session descriptors keyed by the stable local lane UUID.

A descriptor is a local mapping, not a fabricated provider transcript: its
first JSONL line carries the stable ultra-agent lane UUID, the runner kind and
the provider's native session id. It lives at ``<local_id>.jsonl`` in the
configured sessions directory so the durable registry discovers it like any
other session header.

Reads are bounded and validated; invalid or mismatched state raises instead of
being silently reset. Writes replace the file atomically (temp file, file and
directory fsync) with mode 0600. No provider authentication file is ever read.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Final, TypeGuard

from ultra_agent.models import Config, RunnerKind

DESCRIPTOR_VERSION: Final = 1
MAX_HEADER_BYTES: Final = 65536

_NATIVE_RUNNERS: Final[frozenset[str]] = frozenset({"codex", "claude"})


class SessionStateError(RuntimeError):
    """A native-session descriptor is absent, malformed or mismatched."""


def _is_dict(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict)


def session_path(config: Config, local_id: str) -> Path:
    """Return the descriptor path for a stable local lane UUID."""
    return config.sessions_dir / f"{local_id}.jsonl"


def parse_descriptor(header: Mapping[str, object]) -> str | None:
    """Validate a descriptor extension and return its native session id.

    Returns ``None`` for a header that carries no descriptor fields, so an
    ordinary native provider session header stays valid. Raises
    :class:`SessionStateError` when descriptor fields are present but invalid.
    """
    if "runner" not in header and "native_session_id" not in header:
        return None
    if header.get("type") != "session":
        raise SessionStateError("native session descriptor must be a session header")
    version = header.get("version")
    if isinstance(version, bool) or version != DESCRIPTOR_VERSION:
        raise SessionStateError("native session descriptor version must be 1")
    runner = header.get("runner")
    if not isinstance(runner, str) or runner not in _NATIVE_RUNNERS:
        raise SessionStateError("native session descriptor runner must be codex or claude")
    _ = _uuid_text(header.get("id"), "id")
    return _uuid_text(header.get("native_session_id"), "native_session_id")


def load_session(config: Config, local_id: str, runner: RunnerKind) -> str | None:
    """Return the recorded native session id, or ``None`` when none is stored."""
    if runner not in _NATIVE_RUNNERS:
        raise SessionStateError("session descriptors exist only for codex and claude")
    local = _uuid_text(local_id, "local_id")
    path = session_path(config, local)
    if not path.is_file():
        return None
    header = _read_header(path)
    native = parse_descriptor(header)
    if native is None:
        raise SessionStateError(f"session file is not a native descriptor: {path}")
    if _uuid_text(header.get("id"), "id") != local:
        raise SessionStateError(f"descriptor local id does not match lane {local!r}")
    if header.get("runner") != runner:
        raise SessionStateError(
            f"descriptor runner {header.get('runner')!r} does not match {runner!r}"
        )
    return native


def save_session(
    config: Config, local_id: str, runner: RunnerKind, native_id: str
) -> Path:
    """Atomically record the native session id for a local lane UUID."""
    if runner not in _NATIVE_RUNNERS:
        raise SessionStateError("session descriptors exist only for codex and claude")
    local = _uuid_text(local_id, "local_id")
    native = _uuid_text(native_id, "native_session_id")
    target = session_path(config, local)
    directory = target.parent
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    line = json.dumps(
        {
            "type": "session",
            "id": local,
            "runner": runner,
            "native_session_id": native,
            "version": DESCRIPTOR_VERSION,
        },
        separators=(",", ":"),
    )
    _atomic_write(target, line + "\n")
    return target


def _read_header(path: Path) -> Mapping[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            line = handle.readline(MAX_HEADER_BYTES + 1)
    except (OSError, UnicodeError) as exc:
        raise SessionStateError(f"cannot read native session descriptor {path}: {exc}") from exc
    if len(line) > MAX_HEADER_BYTES:
        raise SessionStateError("native session descriptor header exceeds size limit")
    if not line.strip():
        raise SessionStateError("native session descriptor header is empty")
    try:
        parsed: object = json.loads(line)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise SessionStateError("native session descriptor header is not JSON") from exc
    if not _is_dict(parsed):
        raise SessionStateError("native session descriptor header is not an object")
    return parsed


def _uuid_text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise SessionStateError(f"{field} must be a UUID string")
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise SessionStateError(f"{field} is not a valid UUID") from exc


def _atomic_write(path: Path, data: str) -> None:
    directory = path.parent
    descriptor, temporary = tempfile.mkstemp(
        dir=directory, prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        if descriptor >= 0:
            with contextlib.suppress(OSError):
                os.close(descriptor)
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise
    _fsync_directory(directory)


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    except OSError as exc:
        raise SessionStateError(f"cannot fsync sessions directory {directory}: {exc}") from exc
    finally:
        os.close(descriptor)


__all__ = [
    "DESCRIPTOR_VERSION",
    "MAX_HEADER_BYTES",
    "SessionStateError",
    "load_session",
    "parse_descriptor",
    "save_session",
    "session_path",
]
