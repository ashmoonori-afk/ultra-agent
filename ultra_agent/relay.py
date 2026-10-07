"""Authenticated multi-machine status relay: an HTTP receiver and a file sender.

An opt-in receiver replaces deployment-specific SSH scripts. ``relay serve``
accepts a bounded ``POST /v1/status`` JSON body and authenticates it with a
per-source bearer secret read from that source's configured environment
variable. Destination routing is local server configuration, never a request
parameter, so a compromised sender cannot choose where status lands.

Accepted state and its outbox delivery are written in one SQLite transaction
through the shared :class:`~ultra_agent.store.Store`. A source carries a
monotonically increasing sequence: the exact same ``(source, sequence,
payload)`` is an idempotent duplicate that returns the original receipt without
a second delivery, while an altered payload under the same key, a gap or a
stale/out-of-order sequence is refused. The receipt table's primary key
``(source, sequence)`` enforces this uniqueness.

``relay push`` reads only complete UTF-8 lines from a status file and tracks a
durable ``(inode, byte offset, sequence)`` cursor. A line is sent with a stable
sequence and body until the server confirms a receipt; only then does the
cursor advance. A lost response is therefore safe to resend: the server
deduplicates it. Rotation or truncation resets the byte offset while the
sequence continues, and identical text at a new offset is a distinct event.

No body, header or token is ever logged, echoed or embedded in an error
message, so a secret cannot leak through a diagnostic or traceback.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import threading
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TypeAlias, TypeGuard
from urllib.parse import urlsplit

from ultra_agent.config import LOOPBACK_HOSTS
from ultra_agent.http import HttpClient
from ultra_agent.models import Config, JSONValue, RelayPeer, RelaySource
from ultra_agent.store import Store

STATUS_PATH = "/v1/status"
"""The only accepted request path of the relay receiver."""

MAX_REQUEST_BYTES = 64 * 1024
"""Maximum accepted request body size, sixty-four kibibytes."""

MAX_TEXT_LENGTH = 2000
"""Maximum characters in one relayed status line."""

MAX_FILE_READ = 1024 * 1024

SOCKET_DEADLINE_SECONDS = 10.0
"""Per-connection socket timeout enforced by the receiver."""

REQUEST_TIMEOUT_SECONDS = 10.0
"""Request timeout used by the sender's default HTTP client."""

MARKERS: frozenset[str] = frozenset({"PROGRESS", "QUESTION", "CTX_HANDOFF_READY", "RESULT"})
"""Leading tokens accepted as a complete relayed status line."""

_CHECKPOINT_PREFIX = "relay-push:"

_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS relay_sources (
        source TEXT PRIMARY KEY,
        last_sequence INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS relay_receipts (
        source TEXT NOT NULL,
        sequence INTEGER NOT NULL,
        payload_hash TEXT NOT NULL,
        receipt_id TEXT NOT NULL,
        PRIMARY KEY (source, sequence)
    )
    """,
)

_Row: TypeAlias = dict[str, object]


class RelayError(RuntimeError):
    """A relay operation was refused: bad peer, configuration or file state."""


def _as_int(value: object) -> int:
    """Return an ``int`` for a stored integer, rejecting booleans and junk."""
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return value


def _one(
    connection: sqlite3.Connection, sql: str, params: Sequence[object] = ()
) -> _Row | None:
    """Run one query on a connection whose rows are ``dict`` mappings.

    The shared store configures ``sqlite3`` with a dict row factory, so a
    fetched row is a plain string-keyed mapping; this narrows it back to that
    type for the strict checker exactly as :mod:`ultra_agent.store` does.
    """
    raw = connection.execute(sql, params).fetchone()
    if raw is None:
        return None
    row: _Row = raw
    return row


def _status_marker(text: str) -> str | None:
    """Return the leading status marker of ``text``, or ``None``."""
    parts = text.split(None, 1)
    if not parts:
        return None
    marker = parts[0]
    if marker in MARKERS:
        return marker
    return None


def _is_status_line(text: str) -> bool:
    """Whether ``text`` is one complete, bounded PROGRESS/QUESTION/... line."""
    if not text or len(text) > MAX_TEXT_LENGTH:
        return False
    if "\n" in text or "\r" in text:
        return False
    try:
        text.encode("utf-8")
    except UnicodeError:
        return False
    return _status_marker(text) is not None


def _payload_hash(source: str, sequence: int, text: str) -> str:
    """Stable content hash of one accepted relay payload."""
    digest = hashlib.sha256()
    for part in (source, str(sequence), text):
        digest.update(part.encode("utf-8"))
        digest.update(b"\x00")
    return digest.hexdigest()


def _ensure_schema(store: Store) -> None:
    """Create the relay tables inside the shared store's transaction boundary."""
    with store.connection() as connection:
        for statement in _SCHEMA_STATEMENTS:
            connection.execute(statement)


def _record(
    store: Store, source: RelaySource, sequence: int, text: str
) -> tuple[str | None, bool]:
    """Accept one status line, or report a conflict.

    Returns ``(receipt_id, duplicate)``. An exact duplicate returns its existing
    receipt without a second delivery; an altered payload under the same key or
    an out-of-order sequence returns ``(None, False)``. The source cursor, the
    outbox delivery and the receipt are written in a single transaction.
    """
    digest = _payload_hash(source.name, sequence, text)
    with store.connection() as connection:
        existing = _one(
            connection,
            "SELECT payload_hash, receipt_id FROM relay_receipts "
            "WHERE source = ? AND sequence = ?",
            (source.name, sequence),
        )
        if existing is not None:
            if str(existing.get("payload_hash")) == digest:
                return str(existing.get("receipt_id")), True
            return None, False
        row = _one(
            connection,
            "SELECT last_sequence FROM relay_sources WHERE source = ?",
            (source.name,),
        )
        last = 0 if row is None else _as_int(row.get("last_sequence"))
        if sequence != last + 1:
            return None, False
        connection.execute(
            "INSERT INTO relay_sources (source, last_sequence) VALUES (?, ?) "
            "ON CONFLICT(source) DO UPDATE SET last_sequence = excluded.last_sequence",
            (source.name, sequence),
        )
        store.add_delivery(
            source.platform,
            source.channel,
            text,
            source_key=f"relay:{source.name}:{sequence}",
            transaction=connection,
        )
        receipt_id = str(uuid.uuid4())
        connection.execute(
            "INSERT INTO relay_receipts (source, sequence, payload_hash, receipt_id) "
            "VALUES (?, ?, ?, ?)",
            (source.name, sequence, digest, receipt_id),
        )
        return receipt_id, False


def _is_json_object(value: object) -> TypeGuard[dict[str, object]]:
    """Narrow a decoded JSON value to a string-keyed object."""
    return isinstance(value, dict)


def _parse_object(body: bytes) -> dict[str, object] | None:
    """Decode a request body into a JSON object, or ``None`` when invalid."""
    try:
        decoded: object = json.loads(body.decode("utf-8"))
    except (UnicodeError, ValueError, RecursionError):
        return None
    if not _is_json_object(decoded):
        return None
    return dict(decoded.items())


class RelayHTTPServer(ThreadingHTTPServer):
    """A threaded HTTP server carrying the relay configuration and store."""

    relay_sources: Mapping[str, RelaySource]
    relay_store: Store
    daemon_threads = True


class _RelayRequestHandler(BaseHTTPRequestHandler):
    """The single ``POST /v1/status`` receiver handler."""

    protocol_version = "HTTP/1.0"
    server_version = "ultra-agent-relay"

    @property
    def _relay(self) -> RelayHTTPServer:
        server = self.server
        if not isinstance(server, RelayHTTPServer):
            raise RuntimeError("relay handler is bound to an unexpected server")
        return server

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(SOCKET_DEADLINE_SECONDS)

    def log_message(self, format: str, *args: object) -> None:
        """Silence access logging so no header, token or body is recorded."""

    def log_error(self, format: str, *args: object) -> None:
        """Silence error logging for the same reason as :meth:`log_message`."""

    def do_POST(self) -> None:
        relay = self._relay
        if urlsplit(self.path).path != STATUS_PATH:
            self._send(404, {"error": "not found"})
            return
        length = self._content_length()
        if length is None:
            self._send(400, {"error": "bad request"})
            return
        if length > MAX_REQUEST_BYTES:
            self._drain(min(length, MAX_REQUEST_BYTES + 1))
            self._send(413, {"error": "request too large"})
            return
        body = self._read_body(length)
        if body is None:
            return
        message = _parse_object(body)
        if message is None:
            self._send(400, {"error": "bad request"})
            return
        if set(message) != {"source", "sequence", "text"}:
            self._send(400, {"error": "bad request"})
            return
        source_name = message.get("source")
        if not isinstance(source_name, str):
            self._send(400, {"error": "bad request"})
            return
        source = relay.relay_sources.get(source_name)
        if source is None or not self._authorized(source):
            self._send(401, {"error": "unauthorized"})
            return
        sequence = message.get("sequence")
        if (
            isinstance(sequence, bool) or not isinstance(sequence, int)
            or not 1 <= sequence <= 2**63 - 1
        ):
            self._send(400, {"error": "bad request"})
            return
        text = message.get("text")
        if not isinstance(text, str) or not _is_status_line(text):
            self._send(400, {"error": "bad request"})
            return
        receipt, duplicate = _record(relay.relay_store, source, sequence, text)
        if receipt is None:
            self._send(409, {"error": "conflict"})
            return
        self._send(
            200 if duplicate else 201,
            {"receipt": receipt, "source": source_name, "sequence": sequence},
        )

    def _authorized(self, source: RelaySource) -> bool:
        """Constant-time check of the source's bearer secret from its environment."""
        header = self.headers.get("Authorization")
        if header is None:
            return False
        scheme, separator, credentials = header.partition(" ")
        if separator == "" or scheme.lower() != "bearer":
            return False
        token = credentials.strip()
        expected = os.environ.get(source.token_env, "")
        if not token or not expected:
            return False
        return hmac.compare_digest(token.encode("utf-8"), expected.encode("utf-8"))

    def _content_length(self) -> int | None:
        raw = self.headers.get("Content-Length")
        if raw is None:
            return None
        try:
            length = int(raw.strip())
        except ValueError:
            return None
        if length < 0:
            return None
        return length

    def _read_body(self, length: int) -> bytes | None:
        if length == 0:
            return b""
        try:
            data = self.rfile.read(length)
        except (OSError, ValueError):
            return None
        if len(data) != length:
            return None
        return data

    def _drain(self, remaining: int) -> None:
        """Read and discard at most ``remaining`` bytes so a client can read 413."""
        while remaining > 0:
            try:
                chunk = self.rfile.read(min(8192, remaining))
            except (OSError, ValueError):
                return
            if not chunk:
                return
            remaining -= len(chunk)

    def _send(self, status: int, body: JSONValue) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except OSError:
            pass


def create_relay_server(
    config: Config, store: Store, *, bind: str | None = None, port: int | None = None
) -> ThreadingHTTPServer:
    """Build (but do not start) the relay receiver for ``config``.

    The bind address and port come from ``config.relay_server`` unless an
    override is supplied; ``port=0`` asks the OS for an ephemeral port. The
    relay tables are created on the shared store here. Raises :class:`RelayError`
    when the configuration has no relay server.
    """
    relay = config.relay_server
    if relay is None:
        raise RelayError("relay server is not configured")
    _ensure_schema(store)
    server = RelayHTTPServer(
        (relay.bind if bind is None else bind, relay.port if port is None else port),
        _RelayRequestHandler,
    )
    server.relay_sources = {source.name: source for source in relay.sources}
    server.relay_store = store
    return server


def serve_relay(
    config: Config, store: Store, stop_event: threading.Event | None = None
) -> None:
    """Serve the relay receiver until ``stop_event`` is set, then close it.

    With no ``stop_event`` the server runs until interrupted. The server socket
    and the watcher thread are always closed and joined, including on error.
    """
    if stop_event is not None and stop_event.is_set():
        return
    server = create_relay_server(config, store)
    watcher: threading.Thread | None = None
    if stop_event is not None:
        watcher = threading.Thread(
            target=_shutdown_when_set, args=(server, stop_event), daemon=True
        )
        watcher.start()
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()
        if stop_event is not None:
            stop_event.set()
        if watcher is not None:
            watcher.join(timeout=5.0)


def _shutdown_when_set(server: ThreadingHTTPServer, stop_event: threading.Event) -> None:
    stop_event.wait()
    server.shutdown()


@dataclass(frozen=True, slots=True)
class _Cursor:
    """The durable sender position for one peer's status file."""

    inode: int
    offset: int
    sequence: int


def _validate_peer_url(url: str) -> None:
    parts = urlsplit(url)
    if parts.username or parts.password:
        raise RelayError("peer URL must not embed credentials")
    host = parts.hostname
    if parts.scheme == "https" and host:
        return
    if parts.scheme == "http" and host in LOOPBACK_HOSTS:
        return
    raise RelayError("peer URL must use https, or http only for a loopback host")


def _status_url(url: str) -> str:
    return url.rstrip("/") + STATUS_PATH


def _load_cursor(store: Store, key: str) -> _Cursor:
    raw = store.get_checkpoint(key)
    if raw is None:
        return _Cursor(0, 0, 0)
    try:
        decoded: object = json.loads(raw)
    except (json.JSONDecodeError, RecursionError):
        return _Cursor(0, 0, 0)
    if not _is_json_object(decoded):
        return _Cursor(0, 0, 0)
    return _Cursor(
        max(_as_int(decoded.get("inode")), 0),
        max(_as_int(decoded.get("offset")), 0),
        max(_as_int(decoded.get("sequence")), 0),
    )


def _save_cursor(store: Store, key: str, cursor: _Cursor) -> None:
    store.set_checkpoint(
        key,
        json.dumps(
            {"inode": cursor.inode, "offset": cursor.offset, "sequence": cursor.sequence}
        ),
    )


def _post_status(
    client: HttpClient,
    url: str,
    headers: Mapping[str, str],
    payload: dict[str, JSONValue],
) -> str:
    """POST one status line and return its confirmed receipt id."""
    response = client.request("POST", url, headers, payload)
    if not isinstance(response, dict):
        raise RelayError("relay response was not a JSON object")
    receipt = response.get("receipt")
    if not isinstance(receipt, str) or not receipt:
        raise RelayError("relay response did not include a receipt")
    return receipt


def push_status_file(
    config: Config,
    store: Store,
    peer: RelayPeer,
    path: Path,
    http: HttpClient | None = None,
) -> list[str]:
    """Send new complete status lines from ``path`` to ``peer``.

    Only complete UTF-8 lines that start with an accepted marker are sent. Each
    line carries a stable sequence and body until the receiver confirms a
    receipt; only then does the durable ``(inode, byte offset, sequence)``
    cursor advance, so an uncertain send is safely resent and deduplicated by
    the receiver. A partial trailing line waits. File rotation or truncation
    resets the byte offset while the sequence continues. Returns the receipt ids
    of the lines confirmed on this call.

    Relative paths are anchored to ``config.workspace``. Raises
    :class:`~ultra_agent.http.TransportError` from the first unconfirmed line,
    leaving the cursor at the last confirmed position.
    """
    _validate_peer_url(peer.url)
    token = os.environ.get(peer.token_env, "")
    if not token:
        raise RelayError("peer token environment variable is not set")
    client = http if http is not None else HttpClient(timeout=REQUEST_TIMEOUT_SECONDS)
    key = f"{_CHECKPOINT_PREFIX}{peer.name}"
    cursor = _load_cursor(store, key)
    expanded = Path(path).expanduser()
    target = expanded if expanded.is_absolute() else config.workspace / expanded
    try:
        stat = target.stat()
    except OSError:
        return []
    if not target.is_file():
        return []
    inode = stat.st_ino
    offset = cursor.offset
    sequence = cursor.sequence
    if inode != cursor.inode or stat.st_size < offset:
        offset = 0
    url = _status_url(peer.url)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    receipts: list[str] = []
    try:
        with target.open("rb") as handle:
            handle.seek(offset)
            data = handle.read(MAX_FILE_READ + 1)
        complete = data[:MAX_FILE_READ].rfind(b"\n")
        if complete < 0 and len(data) > MAX_FILE_READ:
            raise RelayError("status line exceeds read limit")
        if complete >= 0:
            for raw_line in data[: complete + 1].split(b"\n")[:-1]:
                line_bytes = raw_line + b"\n"
                try:
                    text = raw_line.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise RelayError("status file is not valid UTF-8") from exc
                if not _is_status_line(text):
                    offset += len(line_bytes)
                    continue
                candidate = sequence + 1
                payload: dict[str, JSONValue] = {
                    "source": peer.source,
                    "sequence": candidate,
                    "text": text,
                }
                receipts.append(_post_status(client, url, headers, payload))
                sequence = candidate
                offset += len(line_bytes)
    finally:
        _save_cursor(store, key, _Cursor(inode, offset, sequence))
    return receipts
