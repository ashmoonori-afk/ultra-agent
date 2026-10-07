"""Immutable, typed contracts shared across the ultra-agent runtime.

This module holds data definitions only: frozen slotted dataclasses, state
Literal aliases and a recursive JSONValue alias. Parsing, validation and every
form of I/O live in the module that owns the relevant boundary, so nothing here
reads or writes configuration, state or the network.

Records are safe to construct without credentials: optional secret references
are environment-variable names (token_env, cookie_env), never secrets.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, TypeAlias

Platform: TypeAlias = Literal["discord", "telegram", "slack"]
"""Messenger transports supported by the unified messenger."""

JobStatus: TypeAlias = Literal[
    "awaiting_approval",
    "queued",
    "running",
    "completed",
    "failed",
    "uncertain",
]
"""Durable job lifecycle states."""

DeliveryStatus: TypeAlias = Literal["pending", "sending", "sent", "uncertain", "resolved"]
"""Durable outbound delivery states."""

JSONValue: TypeAlias = (
    None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
)
"""Any value that can round-trip through JSON, nested to arbitrary depth."""

DEFAULT_LANE = "default"
"""Lane key assumed by records that do not name one."""


def _default_slot_dir() -> Path:
    """Host-wide heavy-slot directory under the user's local state directory."""
    return Path.home() / ".local" / "state" / "ultra-agent" / "slots"


@dataclass(frozen=True, slots=True)
class ThreadRef:
    """A messenger thread to track for inbound replies."""

    channel_id: str
    thread_id: str


@dataclass(frozen=True, slots=True)
class MessengerConfig:
    """One messenger transport's configuration; secrets stay in the environment."""

    platform: Platform
    token_env: str = ""
    users: tuple[str, ...] = ()
    channels: tuple[str, ...] = ()
    cookie_env: str | None = None
    threads: tuple[ThreadRef, ...] = ()
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class HostConfig:
    """Fail-closed host guard thresholds and the shared heavy-slot location."""

    min_memory_percent: float = 20.0
    min_disk_gib: float = 1.0
    max_lanes: int = 20
    slot_dir: Path = field(default_factory=_default_slot_dir)
    heavy_slots: int = 1


@dataclass(frozen=True, slots=True)
class Schedule:
    """A recurring automation definition: interval or daily local time."""

    name: str
    prompt: str
    every_seconds: float | None = None
    at: str | None = None
    timezone: str | None = None
    trusted: bool = False
    lane: str = DEFAULT_LANE


@dataclass(frozen=True, slots=True)
class Monitor:
    """A file- or command-based change monitor."""

    name: str
    path: Path | None = None
    command: tuple[str, ...] = ()
    timeout_seconds: float = 600.0
    trusted: bool = False
    lane: str = DEFAULT_LANE
    emit_initial: bool = False


@dataclass(frozen=True, slots=True)
class Relay:
    """A local status file relayed to one configured messenger target."""

    name: str
    path: Path
    platform: Platform
    channel: str


@dataclass(frozen=True, slots=True)
class RelaySource:
    """A named source accepted by the relay server, with its local target."""

    name: str
    platform: Platform
    channel: str
    token_env: str = ""


@dataclass(frozen=True, slots=True)
class RelayPeer:
    """A remote relay receiver a local status file is pushed to."""

    name: str
    url: str
    source: str
    token_env: str = ""


@dataclass(frozen=True, slots=True)
class RelayServerConfig:
    """Opt-in HTTP relay receiver bind address and allowed sources."""

    bind: str = "127.0.0.1"
    port: int = 8765
    sources: tuple[RelaySource, ...] = ()


@dataclass(frozen=True, slots=True)
class Submission:
    """Immutable inbound work request before it becomes a durable job."""

    source_key: str
    prompt: str
    approved: bool = False
    channel_id: str | None = None
    reply_to: str | None = None
    thread_id: str | None = None
    lane: str = DEFAULT_LANE
    platform: Platform | None = None
    heavy: bool = True


@dataclass(frozen=True, slots=True)
class Job:
    """A durable agent job: immutable input plus lifecycle state."""

    id: str
    source_key: str
    prompt: str
    approved: bool = False
    channel_id: str | None = None
    reply_to: str | None = None
    thread_id: str | None = None
    lane: str = DEFAULT_LANE
    platform: Platform | None = None
    heavy: bool = True
    status: JobStatus = "awaiting_approval"
    result: str | None = None
    error: str | None = None
    session_id: str | None = None
    retry_of: str | None = None
    verified: bool = False


@dataclass(frozen=True, slots=True)
class Delivery:
    """One outbound reply chunk and its durable delivery state."""

    id: str
    channel_id: str
    content: str
    job_id: str | None = None
    reply_to: str | None = None
    thread_id: str | None = None
    status: DeliveryStatus = "pending"
    remote_id: str | None = None
    ordinal: int = 0
    platform: Platform | None = None


@dataclass(frozen=True, slots=True)
class TurnResult:
    """Outcome of one bounded omo agent turn."""

    success: bool
    text: str = ""
    error: str | None = None
    compaction_failed: bool = False
    exit_code: int | None = None


@dataclass(frozen=True, slots=True)
class CheckResult:
    """Outcome of the operator-configured verification command."""

    success: bool
    text: str = ""
    error: str | None = None
    exit_code: int | None = None


@dataclass(frozen=True, slots=True)
class Config:
    """The single parsed configuration record; paths derive from workspace."""

    workspace: Path
    command: tuple[str, ...] = ("omo",)
    model: str | None = None
    timeout_seconds: float = 600.0
    poll_seconds: float = 3.0
    tools: tuple[str, ...] = ()
    schedules: tuple[Schedule, ...] = ()
    monitors: tuple[Monitor, ...] = ()
    relays: tuple[Relay, ...] = ()
    verification_command: tuple[str, ...] = ()
    writing_style: str = ""
    messengers: tuple[MessengerConfig, ...] = ()
    host: HostConfig = field(default_factory=HostConfig)
    relay_server: RelayServerConfig | None = None
    peers: tuple[RelayPeer, ...] = ()

    @property
    def state_dir(self) -> Path:
        return self.workspace / "state"

    @property
    def work_dir(self) -> Path:
        return self.workspace / "work"

    @property
    def vault_dir(self) -> Path:
        return self.workspace / "vault"

    @property
    def sessions_dir(self) -> Path:
        return self.state_dir / "sessions"

    @property
    def handoffs_dir(self) -> Path:
        return self.state_dir / "handoffs"

    @property
    def db_path(self) -> Path:
        return self.state_dir / "runtime.db"

    @property
    def config_path(self) -> Path:
        return self.workspace / "ultra-agent.toml"
