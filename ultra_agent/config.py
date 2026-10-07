"""Strict parsing for the single ultra-agent TOML configuration.

One file, ``workspace/ultra-agent.toml``, is parsed exactly once into the
immutable :class:`~ultra_agent.models.Config`. Parsing is deliberately
paranoid: unknown keys, wrong types, nonpositive or nonfinite numbers,
unbounded identifiers or argv, duplicate names, ambiguous schedule/monitor
definitions and any embedded secret are rejected with a :class:`ConfigError`.

Credentials are referenced only by environment-variable name (``token_env``,
``cookie_env``); secret values never live in configuration. Transport endpoints
are fixed by each client, so no arbitrary endpoint key is accepted.

Initialization is explicit: :func:`init_workspace` creates the private
``work``/``vault``/``state``/``sessions``/``handoffs`` directories and the
default configuration, and never overwrites an existing configuration file.
"""

from __future__ import annotations

import ipaddress
import math
import os
import re
import sys
import tomllib
from pathlib import Path
from typing import NoReturn, TypeGuard
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ultra_agent.models import (
    DEFAULT_LANE,
    Config,
    HostConfig,
    MessengerConfig,
    Monitor,
    Platform,
    Relay,
    RelayPeer,
    RelayServerConfig,
    RelaySource,
    Schedule,
    ThreadRef,
)

CONFIG_FILENAME = "ultra-agent.toml"
DEFAULT_COMMAND: tuple[str, ...] = ("omo",)

MAX_NAME_LENGTH = 64
MAX_ID_LENGTH = 200
MAX_ARGV = 64
MAX_ARG_LENGTH = 4096
MAX_TEXT_LENGTH = 65536
MAX_THREADS = 100

PLATFORMS: frozenset[str] = frozenset({"discord", "telegram", "slack"})
LOOPBACK_HOSTS: frozenset[str] = frozenset({"127.0.0.1", "::1", "localhost"})

_AT_PATTERN = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
_ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_SECRET_KEYS: frozenset[str] = frozenset(
    {
        "token",
        "bot_token",
        "api_token",
        "access_token",
        "refresh_token",
        "secret",
        "client_secret",
        "signing_secret",
        "password",
        "cookie",
        "cookies",
        "webhook_url",
        "credential",
        "credentials",
    }
)

_ENDPOINT_KEYS: frozenset[str] = frozenset(
    {"api_base", "base_url", "api_url", "endpoint", "base_endpoint", "host_url"}
)

_TOP_LEVEL_KEYS: frozenset[str] = frozenset(
    {
        "command",
        "model",
        "timeout_seconds",
        "poll_seconds",
        "tools",
        "verification_command",
        "writing_style",
        "host",
        "messengers",
        "schedules",
        "monitors",
        "relays",
        "relay_server",
        "peers",
    }
)
_HOST_KEYS: frozenset[str] = frozenset(
    {"min_memory_percent", "min_disk_gib", "max_lanes", "slot_dir", "heavy_slots"}
)
_MESSENGER_KEYS: frozenset[str] = frozenset(
    {"platform", "token_env", "users", "channels", "cookie_env", "threads", "enabled"}
)
_THREAD_KEYS: frozenset[str] = frozenset({"channel_id", "thread_id"})
_SCHEDULE_KEYS: frozenset[str] = frozenset(
    {"name", "prompt", "every_seconds", "at", "timezone", "trusted", "lane"}
)
_MONITOR_KEYS: frozenset[str] = frozenset(
    {"name", "path", "command", "timeout_seconds", "trusted", "lane", "emit_initial"}
)
_RELAY_KEYS: frozenset[str] = frozenset({"name", "path", "platform", "channel"})
_RELAY_SERVER_KEYS: frozenset[str] = frozenset({"bind", "port", "sources"})
_RELAY_SOURCE_KEYS: frozenset[str] = frozenset({"name", "platform", "channel", "token_env"})
_PEER_KEYS: frozenset[str] = frozenset({"name", "url", "source", "token_env"})


class ConfigError(ValueError):
    """Raised when the configuration file is missing, malformed or unsafe."""


def _fail(where: str, message: str) -> NoReturn:
    raise ConfigError(f"{where}: {message}")


def _check_keys(table: dict[str, object], allowed: frozenset[str], where: str) -> None:
    unknown = sorted(set(table) - allowed)
    for key in unknown:
        if key.lower() in _SECRET_KEYS:
            _fail(
                where,
                f"key '{key}' looks like an embedded secret; reference an "
                "environment variable name instead (token_env/cookie_env)",
            )
        if key.lower() in _ENDPOINT_KEYS:
            _fail(
                where,
                f"key '{key}' is not accepted; transport endpoints are fixed by "
                "each client, not configurable",
            )
    if unknown:
        _fail(where, f"unknown key(s): {', '.join(unknown)}")


def _table(value: object, where: str) -> dict[str, object]:
    if not _is_table_value(value):
        _fail(where, "expected a table")
    return value


def _is_table_value(value: object) -> TypeGuard[dict[str, object]]:
    """TOML tables have string keys by the decoder's contract."""
    return isinstance(value, dict)


def _is_array_value(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def _table_array(table: dict[str, object], key: str, where: str) -> list[dict[str, object]]:
    value = table.get(key)
    if value is None:
        return []
    if not _is_array_value(value):
        _fail(f"{where}.{key}", "expected an array of tables")
    entries = value
    items: list[dict[str, object]] = []
    for index, item in enumerate(entries):
        if not _is_table_value(item):
            _fail(f"{where}.{key}[{index}]", "expected a table")
        items.append(item)
    return items


def _bounded(value: str, where: str, max_length: int, allow_empty: bool) -> str:
    if not allow_empty and not value.strip():
        _fail(where, "must not be empty")
    if len(value) > max_length:
        _fail(where, f"must be at most {max_length} characters")
    return value


def _read_str(table: dict[str, object], key: str, where: str) -> str:
    if key not in table:
        _fail(where, f"missing required key '{key}'")
    value = table[key]
    if not isinstance(value, str):
        _fail(f"{where}.{key}", "must be a string")
    return value


def _required_str(
    table: dict[str, object],
    key: str,
    where: str,
    *,
    max_length: int = MAX_TEXT_LENGTH,
    allow_empty: bool = False,
) -> str:
    return _bounded(_read_str(table, key, where), f"{where}.{key}", max_length, allow_empty)


def _optional_str(
    table: dict[str, object],
    key: str,
    where: str,
    default: str,
    *,
    max_length: int = MAX_TEXT_LENGTH,
    allow_empty: bool = True,
) -> str:
    if key not in table:
        return default
    return _bounded(_read_str(table, key, where), f"{where}.{key}", max_length, allow_empty)


def _optional_str_or_none(
    table: dict[str, object],
    key: str,
    where: str,
    *,
    max_length: int = MAX_TEXT_LENGTH,
    allow_empty: bool = True,
) -> str | None:
    if key not in table:
        return None
    return _bounded(_read_str(table, key, where), f"{where}.{key}", max_length, allow_empty)


def _optional_bool(table: dict[str, object], key: str, where: str, default: bool) -> bool:
    if key not in table:
        return default
    value = table[key]
    if type(value) is not bool:
        _fail(f"{where}.{key}", "must be a boolean, not a number or string")
    return value


def _optional_number(
    table: dict[str, object],
    key: str,
    where: str,
    default: float,
    *,
    positive: bool = False,
) -> float:
    if key not in table:
        return default
    value = table[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(f"{where}.{key}", "must be a number")
    result = float(value)
    if not math.isfinite(result):
        _fail(f"{where}.{key}", "must be finite")
    if positive and result <= 0:
        _fail(f"{where}.{key}", "must be positive")
    return result


def _optional_int(
    table: dict[str, object],
    key: str,
    where: str,
    default: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if key not in table:
        return default
    value = table[key]
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(f"{where}.{key}", "must be an integer")
    if minimum is not None and value < minimum:
        _fail(f"{where}.{key}", f"must be at least {minimum}")
    if maximum is not None and value > maximum:
        _fail(f"{where}.{key}", f"must be at most {maximum}")
    return value


def _str_array(
    table: dict[str, object],
    key: str,
    where: str,
    *,
    default: tuple[str, ...] = (),
    max_items: int = MAX_ARGV,
    max_length: int = MAX_ID_LENGTH,
    allow_empty_items: bool = False,
) -> tuple[str, ...]:
    if key not in table:
        return tuple(default)
    value = table[key]
    if not _is_array_value(value):
        _fail(f"{where}.{key}", "must be an array of strings")
    entries = value
    if len(entries) > max_items:
        _fail(f"{where}.{key}", f"must contain at most {max_items} items")
    items: list[str] = []
    for index, item in enumerate(entries):
        if not isinstance(item, str):
            _fail(f"{where}.{key}[{index}]", "must be a string")
        items.append(_bounded(item, f"{where}.{key}[{index}]", max_length, allow_empty_items))
    return tuple(items)


def _env_name(value: str, where: str) -> str:
    name = _bounded(value, where, MAX_NAME_LENGTH, allow_empty=False)
    if not _ENV_NAME_PATTERN.match(name):
        _fail(
            where,
            "must be an environment variable name (letters, digits and "
            "underscore, not starting with a digit)",
        )
    return name


def _required_env_name(table: dict[str, object], key: str, where: str) -> str:
    return _env_name(_read_str(table, key, where), f"{where}.{key}")


def _required_platform(table: dict[str, object], key: str, where: str) -> Platform:
    value = _required_str(table, key, where, max_length=MAX_NAME_LENGTH)
    match value:
        case "discord" | "telegram" | "slack":
            return value
        case _:
            _fail(f"{where}.{key}", "must be one of discord, telegram or slack")


def _validate_zone(name: str, where: str) -> None:
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, KeyError, OSError) as exc:
        raise ConfigError(f"{where}: '{name}' is not a known IANA time zone") from exc


def _validate_peer_url(url: str, where: str) -> str:
    parts = urlsplit(url)
    if parts.username or parts.password:
        _fail(where, "credentials must not be embedded in the URL")
    host = parts.hostname
    if not host:
        _fail(where, "must be an absolute URL with a host")
    if parts.scheme == "https":
        return url
    if parts.scheme == "http" and host in LOOPBACK_HOSTS:
        return url
    _fail(where, "must use https, or http only for a loopback host")


def _reject_windows_mount(path: Path) -> None:
    if sys.platform != "linux":
        return
    real = os.path.realpath(path)
    if real == "/mnt/c" or real.startswith("/mnt/c/"):
        raise ConfigError(
            f"workspace {real} is on a Windows-mounted filesystem (/mnt/c); "
            "SQLite and the vault require a local filesystem with working locks"
        )


def _parse_host(value: object, where: str) -> HostConfig:
    if value is None:
        return HostConfig()
    table = _table(value, where)
    _check_keys(table, _HOST_KEYS, where)
    percent = _optional_number(table, "min_memory_percent", where, 20.0)
    if percent <= 0 or percent > 100:
        _fail(f"{where}.min_memory_percent", "must be greater than 0 and at most 100")
    disk = _optional_number(table, "min_disk_gib", where, 1.0, positive=True)
    lanes = _optional_int(table, "max_lanes", where, 20, minimum=0)
    slots = _optional_int(table, "heavy_slots", where, 1, minimum=1)
    slot_dir = _optional_str_or_none(table, "slot_dir", where, allow_empty=False)
    if slot_dir is None:
        return HostConfig(
            min_memory_percent=percent,
            min_disk_gib=disk,
            max_lanes=lanes,
            heavy_slots=slots,
        )
    return HostConfig(
        min_memory_percent=percent,
        min_disk_gib=disk,
        max_lanes=lanes,
        slot_dir=Path(slot_dir).expanduser(),
        heavy_slots=slots,
    )


def _parse_threads(table: dict[str, object], where: str) -> tuple[ThreadRef, ...]:
    items = _table_array(table, "threads", where)
    if len(items) > MAX_THREADS:
        _fail(f"{where}.threads", f"must contain at most {MAX_THREADS} entries")
    threads: list[ThreadRef] = []
    for index, item in enumerate(items):
        item_where = f"{where}.threads[{index}]"
        _check_keys(item, _THREAD_KEYS, item_where)
        channel_id = _required_str(item, "channel_id", item_where, max_length=MAX_ID_LENGTH)
        thread_id = _required_str(item, "thread_id", item_where, max_length=MAX_ID_LENGTH)
        threads.append(ThreadRef(channel_id=channel_id, thread_id=thread_id))
    return tuple(threads)


def _parse_messengers(items: list[dict[str, object]], ctx: str) -> tuple[MessengerConfig, ...]:
    seen: set[str] = set()
    messengers: list[MessengerConfig] = []
    for index, table in enumerate(items):
        where = f"{ctx}.messengers[{index}]"
        _check_keys(table, _MESSENGER_KEYS, where)
        platform = _required_platform(table, "platform", where)
        if platform in seen:
            _fail(where, f"duplicate messenger platform '{platform}'")
        seen.add(platform)
        token_env = _required_env_name(table, "token_env", where)
        users = _str_array(table, "users", where, max_length=MAX_ID_LENGTH)
        channels = _str_array(table, "channels", where, max_length=MAX_ID_LENGTH)
        cookie_env = _optional_str_or_none(
            table, "cookie_env", where, max_length=MAX_NAME_LENGTH, allow_empty=False
        )
        if cookie_env is not None:
            _env_name(cookie_env, f"{where}.cookie_env")
        threads = _parse_threads(table, where)
        enabled = _optional_bool(table, "enabled", where, True)
        messengers.append(
            MessengerConfig(
                platform=platform,
                token_env=token_env,
                users=users,
                channels=channels,
                cookie_env=cookie_env,
                threads=threads,
                enabled=enabled,
            )
        )
    return tuple(messengers)


def _parse_schedules(items: list[dict[str, object]], ctx: str) -> tuple[Schedule, ...]:
    seen: set[str] = set()
    schedules: list[Schedule] = []
    for index, table in enumerate(items):
        where = f"{ctx}.schedules[{index}]"
        _check_keys(table, _SCHEDULE_KEYS, where)
        name = _required_str(table, "name", where, max_length=MAX_NAME_LENGTH)
        if name in seen:
            _fail(where, f"duplicate schedule name '{name}'")
        seen.add(name)
        prompt = _required_str(table, "prompt", where)
        has_every = "every_seconds" in table
        has_at = "at" in table
        if has_every and has_at:
            _fail(where, "define either every_seconds or at, not both")
        if not has_every and not has_at:
            _fail(where, "one of every_seconds or at is required")
        every_seconds: float | None = None
        at: str | None = None
        timezone: str | None = None
        if has_every:
            if "timezone" in table:
                _fail(where, "timezone is only valid with at")
            every_seconds = _optional_number(table, "every_seconds", where, 0.0, positive=True)
        else:
            at = _required_str(table, "at", where, max_length=8)
            if not _AT_PATTERN.match(at):
                _fail(f"{where}.at", "must be a 24-hour HH:MM time")
            timezone = _required_str(table, "timezone", where, max_length=MAX_NAME_LENGTH)
            _validate_zone(timezone, f"{where}.timezone")
        trusted = _optional_bool(table, "trusted", where, False)
        lane = _optional_str(
            table, "lane", where, DEFAULT_LANE, max_length=MAX_NAME_LENGTH, allow_empty=False
        )
        schedules.append(
            Schedule(
                name=name,
                prompt=prompt,
                every_seconds=every_seconds,
                at=at,
                timezone=timezone,
                trusted=trusted,
                lane=lane,
            )
        )
    return tuple(schedules)


def _parse_monitors(items: list[dict[str, object]], ctx: str) -> tuple[Monitor, ...]:
    seen: set[str] = set()
    monitors: list[Monitor] = []
    for index, table in enumerate(items):
        where = f"{ctx}.monitors[{index}]"
        _check_keys(table, _MONITOR_KEYS, where)
        name = _required_str(table, "name", where, max_length=MAX_NAME_LENGTH)
        if name in seen:
            _fail(where, f"duplicate monitor name '{name}'")
        seen.add(name)
        has_path = "path" in table
        has_command = "command" in table
        if has_path and has_command:
            _fail(where, "define either path or command, not both")
        if not has_path and not has_command:
            _fail(where, "one of path or command is required")
        path: Path | None = None
        command: tuple[str, ...] = ()
        if has_path:
            path = Path(_required_str(table, "path", where))
        else:
            command = _str_array(
                table, "command", where, max_items=MAX_ARGV, max_length=MAX_ARG_LENGTH
            )
            if not command:
                _fail(f"{where}.command", "must not be empty")
        timeout = _optional_number(table, "timeout_seconds", where, 600.0, positive=True)
        trusted = _optional_bool(table, "trusted", where, False)
        lane = _optional_str(
            table, "lane", where, DEFAULT_LANE, max_length=MAX_NAME_LENGTH, allow_empty=False
        )
        emit_initial = _optional_bool(table, "emit_initial", where, False)
        monitors.append(
            Monitor(
                name=name,
                path=path,
                command=command,
                timeout_seconds=timeout,
                trusted=trusted,
                lane=lane,
                emit_initial=emit_initial,
            )
        )
    return tuple(monitors)


def _parse_relays(items: list[dict[str, object]], ctx: str) -> tuple[Relay, ...]:
    seen: set[str] = set()
    relays: list[Relay] = []
    for index, table in enumerate(items):
        where = f"{ctx}.relays[{index}]"
        _check_keys(table, _RELAY_KEYS, where)
        name = _required_str(table, "name", where, max_length=MAX_NAME_LENGTH)
        if name in seen:
            _fail(where, f"duplicate relay name '{name}'")
        seen.add(name)
        path = Path(_required_str(table, "path", where))
        platform = _required_platform(table, "platform", where)
        channel = _required_str(table, "channel", where, max_length=MAX_ID_LENGTH)
        relays.append(Relay(name=name, path=path, platform=platform, channel=channel))
    return tuple(relays)


def _parse_relay_server(value: object, ctx: str) -> RelayServerConfig | None:
    if value is None:
        return None
    where = f"{ctx}.relay_server"
    table = _table(value, where)
    _check_keys(table, _RELAY_SERVER_KEYS, where)
    bind = _optional_str(
        table, "bind", where, "127.0.0.1", max_length=MAX_NAME_LENGTH, allow_empty=False
    )
    try:
        ipaddress.ip_address(bind)
    except ValueError as exc:
        raise ConfigError(f"{where}.bind: must be an IP address") from exc
    port = _optional_int(table, "port", where, 8765, minimum=1, maximum=65535)
    seen: set[str] = set()
    sources: list[RelaySource] = []
    for index, item in enumerate(_table_array(table, "sources", where)):
        item_where = f"{where}.sources[{index}]"
        _check_keys(item, _RELAY_SOURCE_KEYS, item_where)
        name = _required_str(item, "name", item_where, max_length=MAX_NAME_LENGTH)
        if name in seen:
            _fail(item_where, f"duplicate relay source name '{name}'")
        seen.add(name)
        platform = _required_platform(item, "platform", item_where)
        channel = _required_str(item, "channel", item_where, max_length=MAX_ID_LENGTH)
        token_env = _required_env_name(item, "token_env", item_where)
        sources.append(
            RelaySource(name=name, platform=platform, channel=channel, token_env=token_env)
        )
    return RelayServerConfig(bind=bind, port=port, sources=tuple(sources))


def _parse_peers(items: list[dict[str, object]], ctx: str) -> tuple[RelayPeer, ...]:
    seen: set[str] = set()
    peers: list[RelayPeer] = []
    for index, table in enumerate(items):
        where = f"{ctx}.peers[{index}]"
        _check_keys(table, _PEER_KEYS, where)
        name = _required_str(table, "name", where, max_length=MAX_NAME_LENGTH)
        if name in seen:
            _fail(where, f"duplicate peer name '{name}'")
        seen.add(name)
        url = _validate_peer_url(
            _required_str(table, "url", where, max_length=MAX_TEXT_LENGTH),
            f"{where}.url",
        )
        source = _required_str(table, "source", where, max_length=MAX_NAME_LENGTH)
        token_env = _required_env_name(table, "token_env", where)
        peers.append(RelayPeer(name=name, url=url, source=source, token_env=token_env))
    return tuple(peers)


def _parse_config(workspace: Path, data: dict[str, object], source: Path) -> Config:
    ctx = str(source)
    _check_keys(data, _TOP_LEVEL_KEYS, ctx)

    command = _str_array(
        data, "command", ctx, default=DEFAULT_COMMAND, max_items=MAX_ARGV, max_length=MAX_ARG_LENGTH
    )
    if not command:
        _fail(f"{ctx}.command", "must not be empty")
    tools = _str_array(data, "tools", ctx, max_items=MAX_ARGV, max_length=MAX_ARG_LENGTH)
    verification = _str_array(
        data, "verification_command", ctx, max_items=MAX_ARGV, max_length=MAX_ARG_LENGTH
    )
    model = _optional_str_or_none(data, "model", ctx, max_length=MAX_NAME_LENGTH, allow_empty=False)
    timeout = _optional_number(data, "timeout_seconds", ctx, 600.0, positive=True)
    poll = _optional_number(data, "poll_seconds", ctx, 3.0, positive=True)
    writing_style = _optional_str(data, "writing_style", ctx, "", allow_empty=True)

    host = _parse_host(data.get("host"), f"{ctx}.host")
    messengers = _parse_messengers(_table_array(data, "messengers", ctx), ctx)
    schedules = _parse_schedules(_table_array(data, "schedules", ctx), ctx)
    monitors = _parse_monitors(_table_array(data, "monitors", ctx), ctx)
    relays = _parse_relays(_table_array(data, "relays", ctx), ctx)
    relay_server = _parse_relay_server(data.get("relay_server"), ctx)
    peers = _parse_peers(_table_array(data, "peers", ctx), ctx)

    return Config(
        workspace=workspace,
        command=command,
        model=model,
        timeout_seconds=timeout,
        poll_seconds=poll,
        tools=tools,
        schedules=schedules,
        monitors=monitors,
        relays=relays,
        verification_command=verification,
        writing_style=writing_style,
        messengers=messengers,
        host=host,
        relay_server=relay_server,
        peers=peers,
    )


def load_config(workspace: Path) -> Config:
    """Parse ``workspace/ultra-agent.toml`` into an immutable :class:`Config`.

    Raises :class:`ConfigError` when the file is missing, is not valid TOML or
    fails any strict validation rule.
    """
    root = Path(os.path.realpath(Path(workspace).expanduser()))
    _reject_windows_mount(root)
    _reject_windows_mount(root / "state")
    _reject_windows_mount(root / "vault")
    path = root / CONFIG_FILENAME
    try:
        raw = path.read_bytes()
    except FileNotFoundError as exc:
        raise ConfigError(f"no configuration at {path}; run init first") from exc
    except OSError as exc:
        raise ConfigError(f"cannot read configuration at {path}: {exc.strerror or exc}") from exc
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigError(f"configuration at {path} is not valid UTF-8") from exc
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {path}: {exc}") from exc
    return _parse_config(root, data, path)


def init_workspace(workspace: Path) -> Config:
    """Create the workspace, its private directories and the default config.

    An existing ``ultra-agent.toml`` is never overwritten; it is loaded as-is.
    The private ``state``, ``sessions``, ``handoffs``, ``work`` and ``vault``
    directories are created only here, and only on this explicit call.
    """
    root = Path(os.path.realpath(Path(workspace).expanduser()))
    _reject_windows_mount(root)
    root.mkdir(parents=True, exist_ok=True)
    config_path = root / CONFIG_FILENAME
    try:
        with open(config_path, "x", encoding="utf-8") as handle:
            handle.write(config_text())
    except FileExistsError:
        pass
    config = load_config(root)
    for directory in (
        config.work_dir,
        config.vault_dir,
        config.state_dir,
        config.sessions_dir,
        config.handoffs_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(0o700)
    return config


def config_text() -> str:
    """Return the default configuration template, with no messaging configured."""
    return """\
# ultra-agent configuration. Parsing is strict: unknown keys are rejected.
# Secrets never appear here; credentials are named environment variables.

command = ["omo"]
timeout_seconds = 600.0
poll_seconds = 3.0
tools = []
verification_command = []
writing_style = ""

# Optional model name passed to the agent command.
# model = ""

# Free-form writing style guidance added to prompts. Empty adds nothing.

[host]
min_memory_percent = 20.0
min_disk_gib = 1.0
max_lanes = 20
heavy_slots = 1
# slot_dir = "~/.local/state/ultra-agent/slots"

# --- Messaging (disabled by default) -------------------------------------
# Each messenger reads its token from a named environment variable.
#
# [[messengers]]
# platform = "discord"
# token_env = "DISCORD_TOKEN"
# users = ["1234567890"]
# channels = ["0987654321"]
# enabled = true
# [[messengers.threads]]
# channel_id = "0987654321"
# thread_id = "1122334455"

# --- Schedules -----------------------------------------------------------
# Define either every_seconds (interval) or at + timezone (daily), not both.
#
# [[schedules]]
# name = "interval-job"
# prompt = "Summarize open work."
# every_seconds = 3600
# trusted = false
#
# [[schedules]]
# name = "daily-job"
# prompt = "Write the daily note."
# at = "09:00"
# timezone = "UTC"

# --- Monitors ------------------------------------------------------------
# Define either path or command, not both.
#
# [[monitors]]
# name = "log"
# path = "/var/log/app.log"
# emit_initial = false

# --- Status relay --------------------------------------------------------
# [[relays]]
# name = "progress"
# path = "work/status.log"
# platform = "discord"
# channel = "0987654321"

# --- Relay server (opt-in) ----------------------------------------------
# [relay_server]
# bind = "127.0.0.1"
# port = 8765
# [[relay_server.sources]]
# name = "laptop"
# platform = "discord"
# channel = "0987654321"
# token_env = "RELAY_LAPTOP_TOKEN"

# --- Outbound peers ------------------------------------------------------
# HTTPS, or HTTP only for a loopback host.
# [[peers]]
# name = "laptop"
# url = "https://laptop.example:8765"
# source = "desktop"
# token_env = "RELAY_DESKTOP_TOKEN"
"""
