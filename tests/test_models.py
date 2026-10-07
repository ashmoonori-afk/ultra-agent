"""Contract tests for the shared immutable model types.

No I/O, parsing or validation lives in ultra_agent.models, so these tests only
pin the shape of the contracts: immutability, slots, defaults, derived paths and
the required-field boundaries other modules depend on.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import FrozenInstanceError, fields, is_dataclass
from pathlib import Path
from typing import Any, ForwardRef, get_args, get_type_hints

import pytest

from ultra_agent.models import (
    DEFAULT_LANE,
    CheckResult,
    Config,
    Delivery,
    HostConfig,
    Job,
    JSONValue,
    MessengerConfig,
    Monitor,
    Relay,
    RelayPeer,
    RelayServerConfig,
    RelaySource,
    Schedule,
    Submission,
    ThreadRef,
    TurnResult,
)

CONTRACTS: tuple[type[Any], ...] = (
    Config,
    MessengerConfig,
    ThreadRef,
    HostConfig,
    Schedule,
    Monitor,
    Relay,
    RelaySource,
    RelayPeer,
    RelayServerConfig,
    Submission,
    Job,
    Delivery,
    TurnResult,
    CheckResult,
)


def _field_names(cls: type[Any]) -> set[str]:
    return {f.name for f in fields(cls)}


def _build(factory: Callable[..., object], **kwargs: object) -> object:
    return factory(**kwargs)


def test_every_contract_is_a_frozen_slotted_dataclass() -> None:
    for cls in CONTRACTS:
        assert is_dataclass(cls), cls
        assert cls.__dataclass_params__.frozen is True, cls
        assert set(cls.__slots__) == _field_names(cls), cls


def test_frozen_and_slots_are_enforced_at_runtime() -> None:
    sub = Submission(source_key="src", prompt="hi")
    with pytest.raises(FrozenInstanceError):
        sub.approved = True
    # CPython refuses an unknown slot either with AttributeError or with a
    # TypeError from the generated frozen __setattr__; the write never lands.
    with pytest.raises((AttributeError, TypeError)):
        sub.not_a_field = 1
    assert not hasattr(sub, "not_a_field")
    assert not hasattr(sub, "__dict__")


def test_required_fields_cannot_be_omitted() -> None:
    for cls, kwargs in (
        (Config, {}),
        (Schedule, {"prompt": "p"}),
        (Monitor, {}),
        (Relay, {"path": Path("x"), "platform": "discord", "channel": "c"}),
        (MessengerConfig, {}),
        (RelaySource, {"platform": "slack", "channel": "c"}),
        (RelayPeer, {"url": "https://example.invalid", "source": "s"}),
        (Submission, {"prompt": "p"}),
        (Job, {"source_key": "s", "prompt": "p"}),
        (Delivery, {"channel_id": "c", "content": "x"}),
        (TurnResult, {}),
        (CheckResult, {}),
    ):
        with pytest.raises(TypeError):
            _build(cls, **kwargs)


def test_config_defaults_and_derived_paths(tmp_path: Path) -> None:
    cfg = Config(workspace=tmp_path)
    assert cfg.command == ("omo",)
    assert cfg.model is None
    assert cfg.timeout_seconds == 600
    assert cfg.poll_seconds == 3
    assert cfg.tools == ()
    assert cfg.verification_command == ()
    assert cfg.writing_style == ""
    assert cfg.schedules == () and cfg.monitors == () and cfg.relays == ()
    assert cfg.messengers == () and cfg.peers == ()
    assert cfg.relay_server is None
    assert isinstance(cfg.host, HostConfig)

    assert cfg.state_dir == tmp_path / "state"
    assert cfg.work_dir == tmp_path / "work"
    assert cfg.vault_dir == tmp_path / "vault"
    assert cfg.sessions_dir == tmp_path / "state" / "sessions"
    assert cfg.handoffs_dir == tmp_path / "state" / "handoffs"
    assert cfg.db_path == tmp_path / "state" / "runtime.db"
    assert cfg.config_path == tmp_path / "ultra-agent.toml"


def test_defaults_are_isolated_between_instances(tmp_path: Path) -> None:
    first = Config(workspace=tmp_path)
    second = Config(workspace=tmp_path)
    assert first == second
    assert hash(first) == hash(second)
    assert first.host is not second.host
    assert first.host == second.host
    assert Config(workspace=tmp_path / "other") != first


def test_host_config_fail_closed_defaults() -> None:
    host = HostConfig()
    assert host.min_memory_percent == 20
    assert host.min_disk_gib == 1
    assert host.max_lanes == 20
    assert host.heavy_slots == 1
    assert host.slot_dir.is_absolute()
    assert host.slot_dir.name == "slots"
    assert ".local" in host.slot_dir.parts and "state" in host.slot_dir.parts

    custom = HostConfig(slot_dir=Path("/tmp/other-slots"), heavy_slots=2)
    assert custom.slot_dir == Path("/tmp/other-slots")
    assert custom.heavy_slots == 2
    assert custom != host


def test_messenger_config_needs_no_credentials() -> None:
    msg = MessengerConfig(platform="discord")
    assert msg.token_env == ""
    assert msg.cookie_env is None
    assert msg.users == () and msg.channels == () and msg.threads == ()
    assert msg.enabled is True

    thread = ThreadRef(channel_id="c1", thread_id="t1")
    slack = MessengerConfig(
        platform="slack",
        token_env="SLACK_TOKEN",
        cookie_env="SLACK_COOKIE",
        users=("u1",),
        channels=("c1",),
        threads=(thread,),
        enabled=False,
    )
    assert slack.threads == (thread,)
    assert slack.cookie_env == "SLACK_COOKIE"
    assert slack.enabled is False


def test_credential_fields_are_environment_names_only() -> None:
    assert _field_names(MessengerConfig) == {
        "platform",
        "token_env",
        "users",
        "channels",
        "cookie_env",
        "threads",
        "enabled",
    }
    assert _field_names(RelaySource) == {"name", "platform", "channel", "token_env"}
    assert _field_names(RelayPeer) == {"name", "url", "source", "token_env"}
    assert "token" not in _field_names(MessengerConfig)


def test_submission_lane_platform_and_heavy_defaults() -> None:
    sub = Submission(source_key="src", prompt="hi")
    assert sub.approved is False
    assert sub.channel_id is None and sub.reply_to is None
    assert sub.lane == DEFAULT_LANE == "default"
    assert sub.platform is None
    assert sub.heavy is True
    assert Submission(source_key="s", prompt="p", heavy=False).heavy is False


def test_job_status_and_verified_defaults() -> None:
    job = Job(id="j1", source_key="src", prompt="hi")
    assert job.status == "awaiting_approval"
    assert job.verified is False
    assert job.result is None and job.error is None
    assert job.session_id is None and job.retry_of is None
    assert job.lane == DEFAULT_LANE and job.platform is None and job.heavy is True
    assert job.approved is False


def test_delivery_defaults() -> None:
    delivery = Delivery(id="d1", channel_id="c", content="x")
    assert delivery.job_id is None and delivery.reply_to is None
    assert delivery.status == "pending"
    assert delivery.remote_id is None and delivery.ordinal == 0
    assert delivery.platform is None


def test_turn_and_check_results() -> None:
    ok = TurnResult(success=True)
    assert ok.text == "" and ok.error is None
    assert ok.compaction_failed is False and ok.exit_code is None

    failed = TurnResult(success=False, error="boom", exit_code=1, compaction_failed=True)
    assert failed.success is False
    assert failed.compaction_failed is True

    check = CheckResult(success=True, exit_code=0)
    assert check.success is True and check.exit_code == 0 and check.error is None


def test_schedule_monitor_relay_and_relay_server_shapes(tmp_path: Path) -> None:
    interval = Schedule(name="nightly", prompt="p", every_seconds=3600, trusted=True)
    assert interval.every_seconds == 3600 and interval.at is None
    assert interval.timezone is None and interval.trusted is True
    assert interval.lane == DEFAULT_LANE

    daily = Schedule(name="daily", prompt="p", at="09:30", timezone="UTC")
    assert daily.at == "09:30" and daily.timezone == "UTC"
    assert daily.every_seconds is None and daily.trusted is False

    probe = Monitor(name="m", path=tmp_path / "status.log", emit_initial=True)
    assert probe.path == tmp_path / "status.log"
    assert probe.command == () and probe.timeout_seconds == 600
    assert probe.trusted is False and probe.lane == DEFAULT_LANE
    assert probe.emit_initial is True

    command = Monitor(name="c", command=("probe", "--flag"), trusted=True)
    assert command.command == ("probe", "--flag") and command.path is None

    relay = Relay(name="r", path=tmp_path / "out.log", platform="discord", channel="42")
    assert relay.platform == "discord" and relay.channel == "42"

    source = RelaySource(name="laptop", platform="telegram", channel="7", token_env="T")
    peer = RelayPeer(name="desk", url="https://example.invalid", source="laptop")
    server = RelayServerConfig(bind="0.0.0.0", port=9000, sources=(source,))
    assert server.port == 9000 and server.sources == (source,)
    assert peer.token_env == "" and source.token_env == "T"
    assert RelayServerConfig().port == 8765
    assert RelayServerConfig().bind == "127.0.0.1"


def test_config_carries_every_expanded_feature(tmp_path: Path) -> None:
    cfg = Config(
        workspace=tmp_path,
        tools=("read", "write"),
        writing_style="terse",
        verification_command=("pytest", "-q"),
        schedules=(Schedule(name="s", prompt="p", every_seconds=60),),
        monitors=(Monitor(name="m", command=("probe",)),),
        relays=(
            Relay(name="r", path=tmp_path / "o.log", platform="slack", channel="c"),
        ),
        messengers=(MessengerConfig(platform="telegram", token_env="TG_TOKEN"),),
        host=HostConfig(heavy_slots=2, slot_dir=tmp_path / "slots"),
        relay_server=RelayServerConfig(port=8888),
        peers=(RelayPeer(name="p", url="http://127.0.0.1:8888", source="s"),),
    )
    assert cfg.tools == ("read", "write")
    assert cfg.writing_style == "terse"
    assert cfg.verification_command == ("pytest", "-q")
    assert cfg.messengers[0].platform == "telegram"
    assert cfg.host.slot_dir == tmp_path / "slots"
    assert cfg.host.heavy_slots == 2
    assert cfg.relay_server is not None and cfg.relay_server.port == 8888
    assert cfg.peers[0].url == "http://127.0.0.1:8888"


def _references_jsonvalue(alias: object) -> bool:
    # A builtin generic stores its forward reference as a plain string, while
    # typing constructs store a ForwardRef; both mean the alias recurses.
    if isinstance(alias, str):
        return alias == "JSONValue"
    if isinstance(alias, ForwardRef):
        return alias.__forward_arg__ == "JSONValue"
    return any(_references_jsonvalue(arg) for arg in get_args(alias))


def test_jsonvalue_alias_is_recursive_and_usable() -> None:
    def _identity(value: JSONValue) -> JSONValue:
        return value

    assert get_type_hints(_identity)["value"] is not None
    assert _references_jsonvalue(JSONValue)

    payload: JSONValue = {"a": [1, 2.5, None, True, "s"], "b": {"c": []}}
    assert _identity(payload) is payload
