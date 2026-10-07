from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

import ultra_agent.config as config
from ultra_agent.config import (
    ConfigError,
    config_text,
    init_workspace,
    load_config,
)
from ultra_agent.models import (
    Config,
    HostConfig,
    MessengerConfig,
    Monitor,
    Relay,
    RelayPeer,
    RelayServerConfig,
    Schedule,
    ThreadRef,
)


def _write(tmp_path: Path, text: str) -> Path:
    (tmp_path / "ultra-agent.toml").write_text(text, encoding="utf-8")
    return tmp_path


def _real(tmp_path: Path) -> Path:
    return Path(os.path.realpath(tmp_path))


def test_default_template_parses_to_defaults(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, config_text()))
    assert isinstance(cfg, Config)
    assert cfg.workspace == _real(tmp_path)
    assert cfg.command == ("omo",)
    assert cfg.model is None
    assert cfg.timeout_seconds == 600.0
    assert cfg.poll_seconds == 3.0
    assert cfg.tools == ()
    assert cfg.verification_command == ()
    assert cfg.writing_style == ""
    assert cfg.messengers == ()
    assert cfg.schedules == ()
    assert cfg.monitors == ()
    assert cfg.relays == ()
    assert cfg.peers == ()
    assert cfg.relay_server is None
    assert cfg.host == HostConfig()


def test_init_creates_private_dirs_and_config(tmp_path: Path) -> None:
    cfg = init_workspace(tmp_path)
    assert (tmp_path / "ultra-agent.toml").is_file()
    for directory in (
        cfg.state_dir,
        cfg.sessions_dir,
        cfg.handoffs_dir,
        cfg.work_dir,
        cfg.vault_dir,
    ):
        assert directory.is_dir()
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert not cfg.db_path.exists()


def test_directories_are_created_only_on_init(tmp_path: Path) -> None:
    _write(tmp_path, config_text())
    cfg = load_config(tmp_path)
    assert not cfg.state_dir.exists()
    assert not cfg.work_dir.exists()
    assert not cfg.vault_dir.exists()


def test_init_does_not_overwrite_existing_config(tmp_path: Path) -> None:
    text = 'poll_seconds = 9.0\nwriting_style = "custom"\n'
    _write(tmp_path, text)
    cfg = init_workspace(tmp_path)
    assert (tmp_path / "ultra-agent.toml").read_text(encoding="utf-8") == text
    assert cfg.poll_seconds == 9.0
    assert cfg.writing_style == "custom"


def test_load_missing_config_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="no configuration"):
        load_config(tmp_path)


def test_malformed_toml_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="invalid TOML"):
        load_config(_write(tmp_path, "command = [\n"))


def test_unknown_top_level_key_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unknown key"):
        load_config(_write(tmp_path, "mystery = 1\n"))


def test_unknown_nested_key_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unknown key"):
        load_config(_write(tmp_path, "[host]\nbogus = 1\n"))


def test_embedded_secret_key_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="embedded secret"):
        load_config(_write(tmp_path, "[[messengers]]\nplatform = \"discord\"\ntoken = \"abc\"\n"))


def test_arbitrary_endpoint_key_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="endpoint"):
        load_config(_write(tmp_path, "[[messengers]]\nplatform = \"discord\"\nbase_url = \"http://x\"\n"))


def test_command_must_be_string_array(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="array of strings"):
        load_config(_write(tmp_path, "command = \"omo\"\n"))


def test_command_must_not_be_empty(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="must not be empty"):
        load_config(_write(tmp_path, "command = []\n"))


def test_argv_is_bounded(tmp_path: Path) -> None:
    long_arg = "a" * (config.MAX_ARG_LENGTH + 1)
    with pytest.raises(ConfigError, match="at most"):
        load_config(_write(tmp_path, f'command = ["omo", "{long_arg}"]\n'))


@pytest.mark.parametrize("value", ["0.0", "-1.0", "inf", "-inf", "nan"])
def test_timeout_must_be_positive_and_finite(tmp_path: Path, value: str) -> None:
    with pytest.raises(ConfigError):
        load_config(_write(tmp_path, f"timeout_seconds = {value}\n"))


def test_poll_must_be_positive(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="positive"):
        load_config(_write(tmp_path, "poll_seconds = 0\n"))


def test_boolean_field_rejects_integer(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="boolean"):
        load_config(
            _write(
                tmp_path,
                "[[schedules]]\nname = \"s\"\nprompt = \"p\"\nevery_seconds = 10\ntrusted = 1\n",
            )
        )


def test_schedule_requires_exactly_one_trigger(tmp_path: Path) -> None:
    both = (
        '[[schedules]]\nname = "s"\nprompt = "p"\nevery_seconds = 10\n'
        'at = "09:00"\ntimezone = "UTC"\n'
    )
    with pytest.raises(ConfigError, match="not both"):
        load_config(_write(tmp_path, both))
    neither = "[[schedules]]\nname = \"s\"\nprompt = \"p\"\n"
    with pytest.raises(ConfigError, match="one of"):
        load_config(_write(tmp_path, neither))


def test_schedule_interval_must_be_positive(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="positive"):
        load_config(
            _write(tmp_path, '[[schedules]]\nname = "s"\nprompt = "p"\nevery_seconds = -5\n')
        )


def test_schedule_daily_rejects_bad_time_and_zone(tmp_path: Path) -> None:
    bad_time = '[[schedules]]\nname = "s"\nprompt = "p"\nat = "9:00"\ntimezone = "UTC"\n'
    with pytest.raises(ConfigError, match="HH:MM"):
        load_config(_write(tmp_path, bad_time))
    bad_zone = (
        '[[schedules]]\nname = "s"\nprompt = "p"\n'
        'at = "09:00"\ntimezone = "Mars/Olympus"\n'
    )
    with pytest.raises(ConfigError, match="IANA time zone"):
        load_config(_write(tmp_path, bad_zone))


def test_schedule_zone_only_with_daily(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="only valid with at"):
        load_config(
            _write(
                tmp_path,
                '[[schedules]]\nname = "s"\nprompt = "p"\n'
                'every_seconds = 10\ntimezone = "UTC"\n',
            )
        )


def test_duplicate_schedule_names_rejected(tmp_path: Path) -> None:
    body = (
        "[[schedules]]\nname = \"dup\"\nprompt = \"p\"\nevery_seconds = 10\n"
        "[[schedules]]\nname = \"dup\"\nprompt = \"q\"\nevery_seconds = 20\n"
    )
    with pytest.raises(ConfigError, match="duplicate schedule name"):
        load_config(_write(tmp_path, body))


def test_schedule_name_is_bounded(tmp_path: Path) -> None:
    long_name = "n" * (config.MAX_NAME_LENGTH + 1)
    with pytest.raises(ConfigError, match="at most"):
        load_config(
            _write(
                tmp_path,
                f'[[schedules]]\nname = "{long_name}"\n'
                'prompt = "p"\nevery_seconds = 10\n',
            )
        )


def test_monitor_requires_exactly_one_source(tmp_path: Path) -> None:
    both = '[[monitors]]\nname = "m"\npath = "/tmp/a"\ncommand = ["tail"]\n'
    with pytest.raises(ConfigError, match="not both"):
        load_config(_write(tmp_path, both))
    neither = '[[monitors]]\nname = "m"\n'
    with pytest.raises(ConfigError, match="one of"):
        load_config(_write(tmp_path, neither))


def test_monitor_command_must_not_be_empty(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="must not be empty"):
        load_config(_write(tmp_path, '[[monitors]]\nname = "m"\ncommand = []\n'))


def test_duplicate_monitor_names_rejected(tmp_path: Path) -> None:
    body = (
        '[[monitors]]\nname = "m"\npath = "/tmp/a"\n'
        '[[monitors]]\nname = "m"\npath = "/tmp/b"\n'
    )
    with pytest.raises(ConfigError, match="duplicate monitor name"):
        load_config(_write(tmp_path, body))


def test_duplicate_messenger_platform_rejected(tmp_path: Path) -> None:
    body = (
        '[[messengers]]\nplatform = "discord"\ntoken_env = "A"\n'
        '[[messengers]]\nplatform = "discord"\ntoken_env = "B"\n'
    )
    with pytest.raises(ConfigError, match="duplicate messenger platform"):
        load_config(_write(tmp_path, body))


def test_messenger_requires_env_token(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="missing required key 'token_env'"):
        load_config(_write(tmp_path, '[[messengers]]\nplatform = "discord"\n'))


def test_env_name_must_be_valid(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="environment variable name"):
        load_config(
            _write(tmp_path, '[[messengers]]\nplatform = "discord"\ntoken_env = "1bad-name"\n')
        )


def test_relay_requires_fields(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="missing required key"):
        load_config(
            _write(
                tmp_path,
                '[[relays]]\nname = "r"\npath = "p.log"\n'
                'platform = "discord"\n',
            )
        )


def test_duplicate_relay_names_rejected(tmp_path: Path) -> None:
    body = (
        '[[relays]]\nname = "r"\npath = "a"\nplatform = "discord"\nchannel = "c"\n'
        '[[relays]]\nname = "r"\npath = "b"\nplatform = "slack"\nchannel = "d"\n'
    )
    with pytest.raises(ConfigError, match="duplicate relay name"):
        load_config(_write(tmp_path, body))


def test_relay_server_bind_must_be_ip(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="IP address"):
        load_config(_write(tmp_path, '[relay_server]\nbind = "not-an-ip"\n'))


def test_relay_server_port_is_bounded(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="at most"):
        load_config(_write(tmp_path, "[relay_server]\nport = 70000\n"))


def test_peer_url_https_or_loopback_http(tmp_path: Path) -> None:
    ok = '[[peers]]\nname = "p"\nurl = "http://127.0.0.1:8765"\nsource = "s"\ntoken_env = "T"\n'
    peer = load_config(_write(tmp_path, ok)).peers[0]
    assert peer.url == "http://127.0.0.1:8765"
    bad = (
        '[[peers]]\nname = "p"\nurl = "http://remote.example:8765"\n'
        'source = "s"\ntoken_env = "T"\n'
    )
    with pytest.raises(ConfigError, match="loopback"):
        load_config(_write(tmp_path, bad))


def test_peer_url_rejects_embedded_credentials(tmp_path: Path) -> None:
    bad = (
        '[[peers]]\nname = "p"\nurl = "https://user:pw@example.invalid"\n'
        'source = "s"\ntoken_env = "T"\n'
    )
    with pytest.raises(ConfigError, match="credentials"):
        load_config(_write(tmp_path, bad))


def test_full_expanded_config_parses(tmp_path: Path) -> None:
    text = """\
command = ["omo", "-p"]
model = "gpt-example"
timeout_seconds = 120.5
poll_seconds = 1.5
tools = ["read", "grep"]
verification_command = ["python", "-c", "print(1)"]
writing_style = "be terse"

[host]
min_memory_percent = 25.0
min_disk_gib = 2.0
max_lanes = 5
heavy_slots = 2
slot_dir = "/tmp/slots"

[[messengers]]
platform = "discord"
token_env = "DISCORD_TOKEN"
users = ["111"]
channels = ["222"]
[[messengers.threads]]
channel_id = "222"
thread_id = "333"

[[schedules]]
name = "every"
prompt = "p"
every_seconds = 60

[[schedules]]
name = "daily"
prompt = "d"
at = "09:30"
timezone = "UTC"
trusted = true

[[monitors]]
name = "log"
path = "/tmp/a.log"
emit_initial = true

[[monitors]]
name = "cmd"
command = ["tail", "-f", "/tmp/a.log"]
timeout_seconds = 5

[[relays]]
name = "progress"
path = "work/status.log"
platform = "slack"
channel = "C1"

[relay_server]
bind = "0.0.0.0"
port = 9000
[[relay_server.sources]]
name = "laptop"
platform = "slack"
channel = "C2"
token_env = "RELAY_TOKEN"

[[peers]]
name = "remote"
url = "https://example.invalid:8765"
source = "desk"
token_env = "PEER_TOKEN"
"""
    cfg = load_config(_write(tmp_path, text))
    assert cfg.command == ("omo", "-p")
    assert cfg.model == "gpt-example"
    assert cfg.timeout_seconds == 120.5
    assert cfg.poll_seconds == 1.5
    assert cfg.tools == ("read", "grep")
    assert cfg.verification_command == ("python", "-c", "print(1)")
    assert cfg.writing_style == "be terse"
    assert cfg.host == HostConfig(
        min_memory_percent=25.0,
        min_disk_gib=2.0,
        max_lanes=5,
        slot_dir=Path("/tmp/slots"),
        heavy_slots=2,
    )
    assert cfg.messengers == (
        MessengerConfig(
            platform="discord",
            token_env="DISCORD_TOKEN",
            users=("111",),
            channels=("222",),
            threads=(ThreadRef(channel_id="222", thread_id="333"),),
        ),
    )
    assert cfg.schedules == (
        Schedule(name="every", prompt="p", every_seconds=60.0),
        Schedule(name="daily", prompt="d", at="09:30", timezone="UTC", trusted=True),
    )
    assert cfg.monitors == (
        Monitor(name="log", path=Path("/tmp/a.log"), emit_initial=True),
        Monitor(name="cmd", command=("tail", "-f", "/tmp/a.log"), timeout_seconds=5.0),
    )
    assert cfg.relays == (
        Relay(name="progress", path=Path("work/status.log"), platform="slack", channel="C1"),
    )
    assert isinstance(cfg.relay_server, RelayServerConfig)
    assert cfg.relay_server.bind == "0.0.0.0"
    assert cfg.relay_server.port == 9000
    assert [s.name for s in cfg.relay_server.sources] == ["laptop"]
    assert cfg.relay_server.sources[0].token_env == "RELAY_TOKEN"
    assert cfg.peers == (
        RelayPeer(
            name="remote",
            url="https://example.invalid:8765",
            source="desk",
            token_env="PEER_TOKEN",
        ),
    )


def test_windows_mount_workspace_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config._reject_windows_mount(Path(tmp_path))
    mounted_workspace = Path("/mnt/c") / "Users" / "example" / "ws"
    with pytest.raises(ConfigError, match="/mnt/c"):
        config._reject_windows_mount(mounted_workspace)
    monkeypatch.setattr(config.os.path, "realpath", lambda _path: str(mounted_workspace))
    with pytest.raises(ConfigError, match="/mnt/c"):
        init_workspace(tmp_path)


def test_blank_argv_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        load_config(_write(tmp_path, 'command = ["   "]\n'))
