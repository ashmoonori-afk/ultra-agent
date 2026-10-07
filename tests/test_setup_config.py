"""Setup input collection and private configuration writing tests."""

from __future__ import annotations

import io
import json
import os
import stat
from pathlib import Path

import pytest

from ultra_agent import setup_config, startup
from ultra_agent.config import ConfigError, init_workspace, load_config
from ultra_agent.setup_config import (
    SetupConfigError,
    SetupInputs,
    collect_inputs,
    write_setup,
)

_TOKEN = "tok-secret-1234"


def _env(**over: str | None) -> dict[str, str]:
    base: dict[str, str | None] = {
        "ULTRA_AGENT_PLATFORM": "discord",
        "ULTRA_AGENT_BOT_TOKEN": _TOKEN,
        "ULTRA_AGENT_CHANNEL": "chan-1",
        "ULTRA_AGENT_USER": "user-1",
    }
    base.update(over)
    return {key: value for key, value in base.items() if value is not None}


def _inputs(**over: object) -> SetupInputs:
    fields: dict[str, object] = {
        "platform": "discord",
        "token": _TOKEN,
        "channel": "chan-1",
        "user": "user-1",
        "api_base": None,
    }
    fields.update(over)
    return SetupInputs(**fields)  # type: ignore[arg-type]


def _real(path: Path) -> Path:
    return Path(os.path.realpath(path))


class _FakeTTY:
    """Stand-in terminal: input and prompt output never share a buffer."""

    def __init__(self, answers: str) -> None:
        self._input = io.StringIO(answers)
        self.output = io.StringIO()

    def write(self, text: str) -> int:
        return self.output.write(text)

    def flush(self) -> None:
        pass

    def readline(self) -> str:
        return self._input.readline()


def test_collect_inputs_from_environment() -> None:
    inputs = collect_inputs(_env(), interactive=False, tty=None)
    assert inputs == SetupInputs(
        platform="discord", token=_TOKEN, channel="chan-1", user="user-1", api_base=None
    )
    assert inputs.api_base is None


def test_collect_inputs_reads_optional_api_base() -> None:
    inputs = collect_inputs(
        _env(ULTRA_AGENT_API_BASE="http://127.0.0.1:8080"), interactive=False, tty=None
    )
    assert inputs.api_base == "http://127.0.0.1:8080"


def test_repr_and_str_hide_token() -> None:
    inputs = collect_inputs(_env(), interactive=False, tty=None)
    assert _TOKEN not in repr(inputs)
    assert _TOKEN not in str(inputs)
    assert "chan-1" in repr(inputs)


def test_missing_noninteractive_names_variables_only() -> None:
    with pytest.raises(SetupConfigError) as caught:
        collect_inputs(
            {"ULTRA_AGENT_BOT_TOKEN": _TOKEN}, interactive=False, tty=None
        )
    message = str(caught.value)
    assert "ULTRA_AGENT_PLATFORM" in message
    assert "ULTRA_AGENT_CHANNEL" in message
    assert "ULTRA_AGENT_USER" in message
    assert "ULTRA_AGENT_BOT_TOKEN" not in message
    assert _TOKEN not in message


def test_invalid_token_value_never_reaches_the_message() -> None:
    with pytest.raises(SetupConfigError) as caught:
        collect_inputs(
            _env(ULTRA_AGENT_BOT_TOKEN="bad-token value"), interactive=False, tty=None
        )
    assert "bad-token value" not in str(caught.value)
    assert "whitespace" in str(caught.value)


def test_collect_inputs_prompts_on_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    tty = _FakeTTY("telegram\nchan-9\nuser-9\n")
    monkeypatch.setattr(setup_config.getpass, "getpass", lambda *a, **k: _TOKEN)
    inputs = collect_inputs({}, interactive=True, tty=tty)
    assert (inputs.platform, inputs.channel, inputs.user) == ("telegram", "chan-9", "user-9")
    assert inputs.token == _TOKEN
    prompts = tty.output.getvalue()
    assert "platform" in prompts
    assert "Allowed sender id" in prompts


@pytest.mark.parametrize(
    "over",
    [
        {"ULTRA_AGENT_PLATFORM": "irc"},
        {"ULTRA_AGENT_PLATFORM": "DISCORD"},
        {"ULTRA_AGENT_BOT_TOKEN": ""},
        {"ULTRA_AGENT_BOT_TOKEN": "bad token"},
        {"ULTRA_AGENT_BOT_TOKEN": "line\nbreak"},
        {"ULTRA_AGENT_CHANNEL": ""},
        {"ULTRA_AGENT_CHANNEL": "chan nel"},
        {"ULTRA_AGENT_CHANNEL": "x" * 201},
        {"ULTRA_AGENT_USER": ""},
        {"ULTRA_AGENT_USER": "us er"},
    ],
)
def test_invalid_values_rejected(over: dict[str, str]) -> None:
    with pytest.raises(SetupConfigError):
        collect_inputs(_env(**over), interactive=False, tty=None)


def test_write_setup_generates_parseable_config(tmp_path: Path) -> None:
    workspace = tmp_path / "ws"
    executable = tmp_path / "bin" / "agent"
    config = write_setup(workspace, _inputs(), "codex", executable)
    assert config.workspace == _real(workspace)
    assert config.runner == "codex"
    assert config.command == (str(executable),)
    assert config.tools == ()
    assert config.verification_command == ()
    assert config.writing_style == ""
    messenger = config.messengers[0]
    assert messenger.platform == "discord"
    assert messenger.token_env == "ULTRA_AGENT_DISCORD_TOKEN"
    assert messenger.users == ("user-1",)
    assert messenger.channels == ("chan-1",)
    assert messenger.base_url is None
    text = config.config_path.read_text(encoding="utf-8")
    assert _TOKEN not in text
    assert "ULTRA_AGENT_BOT_TOKEN" not in text
    assert "runner = \"codex\"" in text
    assert "[[messengers]]" in text


def test_write_setup_requires_absolute_executable(tmp_path: Path) -> None:
    with pytest.raises(SetupConfigError):
        write_setup(tmp_path / "ws", _inputs(), "omo", Path("agent"))


def test_write_setup_writes_private_service_env(tmp_path: Path) -> None:
    config = write_setup(tmp_path / "ws", _inputs(), "codex", tmp_path / "agent")
    assert stat.S_IMODE(config.workspace.stat().st_mode) == 0o700
    assert stat.S_IMODE(config.state_dir.stat().st_mode) == 0o700
    path = config.state_dir / "service.env"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert startup.load_service_env(config) == {"ULTRA_AGENT_DISCORD_TOKEN": _TOKEN}


def test_service_env_merges_existing_entries(tmp_path: Path) -> None:
    workspace = _real(tmp_path / "ws")
    config = init_workspace(workspace)
    (config.state_dir / "service.env").write_text(
        f"OTHER={json.dumps('kept')}\n", encoding="utf-8"
    )
    write_setup(workspace, _inputs(), "codex", tmp_path / "agent")
    values = startup.load_service_env(load_config(workspace))
    assert values["OTHER"] == "kept"
    assert values["ULTRA_AGENT_DISCORD_TOKEN"] == _TOKEN


def test_service_env_token_not_duplicated_on_rerun(tmp_path: Path) -> None:
    workspace = _real(tmp_path / "ws")
    write_setup(workspace, _inputs(), "codex", tmp_path / "agent")
    write_setup(workspace, _inputs(), "codex", tmp_path / "agent")
    text = (workspace / "state" / "service.env").read_text(encoding="utf-8")
    assert text.count("ULTRA_AGENT_DISCORD_TOKEN=") == 1


def test_repeated_setup_preserves_existing_configuration(tmp_path: Path) -> None:
    workspace = _real(tmp_path / "ws")
    write_setup(workspace, _inputs(), "codex", tmp_path / "agent")
    config_path = workspace / "ultra-agent.toml"
    before = config_path.read_bytes()
    write_setup(workspace, _inputs(), "claude", tmp_path / "other")
    assert config_path.read_bytes() == before


def test_operator_configuration_is_never_overwritten(tmp_path: Path) -> None:
    workspace = _real(tmp_path / "ws")
    workspace.mkdir(parents=True)
    custom = (
        "# hand written\n"
        'command = ["omo"]\n'
        'runner = "omo"\n'
        "\n"
        "[[messengers]]\n"
        'platform = "slack"\n'
        'token_env = "SLACK_TOKEN"\n'
        'users = ["u"]\n'
        'channels = ["c"]\n'
    )
    (workspace / "ultra-agent.toml").write_text(custom, encoding="utf-8")
    write_setup(workspace, _inputs(platform="slack"), "codex", tmp_path / "agent")
    assert (workspace / "ultra-agent.toml").read_text(encoding="utf-8") == custom
    values = startup.load_service_env(load_config(workspace))
    assert values == {"SLACK_TOKEN": _TOKEN}


def test_rerun_stores_token_under_the_configured_messenger_name(tmp_path: Path) -> None:
    # Given a configured discord workspace whose stored token was removed.
    executable = Path("/usr/bin/true")
    config = write_setup(tmp_path, _inputs(), "codex", executable)
    (config.state_dir / "service.env").unlink()
    # When a rerun names a different platform, then it is refused, not misfiled.
    with pytest.raises(SetupConfigError):
        write_setup(tmp_path, _inputs(platform="telegram"), "codex", executable)
    assert not (config.state_dir / "service.env").exists()
    # And a matching rerun restores the token under the configured variable.
    write_setup(tmp_path, _inputs(), "codex", executable)
    stored = startup.load_service_env(load_config(config.workspace))
    assert stored == {config.messengers[0].token_env: _TOKEN}


def test_loopback_api_base_accepted(tmp_path: Path) -> None:
    inputs = _inputs(api_base="http://127.0.0.1:8080")
    config = write_setup(tmp_path / "ws", inputs, "omo", tmp_path / "agent")
    assert config.messengers[0].base_url == "http://127.0.0.1:8080"


def test_foreign_api_base_rejected(tmp_path: Path) -> None:
    inputs = _inputs(api_base="https://evil.example.com")
    with pytest.raises(ConfigError):
        write_setup(tmp_path / "ws", inputs, "omo", tmp_path / "agent")
