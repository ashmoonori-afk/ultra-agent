"""Direct tests for atomic native-session descriptors.

Every test uses a real temporary workspace on disk: it writes a descriptor,
reopens the workspace with a fresh Config and reads it back, and asserts the
exact canonical first line, the 0600 mode, malformed/mismatched refusal and
that invalid state is never silently reset to a fresh session.
"""

from __future__ import annotations

import stat
from pathlib import Path

import pytest

from ultra_agent.models import Config
from ultra_agent.runner_state import (
    SessionStateError,
    load_session,
    save_session,
    session_path,
)

LOCAL = "11111111-2222-4333-8444-555555555555"
NATIVE = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"


def _config(tmp_path: Path) -> Config:
    workspace = tmp_path / "workspace"
    (workspace / "state" / "sessions").mkdir(parents=True)
    return Config(workspace=workspace)


def test_save_writes_canonical_descriptor_and_load_returns_native(tmp_path: Path) -> None:
    config = _config(tmp_path)
    path = save_session(config, LOCAL, "codex", NATIVE)

    assert path == session_path(config, LOCAL)
    assert path.is_file()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    first = path.read_text(encoding="utf-8").splitlines()[0]
    expected = (
        '{"type":"session","id":"' + LOCAL + '","runner":"codex",'
        '"native_session_id":"' + NATIVE + '","version":1}'
    )
    assert first == expected
    assert load_session(config, LOCAL, "codex") == NATIVE


def test_load_absent_descriptor_returns_none(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert load_session(config, LOCAL, "codex") is None


def test_descriptor_survives_reopen(tmp_path: Path) -> None:
    config = _config(tmp_path)
    save_session(config, LOCAL, "claude", NATIVE)
    reopened = Config(workspace=config.workspace)
    assert load_session(reopened, LOCAL, "claude") == NATIVE


def test_save_replaces_atomically(tmp_path: Path) -> None:
    config = _config(tmp_path)
    first = "aaaaaaaa-0000-4000-8000-000000000001"
    second = "aaaaaaaa-0000-4000-8000-000000000002"
    save_session(config, LOCAL, "codex", first)
    save_session(config, LOCAL, "codex", second)
    assert load_session(config, LOCAL, "codex") == second
    leftovers = [
        entry for entry in config.sessions_dir.iterdir()
        if entry.name != f"{LOCAL}.jsonl"
    ]
    assert leftovers == []


def test_save_normalizes_uppercase_native_id(tmp_path: Path) -> None:
    config = _config(tmp_path)
    save_session(config, LOCAL, "codex", NATIVE.upper())
    assert load_session(config, LOCAL, "codex") == NATIVE


@pytest.mark.parametrize("runner", ["omo", "bogus"])
def test_non_native_runner_is_refused(tmp_path: Path, runner: str) -> None:
    config = _config(tmp_path)
    with pytest.raises(SessionStateError):
        save_session(config, LOCAL, runner, NATIVE)  # type: ignore[arg-type]


def test_load_refuses_runner_mismatch(tmp_path: Path) -> None:
    config = _config(tmp_path)
    save_session(config, LOCAL, "codex", NATIVE)
    with pytest.raises(SessionStateError):
        load_session(config, LOCAL, "claude")


def test_load_refuses_local_id_mismatch(tmp_path: Path) -> None:
    config = _config(tmp_path)
    save_session(config, LOCAL, "codex", NATIVE)
    other = "99999999-2222-4333-8444-555555555555"
    session_path(config, LOCAL).rename(session_path(config, other))
    with pytest.raises(SessionStateError):
        load_session(config, other, "codex")


@pytest.mark.parametrize(
    "header",
    [
        "{not json",
        "[]",
        '{"type":"turn","id":"' + LOCAL + '","runner":"codex",'
        '"native_session_id":"' + NATIVE + '","version":1}',
        '{"type":"session","id":"' + LOCAL + '","runner":"codex",'
        '"native_session_id":"' + NATIVE + '","version":2}',
        '{"type":"session","id":"' + LOCAL + '","runner":"bogus",'
        '"native_session_id":"' + NATIVE + '","version":1}',
        '{"type":"session","id":"' + LOCAL + '","runner":"codex",'
        '"native_session_id":"not-a-uuid","version":1}',
        '{"type":"session","id":"not-a-uuid","runner":"codex",'
        '"native_session_id":"' + NATIVE + '","version":1}',
    ],
)
def test_load_refuses_malformed_descriptor(tmp_path: Path, header: str) -> None:
    config = _config(tmp_path)
    path = session_path(config, LOCAL)
    path.write_text(header + "\n", encoding="utf-8")
    with pytest.raises(SessionStateError):
        load_session(config, LOCAL, "codex")
    assert path.read_text(encoding="utf-8").startswith(header)
