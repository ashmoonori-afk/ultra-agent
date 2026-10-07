"""Behavior tests for durable context handoff snapshots.

Every test uses a real SQLite store, a real lane registry and the actual
Mnemosyne vault: no component under test is mocked. Handoff files are reopened
from disk and their embedded JSON is parsed, so the assertions cover the
persisted artifact rather than in-memory state. Failure tests inject a real
disk or memory error and prove the checkpoint and lane session are untouched.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from ultra_agent.handoff import handoff_checkpoint_key, write_handoff
from ultra_agent.memory import Memory
from ultra_agent.models import Config, Submission
from ultra_agent.registry import Registry
from ultra_agent.store import Store

LANE = "lane-a"
OTHER = "lane-b"


def _config(tmp_path: Path) -> Config:
    workspace = Path(os.path.realpath(tmp_path))
    (workspace / "state" / "sessions").mkdir(parents=True, exist_ok=True)
    return Config(
        workspace=workspace,
        command=("omo", "--mode", "json"),
        model="example-model",
        tools=("Bash", "Read"),
        verification_command=("true",),
        writing_style="terse",
        timeout_seconds=120.0,
        poll_seconds=2.0,
    )


def _registry(tmp_path: Path) -> tuple[Registry, Store, Config]:
    config = _config(tmp_path)
    store = Store(config.db_path)
    return Registry(store, config), store, config


def _read_snapshot(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    body = text.split("```json", 1)[1].split("```", 1)[0]
    parsed = json.loads(body)
    assert isinstance(parsed, dict)
    return parsed


def _jobs_by_id(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {job["id"]: job for job in snapshot["jobs"]}


def test_write_handoff_persists_snapshot_provenance_and_checkpoint(tmp_path: Path) -> None:
    registry, store, config = _registry(tmp_path)
    worktrees = (tmp_path / "wt-one", tmp_path / "wt-two")
    lane = registry.launch(
        LANE, "worker", "finish the report", cwd=config.work_dir / LANE, host="host-1",
        worktrees=worktrees,
    )
    awaiting_id = store.enqueue(
        Submission(source_key="awaiting", prompt="needs approval", lane=LANE)
    )
    completed_id = store.enqueue(
        Submission(source_key="done", prompt="already finished", approved=True, lane=LANE)
    )
    assert store.claim() is not None
    store.finish(completed_id, "completed", result="ok")
    running_id = store.enqueue(
        Submission(source_key="running", prompt="long task", approved=True, lane=LANE)
    )
    claimed = store.claim()
    assert claimed is not None and claimed.id == running_id
    other_lane_id = registry.launch(OTHER, "peer", "other work")
    _ = store.enqueue(Submission(source_key="other", prompt="elsewhere", lane=OTHER))

    path = write_handoff(config, store, lane, "compaction failed")

    assert path.is_file()
    assert path.parent == config.handoffs_dir
    assert path.suffix == ".md"
    assert path.stat().st_mode & 0o777 == 0o600

    snapshot = _read_snapshot(path)
    assert snapshot["kind"] == "ultra-agent-handoff"
    assert snapshot["version"] == 1
    assert snapshot["reason"] == "compaction failed"

    lane_block = snapshot["lane"]
    assert lane_block["key"] == LANE
    assert lane_block["role"] == "worker"
    assert lane_block["session_id"] == lane.session_id
    assert lane_block["cwd"] == str(config.work_dir / LANE)
    assert lane_block["host"] == "host-1"
    assert lane_block["done"] == "finish the report"
    assert lane_block["worktrees"] == [str(item) for item in worktrees]
    assert lane_block["status"] == "working"
    assert lane_block["revision"] == lane.revision

    jobs = _jobs_by_id(snapshot)
    assert set(jobs) == {awaiting_id, running_id}
    assert jobs[awaiting_id]["status"] == "awaiting_approval"
    assert jobs[running_id]["status"] == "running"
    for job in jobs.values():
        assert isinstance(job["next_action"], str) and job["next_action"]

    cfg = snapshot["config"]
    assert cfg["command"] == list(config.command)
    assert cfg["tools"] == list(config.tools)
    assert cfg["writing_style"] == "terse"
    assert cfg["model"] == "example-model"
    assert cfg["verification_command"] == list(config.verification_command)

    paths = snapshot["paths"]
    assert paths["workspace"] == str(config.workspace)
    assert paths["vault_dir"] == str(config.vault_dir)
    assert paths["sessions_dir"] == str(config.sessions_dir)
    assert paths["handoffs_dir"] == str(config.handoffs_dir)
    assert paths["db_path"] == str(config.db_path)

    rules = snapshot["rules"]
    assert rules["uncertain_jobs_are_not_replayed"] is True
    assert rules["remote_work_requires_local_approval"] is True
    assert rules["reconciliation_requires_explicit_acknowledgement"] is True

    sessions = snapshot["sessions"]
    assert sessions["current_session_id"] == lane.session_id
    assert sessions["sessions_dir"] == str(config.sessions_dir)
    assert sessions["history"]

    provenance = snapshot["memory"]
    assert provenance["source"]
    assert provenance["title"]
    assert Path(provenance["note_path"]).is_file()
    note = Memory(config.vault_dir).get(provenance["title"])
    assert note is not None
    assert provenance["source"] in note

    assert store.get_checkpoint(handoff_checkpoint_key(LANE)) == str(path)
    assert store.get_checkpoint(handoff_checkpoint_key(OTHER)) is None
    assert store.get_session(LANE) == lane.session_id
    assert other_lane_id is not None


def test_uncertain_job_state_survives_handoff(tmp_path: Path) -> None:
    registry, store, config = _registry(tmp_path)
    lane = registry.launch(LANE, "worker", "finish the report")
    job_id = store.enqueue(Submission(source_key="u", prompt="risky", approved=True, lane=LANE))
    store.claim()
    assert store.recover_interrupted() == 1

    path = write_handoff(config, store, lane, "operator rotation")

    jobs = _jobs_by_id(_read_snapshot(path))
    assert jobs[job_id]["status"] == "uncertain"
    assert jobs[job_id]["next_action"]


def test_handoff_reopens_with_fresh_store_and_registry(tmp_path: Path) -> None:
    registry, store, config = _registry(tmp_path)
    lane = registry.launch(LANE, "worker", "finish the report")
    _ = store.enqueue(Submission(source_key="a", prompt="do a", lane=LANE))
    path = write_handoff(config, store, lane, "compaction failed")

    reopened_store = Store(config.db_path)
    reopened_registry = Registry(reopened_store, config)
    assert reopened_store.get_checkpoint(handoff_checkpoint_key(LANE)) == str(path)
    assert reopened_registry.get(LANE).session_id == lane.session_id
    assert reopened_store.get_session(LANE) == lane.session_id
    assert _read_snapshot(Path(path))["lane"]["session_id"] == lane.session_id


def test_repeated_handoffs_are_unique_and_advance_the_checkpoint(tmp_path: Path) -> None:
    registry, store, config = _registry(tmp_path)
    lane = registry.launch(LANE, "worker", "finish the report")

    first = write_handoff(config, store, lane, "compaction failed")
    second = write_handoff(config, store, lane, "recovery retry")

    assert first != second
    assert first.is_file() and second.is_file()
    assert store.get_checkpoint(handoff_checkpoint_key(LANE)) == str(second)


def test_memory_failure_leaves_checkpoint_and_session_untouched(tmp_path: Path) -> None:
    registry, store, config = _registry(tmp_path)
    lane = registry.launch(LANE, "worker", "finish the report")
    before = store.get_session(LANE)
    config.vault_dir.parent.mkdir(parents=True, exist_ok=True)
    config.vault_dir.write_text("not a vault", encoding="utf-8")

    with pytest.raises(OSError):
        _ = write_handoff(config, store, lane, "compaction failed")

    assert store.get_checkpoint(handoff_checkpoint_key(LANE)) is None
    assert store.get_session(LANE) == before
    assert registry.get(LANE).session_id == lane.session_id
    assert list(config.handoffs_dir.glob("*.md")) == []


def test_disk_failure_leaves_checkpoint_and_session_untouched(tmp_path: Path) -> None:
    registry, store, config = _registry(tmp_path)
    lane = registry.launch(LANE, "worker", "finish the report")
    before = store.get_session(LANE)
    config.handoffs_dir.parent.mkdir(parents=True, exist_ok=True)
    config.handoffs_dir.write_text("blocked", encoding="utf-8")

    with pytest.raises(OSError):
        _ = write_handoff(config, store, lane, "compaction failed")

    assert store.get_checkpoint(handoff_checkpoint_key(LANE)) is None
    assert store.get_session(LANE) == before
    assert registry.get(LANE).session_id == lane.session_id


def test_write_handoff_rejects_empty_reason(tmp_path: Path) -> None:
    registry, store, config = _registry(tmp_path)
    lane = registry.launch(LANE, "worker", "finish the report")
    with pytest.raises(ValueError):
        _ = write_handoff(config, store, lane, "   ")
    assert store.get_checkpoint(handoff_checkpoint_key(LANE)) is None
