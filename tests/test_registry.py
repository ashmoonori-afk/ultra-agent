"""Behavior tests for the durable lane registry and authoritative sync.

The tests use a real SQLite file in a temporary directory, reopen it with a
fresh Store/Registry to simulate a restart, write actual JSONL session headers
and drive the state machines the runtime depends on: exact per-lane sessions,
the fixed objection window, compare-and-swap status transitions, terminal
closure and sync drift detection. No test sleeps or measures time; every
deadline is supplied as an explicit value.
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

import pytest

from ultra_agent.models import Config, Submission
from ultra_agent.registry import (
    CLAIM_SECONDS,
    Lane,
    Registry,
    RegistryError,
    SyncReport,
)
from ultra_agent.store import Store

LANE = "lane-a"
OTHER = "lane-b"


def _config(tmp_path: Path) -> Config:
    workspace = Path(os.path.realpath(tmp_path))
    (workspace / "state" / "sessions").mkdir(parents=True, exist_ok=True)
    return Config(workspace=workspace)


def _registry(tmp_path: Path) -> tuple[Registry, Store, Config]:
    config = _config(tmp_path)
    store = Store(config.db_path)
    return Registry(store, config), store, config


def _header(session_id: str) -> str:
    return json.dumps(
        {
            "type": "session",
            "version": 3,
            "id": session_id,
            "timestamp": "2024-12-03T14:00:00.000Z",
            "cwd": "/tmp/project",
        }
    )


def _write_session(path: Path, session_id: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_header(session_id) + "\n", encoding="utf-8")
    return path


def test_launch_persists_lane_sessions_and_history(tmp_path: Path) -> None:
    registry, store, config = _registry(tmp_path)
    lane = registry.launch(LANE, "worker", "finish the report")

    assert isinstance(lane, Lane)
    assert lane.key == LANE
    assert lane.role == "worker"
    assert lane.done == "finish the report"
    assert lane.host == "local"
    assert lane.cwd == config.work_dir / LANE
    assert lane.status == "working"
    assert lane.revision == 1
    assert lane.session_file is None
    assert lane.worktrees == ()
    assert lane.claim_at is None and lane.deadline is None
    assert lane.claim_emitted is False

    assert store.get_session(LANE) == lane.session_id

    events = registry.history(LANE)
    assert [event.revision for event in events] == [1]
    assert events[0].source == "launch"
    assert events[0].status == "working"

    reopened = Registry(Store(config.db_path), config)
    assert reopened.get(LANE) == lane
    assert reopened.history(LANE) == events


def test_launch_shares_session_table_with_store_claim(tmp_path: Path) -> None:
    registry, store, _ = _registry(tmp_path)
    lane = registry.launch(LANE, "worker", "done criteria")
    store.enqueue(
        Submission(source_key="job-1", prompt="run", approved=True, lane=LANE)
    )
    job = store.claim()
    assert job is not None
    assert job.session_id == lane.session_id


def test_launch_persists_cwd_host_and_worktrees(tmp_path: Path) -> None:
    registry, _, config = _registry(tmp_path)
    lane = registry.launch(
        LANE,
        "worker",
        "done",
        cwd=tmp_path / "custom",
        host="node-1",
        worktrees=(tmp_path / "wt1", tmp_path / "wt2"),
    )
    assert lane.cwd == tmp_path / "custom"
    assert lane.host == "node-1"
    assert lane.worktrees == (tmp_path / "wt1", tmp_path / "wt2")

    reopened = Registry(Store(config.db_path), config)
    assert reopened.get(LANE) == lane


def test_per_lane_session_ids_are_distinct(tmp_path: Path) -> None:
    registry, store, _ = _registry(tmp_path)
    first = registry.launch(LANE, "worker", "one")
    second = registry.launch(OTHER, "reviewer", "two")

    assert first.session_id != second.session_id
    assert store.get_session(LANE) == first.session_id
    assert store.get_session(OTHER) == second.session_id


def test_launch_validates_key_role_done_and_duplicates(tmp_path: Path) -> None:
    registry, _, _ = _registry(tmp_path)
    with pytest.raises(ValueError):
        registry.launch("A", "worker", "done")
    with pytest.raises(ValueError):
        registry.launch("has space", "worker", "done")
    with pytest.raises(ValueError):
        registry.launch("-leading", "worker", "done")
    with pytest.raises(ValueError):
        registry.launch(LANE, "   ", "done")
    with pytest.raises(ValueError):
        registry.launch(LANE, "worker", "   ")

    registry.launch(LANE, "worker", "done")
    with pytest.raises(RegistryError):
        registry.launch(LANE, "worker", "done")

    with pytest.raises(RegistryError):
        registry.get("missing-lane")


def test_claim_window_is_exact_and_not_extended(tmp_path: Path) -> None:
    registry, _, _ = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")

    claimed = registry.claim(LANE, "tests pass", 1000.0)
    assert claimed.status == "done-claimed"
    assert claimed.claim_at == 1000.0
    assert claimed.deadline == 1000.0 + CLAIM_SECONDS
    assert claimed.evidence == "tests pass"
    assert claimed.revision == 2

    repeated = registry.claim(LANE, "tests pass", 1100.0)
    assert repeated == claimed
    assert repeated.deadline == 1300.0

    renewed = registry.claim(LANE, "still done", 1300.0)
    assert renewed.revision == 3
    assert renewed.claim_at == 1300.0
    assert renewed.deadline == 1600.0
    assert renewed.claim_emitted is False


def test_mark_claim_emitted_is_once_only(tmp_path: Path) -> None:
    registry, _, _ = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")
    claimed = registry.claim(LANE, "evidence", 1000.0)

    emitted = registry.mark_claim_emitted(LANE)
    assert emitted.claim_emitted is True
    assert emitted.revision == claimed.revision + 1
    assert registry.mark_claim_emitted(LANE) == emitted


def test_generic_transition_cannot_skip_claim_window(tmp_path: Path) -> None:
    registry, _, _ = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")
    with pytest.raises(RegistryError):
        registry.transition(LANE, "done-claimed", "claimed", 100)


def test_adoption_updates_execution_session_mapping(tmp_path: Path) -> None:
    registry, store, config = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")
    path = _write_session(config.sessions_dir / "adopted.jsonl", "adopted-session")
    registry.adopt(LANE, path)
    assert store.get_session(LANE) == "adopted-session"


def test_sync_uses_authoritative_header_not_filename_guess(tmp_path: Path) -> None:
    registry, _, config = _registry(tmp_path)
    lane = registry.launch(LANE, "worker", "done")
    path = _write_session(
        config.sessions_dir / f"timestamp_{lane.session_id}.jsonl", lane.session_id
    )
    report = registry.sync(write=True)
    assert not [finding for finding in report.findings if finding.key == LANE]
    assert registry.get(LANE).session_file == path


def test_rotation_updates_both_bindings_without_replaying_uncertain_work(tmp_path: Path) -> None:
    registry, store, _ = _registry(tmp_path)
    old = registry.launch(LANE, "worker", "done")
    job_id = store.enqueue(Submission("turn", "work", approved=True, lane=LANE))
    store.claim()
    with pytest.raises(RegistryError):
        registry.rotate(LANE, "fresh-session", 100, "handoff")
    store.finish(job_id, "uncertain", error="compaction failed")
    rotated = registry.rotate(LANE, "fresh-session", 101, "durable handoff")
    assert rotated.session_id != old.session_id
    assert store.get_session(LANE) == rotated.session_id
    assert store.get_job(job_id).status == "uncertain"
    assert store.get_job(job_id).session_id == old.session_id
    assert store.claim() is None


def test_sync_rechecks_claim_state_in_its_write_transaction(tmp_path, monkeypatch):
    from ultra_agent import registry as module

    registry, store, config = _registry(tmp_path)
    original = registry.launch(LANE, "worker", "done")
    path = _write_session(config.sessions_dir / "owned.jsonl", original.session_id)
    registry.adopt(LANE, path)
    identifier = store.enqueue(Submission("queued", "work", approved=True, lane=LANE))
    changed = "11111111-2222-4333-8444-555555555555"
    _write_session(path, changed)
    read_header = module._read_header_or_none

    def interleaved(path):
        store.claim()
        return read_header(path)

    monkeypatch.setattr(module, "_read_header_or_none", interleaved)
    report = registry.sync(write=True)
    assert report.repairs == ()
    assert store.get_job(identifier).status == "running"
    assert registry.get(LANE).session_id == original.session_id


def test_objection_sets_not_done_and_clears_claim(tmp_path: Path) -> None:
    registry, _, _ = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")
    registry.claim(LANE, "premature", 1000.0)

    objected = registry.object(LANE, ["not merged", "   "], 1010.0)
    assert objected.status == "not-done"
    assert objected.claim_at is None and objected.deadline is None
    assert objected.evidence == ""
    assert objected.claim_emitted is False

    events = registry.history(LANE)
    assert [event.status for event in events] == ["working", "done-claimed", "not-done"]
    assert events[-1].note == "not merged"
    assert events[-1].source == "object"

    with pytest.raises(ValueError):
        registry.object(LANE, ["   "], 1020.0)


def test_closed_is_terminal(tmp_path: Path) -> None:
    registry, _, _ = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")
    closed = registry.transition(LANE, "closed", "verified", 1000.0)
    assert closed.status == "closed"

    with pytest.raises(RegistryError):
        registry.transition(LANE, "working", "reopen", 1001.0)
    with pytest.raises(RegistryError):
        registry.claim(LANE, "more", 1002.0)
    with pytest.raises(RegistryError):
        registry.object(LANE, ["no"], 1003.0)
    with pytest.raises(RegistryError):
        registry.mark_claim_emitted(LANE)
    with pytest.raises(RegistryError):
        registry.adopt(LANE, tmp_path / "missing.jsonl")


def test_stale_revision_cannot_close_after_objection(tmp_path: Path) -> None:
    registry, _, _ = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")
    claimed = registry.claim(LANE, "evidence", 1000.0)
    assert claimed.revision == 2

    registry.object(LANE, ["not actually done"], 1010.0)

    with pytest.raises(RegistryError) as excinfo:
        registry.transition(LANE, "closed", "old checker", 1020.0, expected_revision=2)
    assert "stale revision" in str(excinfo.value)
    assert registry.get(LANE).status == "not-done"


def test_objection_and_closure_race_is_atomic(tmp_path: Path) -> None:
    registry, _, _ = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")
    registry.claim(LANE, "evidence", 1000.0)

    barrier = threading.Barrier(2)
    outcome: list[str] = []
    lock = threading.Lock()

    def objecting() -> None:
        barrier.wait()
        try:
            registry.object(LANE, ["stop"], 1001.0)
            with lock:
                outcome.append("object")
        except RegistryError:
            with lock:
                outcome.append("object-refused")

    def closing() -> None:
        barrier.wait()
        try:
            registry.transition(LANE, "closed", "checker", 1001.0, expected_revision=2)
            with lock:
                outcome.append("close")
        except RegistryError:
            with lock:
                outcome.append("close-refused")

    threads = [threading.Thread(target=objecting), threading.Thread(target=closing)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(outcome) == 2
    winners = [entry for entry in outcome if not entry.endswith("refused")]
    assert len(winners) == 1
    final = registry.get(LANE)
    if winners == ["close"]:
        assert final.status == "closed"
    else:
        assert final.status == "not-done"


def test_list_open_only_excludes_closed(tmp_path: Path) -> None:
    registry, _, _ = _registry(tmp_path)
    registry.launch(LANE, "worker", "one")
    registry.launch(OTHER, "reviewer", "two")
    registry.transition(OTHER, "closed", "done", 1000.0)

    assert {lane.key for lane in registry.list()} == {LANE, OTHER}
    assert [lane.key for lane in registry.list(open_only=True)] == [LANE]


def test_adopt_binds_authoritative_header_and_refuses_conflict(tmp_path: Path) -> None:
    registry, _, config = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")
    registry.launch(OTHER, "reviewer", "other done")

    session_id = "11111111-2222-3333-4444-555555555555"
    path = _write_session(config.sessions_dir / f"{session_id}.jsonl", session_id)
    adopted = registry.adopt(LANE, path)
    assert adopted.session_file == path
    assert adopted.session_id == session_id
    assert adopted.revision == 2

    with pytest.raises(RegistryError):
        registry.adopt(OTHER, path)

    with pytest.raises(RegistryError):
        registry.adopt(LANE, config.sessions_dir / "absent.jsonl")


def test_sync_reports_pending_for_unexecuted_lane(tmp_path: Path) -> None:
    registry, _, _ = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")

    report = registry.sync()
    assert isinstance(report, SyncReport)
    assert report.written is False
    kinds = {(finding.kind, finding.key) for finding in report.findings}
    assert ("pending", LANE) in kinds
    assert all(finding.kind != "missing" for finding in report.findings)
    assert report.repairs == ()


def test_sync_reports_missing_file_for_claimed_lane(tmp_path: Path) -> None:
    registry, store, _ = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")
    store.enqueue(Submission(source_key="j", prompt="run", approved=True, lane=LANE))
    assert store.claim() is not None

    report = registry.sync()
    missing = [f for f in report.findings if f.kind == "missing"]
    assert [f.key for f in missing] == [LANE]


def test_sync_detects_changed_header_and_repairs_with_write(tmp_path: Path) -> None:
    registry, _, config = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")

    first = "11111111-1111-1111-1111-111111111111"
    second = "99999999-9999-9999-9999-999999999999"
    path = _write_session(config.sessions_dir / f"{first}.jsonl", first)
    registry.adopt(LANE, path)
    _write_session(path, second)

    dry = registry.sync()
    changed = [f for f in dry.findings if f.kind == "changed-header"]
    assert [f.key for f in changed] == [LANE]
    assert dry.repairs == ()
    assert registry.get(LANE).session_id == first

    written = registry.sync(write=True)
    assert written.written is True
    assert len(written.repairs) == 1
    repair = written.repairs[0]
    assert repair.key == LANE
    assert repair.session_id == second
    assert repair.source == "sync"
    assert repair.revision == 3

    lane = registry.get(LANE)
    assert lane.session_id == second
    assert lane.revision == 3
    assert registry.history(LANE)[-1].source == "sync"


def test_sync_write_refuses_while_job_running(tmp_path: Path) -> None:
    registry, store, config = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")

    first = "11111111-1111-1111-1111-111111111111"
    second = "99999999-9999-9999-9999-999999999999"
    path = _write_session(config.sessions_dir / f"{first}.jsonl", first)
    registry.adopt(LANE, path)
    _write_session(path, second)

    store.enqueue(Submission(source_key="j", prompt="run", approved=True, lane=LANE))
    job = store.claim()
    assert job is not None and job.status == "running"

    report = registry.sync(write=True)
    assert any(f.kind == "changed-header" for f in report.findings)
    assert report.repairs == ()
    assert registry.get(LANE).session_id == first


def test_sync_never_adopts_unregistered_file(tmp_path: Path) -> None:
    registry, _, config = _registry(tmp_path)
    registry.launch(LANE, "worker", "done")
    stray = _write_session(
        config.sessions_dir / "stray.jsonl", "77777777-7777-7777-7777-777777777777"
    )

    report = registry.sync()
    unregistered = [f for f in report.findings if f.kind == "unregistered-file"]
    assert [f.path for f in unregistered] == [stray]

    registry.sync(write=True)
    assert registry.get(LANE).session_file is None
    assert registry.get(LANE).session_id != "77777777-7777-7777-7777-777777777777"


def test_sync_reports_missing_done_criteria(tmp_path: Path) -> None:
    registry, store, _ = _registry(tmp_path)
    with store.connection() as connection:
        connection.execute(
            """
            INSERT INTO lanes (
                key, role, cwd, host, session_id, session_file, done, worktrees,
                status, revision, claim_at, deadline, evidence, claim_emitted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-lane",
                "worker",
                "/tmp/work",
                "local",
                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                None,
                "",
                "[]",
                "working",
                1,
                None,
                None,
                "",
                0,
            ),
        )

    report = registry.sync()
    missing_done = [f for f in report.findings if f.kind == "missing-done"]
    assert [f.key for f in missing_done] == ["legacy-lane"]
