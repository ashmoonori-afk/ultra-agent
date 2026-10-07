"""Behavior tests for the crash-honest durable store.

These tests use a real SQLite file in a temporary directory, reopen it with a
fresh Store to simulate a process restart, and drive the exact state machines
the runtime depends on: immutable dedupe, atomic cursor batches, one claim,
explicit uncertain recovery, deterministic chunking and ordered outbox
delivery. No test sleeps; the concurrency case synchronizes on a barrier.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from ultra_agent.models import Job, Submission
from ultra_agent.store import CHUNK_SIZE, Store, StoreError, chunks


def _sub(
    source_key: str,
    prompt: str = "do the thing",
    approved: bool = False,
    channel_id: str | None = "chan-1",
) -> Submission:
    return Submission(
        source_key=source_key,
        prompt=prompt,
        approved=approved,
        channel_id=channel_id,
    )


def _db(tmp_path: Path) -> Path:
    return tmp_path / "runtime.db"


def test_state_survives_reopen(tmp_path: Path) -> None:
    db = _db(tmp_path)
    first = Store(db)
    job_id = first.enqueue(_sub("k1"))
    first.set_checkpoint("discord:chan-1", "42")
    first.set_session("session-abc")

    second = Store(db)
    job = second.get_job(job_id)
    assert job.source_key == "k1"
    assert job.status == "awaiting_approval"
    assert second.get_checkpoint("discord:chan-1") == "42"
    assert second.get_checkpoint("absent") is None
    assert second.get_session() == "session-abc"
    assert second.enqueue(_sub("k1")) == job_id


def test_repeated_source_key_dedupes_and_conflicts(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    first = store.enqueue(_sub("k", prompt="original"))
    assert store.enqueue(_sub("k", prompt="original")) == first
    assert len(store.list_jobs()) == 1

    with pytest.raises(StoreError):
        store.enqueue(_sub("k", prompt="changed"))

    assert store.get_job(first).prompt == "original"
    assert len(store.list_jobs()) == 1


def test_blank_source_key_rejected(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    with pytest.raises(ValueError):
        store.enqueue(_sub("   "))


def test_enqueue_batch_commits_cursor_atomically(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    ids = store.enqueue_batch([_sub("a"), _sub("b")], ("discord:chan-1", "10"))
    assert len(ids) == 2 == len(set(ids))
    assert store.get_checkpoint("discord:chan-1") == "10"

    with pytest.raises(StoreError):
        store.enqueue_batch(
            [_sub("c"), _sub("a", prompt="different")], ("discord:chan-1", "20")
        )

    assert store.get_checkpoint("discord:chan-1") == "10"
    assert {job.source_key for job in store.list_jobs()} == {"a", "b"}


def test_approval_only_moves_awaiting_and_gates_claim(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    job_id = store.enqueue(_sub("k"))
    assert store.claim() is None

    store.approve(job_id)
    assert store.get_job(job_id).status == "queued"
    with pytest.raises(StoreError):
        store.approve(job_id)

    claimed = store.claim()
    assert claimed is not None
    assert claimed.id == job_id
    assert claimed.status == "running"


def test_claim_has_exactly_one_winner(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    store.enqueue(_sub("k", approved=True))
    barrier = threading.Barrier(2)
    results: list[Job | None] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        claimed = store.claim()
        with lock:
            results.append(claimed)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    winners = [job for job in results if job is not None]
    assert len(winners) == 1
    assert store.get_job(winners[0].id).status == "running"


def test_recover_marks_running_uncertain_and_blocks_claim(tmp_path: Path) -> None:
    db = _db(tmp_path)
    store = Store(db)
    store.enqueue(_sub("k", approved=True))
    claimed = store.claim()
    assert claimed is not None

    reopened = Store(db)
    assert reopened.recover_interrupted() == 1
    assert reopened.get_job(claimed.id).status == "uncertain"

    reopened.enqueue(_sub("other", prompt="second", approved=True))
    assert reopened.claim() is None

    reopened.resolve(claimed.id)
    assert reopened.get_job(claimed.id).status == "failed"
    with pytest.raises(StoreError):
        reopened.resolve(claimed.id)

    resumed = reopened.claim()
    assert resumed is not None
    assert resumed.source_key == "other"


def test_finish_inserts_deterministic_chunks_atomically(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    job_id = store.enqueue(_sub("k", approved=True, channel_id="c1"))
    assert store.claim() is not None

    text = "x" * (CHUNK_SIZE * 2 + 5)
    store.finish(job_id, "completed", result=text, verified=True)

    job = store.get_job(job_id)
    assert job.status == "completed"
    assert job.verified is True

    out = store.deliveries()
    assert [delivery.content for delivery in out] == chunks(text)
    assert len(out) == 3
    assert [delivery.ordinal for delivery in out] == [0, 1, 2]
    assert all(delivery.job_id == job_id for delivery in out)
    assert all(delivery.status == "pending" for delivery in out)
    assert all(delivery.channel_id == "c1" for delivery in out)
    assert "".join(delivery.content for delivery in out) == text


def test_finish_local_job_creates_no_delivery(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    job_id = store.enqueue(_sub("k", approved=True, channel_id=None))
    assert store.claim() is not None
    store.finish(job_id, "completed", result="local only")
    assert store.deliveries() == []


def test_finish_requires_running_and_valid_status(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    job_id = store.enqueue(_sub("k", approved=True))
    with pytest.raises(StoreError):
        store.finish(job_id, "completed", result="too early")
    with pytest.raises(ValueError):
        store.finish(job_id, "queued")


def test_ambiguous_chunk_blocks_later_chunks(tmp_path: Path) -> None:
    db = _db(tmp_path)
    store = Store(db)
    job_id = store.enqueue(_sub("k", approved=True, channel_id="c1"))
    assert store.claim() is not None
    store.finish(job_id, "completed", result="a" * (CHUNK_SIZE * 2))

    first, second = store.deliveries()
    assert store.start_delivery(first.id) is True
    assert store.start_delivery(second.id) is False

    assert Store(db).recover_interrupted() == 1
    assert store.get_job(job_id).status == "completed"
    uncertain = [d for d in store.deliveries() if d.id == first.id][0]
    assert uncertain.status == "uncertain"

    assert store.start_delivery(second.id) is False
    store.resolve(first.id)
    assert store.start_delivery(second.id) is True
    store.finish_delivery(second.id, "sent", remote_id="remote-9")

    final = [d for d in store.deliveries() if d.id == second.id][0]
    assert final.status == "sent"
    assert final.remote_id == "remote-9"


def test_retry_links_new_job_and_resend_queues_new_delivery(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    job_id = store.enqueue(_sub("k", approved=True, channel_id="c1"))
    assert store.claim() is not None
    store.finish(job_id, "completed", result="hello")
    original = store.get_job(job_id)

    new_id = store.retry(job_id)
    retried = store.get_job(new_id)
    assert new_id != job_id
    assert retried.retry_of == job_id
    assert retried.status == "awaiting_approval"
    assert retried.approved is False
    assert retried.prompt == original.prompt
    assert retried.source_key != original.source_key
    with pytest.raises(StoreError):
        store.retry(new_id)

    delivery = store.deliveries()[0]
    resend_id = store.resend(delivery.id)
    assert resend_id != delivery.id
    resent = [d for d in store.deliveries() if d.id == resend_id][0]
    assert resent.content == delivery.content
    assert resent.status == "pending"
    assert resent.ordinal == delivery.ordinal + 1
    assert store.get_job(job_id).status == "completed"


def test_add_delivery_dedupes_and_orders_per_job(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    job_id = store.enqueue(_sub("k", approved=True, channel_id="c1"))
    assert store.claim() is not None
    store.finish(job_id, "completed", result="first chunk")

    relay_a = store.add_delivery("discord", "c1", "relay-a", source_key="s1")
    assert store.add_delivery("discord", "c1", "relay-a", source_key="s1") == relay_a
    relay_b = store.add_delivery("discord", "c1", "relay-b", source_key="s2")
    assert store.add_delivery(None, "c1", "extra", job_id=job_id) != relay_b

    with pytest.raises(ValueError):
        store.add_delivery("discord", "   ", "no channel")

    job_chunks = [d for d in store.deliveries() if d.job_id == job_id]
    assert [d.ordinal for d in job_chunks] == [0, 1]
    assert store.start_delivery(relay_a) is True
    assert store.start_delivery(relay_b) is True


def test_delivery_transition_validation(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    with pytest.raises(StoreError):
        store.start_delivery("missing")

    delivery_id = store.add_delivery("telegram", "c1", "hi")
    assert store.start_delivery(delivery_id) is True
    with pytest.raises(StoreError):
        store.start_delivery(delivery_id)
    store.finish_delivery(delivery_id, "sent")
    with pytest.raises(StoreError):
        store.finish_delivery(delivery_id, "sent")
    with pytest.raises(ValueError):
        store.finish_delivery(delivery_id, "pending")


def test_sessions_are_per_lane(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    store.set_session("s-default")
    store.set_session("s-feature", lane="feature")
    store.set_session("s-default-2")
    assert store.get_session() == "s-default-2"
    assert store.get_session("feature") == "s-feature"
    assert store.get_session("missing") is None


def test_connection_context_commits_and_rolls_back(tmp_path: Path) -> None:
    db = _db(tmp_path)
    store = Store(db)
    with store.connection() as connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS probe (id INTEGER PRIMARY KEY, value TEXT)"
        )
        connection.execute("INSERT INTO probe (value) VALUES ('kept')")

    with pytest.raises(RuntimeError):
        with store.connection() as connection:
            connection.execute("INSERT INTO probe (value) VALUES ('dropped')")
            raise RuntimeError("simulated crash")

    with sqlite3.connect(db) as raw:
        values = [row[0] for row in raw.execute("SELECT value FROM probe ORDER BY id")]
    assert values == ["kept"]


def test_finished_reply_preserves_inbound_thread(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    job_id = store.enqueue(Submission(
        "thread:42", "reply", approved=True, channel_id="7",
        reply_to="42", platform="discord",
    ))
    store.claim()
    store.finish(job_id, "completed", result="answer")
    assert store.deliveries()[0].reply_to == "42"


def test_claim_records_the_lane_session_identity(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    store.set_session("session-for-lane", lane="worker")
    job_id = store.enqueue(Submission("claim", "work", approved=True, lane="worker"))
    claimed = store.claim()
    assert claimed is not None
    assert claimed.session_id == "session-for-lane"
    assert Store(_db(tmp_path)).get_job(job_id).session_id == "session-for-lane"


def test_explicit_resend_of_uncertain_chunk_can_proceed(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    job_id = store.enqueue(_sub("uncertain", approved=True))
    store.claim()
    store.finish(job_id, "completed", result="a" * (CHUNK_SIZE + 1))
    first, second = store.deliveries()
    store.start_delivery(first.id)
    store.finish_delivery(first.id, "uncertain")
    resent = store.resend(first.id)
    assert store.start_delivery(resent) is True
    assert store.start_delivery(second.id) is False
    store.finish_delivery(resent, "sent", remote_id="confirmed")
    assert store.start_delivery(second.id) is True


def test_duplicate_source_cannot_change_routing(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    store.enqueue(Submission("same", "prompt", channel_id="one"))
    with pytest.raises(StoreError):
        store.enqueue(Submission("same", "prompt", channel_id="two"))


def test_unicode_reply_chunks_fit_wire_budget_without_losing_text() -> None:
    text = "\U0001F600" * 1000
    result = chunks(text)
    assert "".join(result) == text
    assert all(len(part.encode("utf-16-le")) // 2 <= CHUNK_SIZE for part in result)


def test_definite_rate_limit_wait_survives_restart(tmp_path: Path) -> None:
    store = Store(_db(tmp_path))
    delivery = store.add_delivery("discord", "7", "message")
    assert store.start_delivery(delivery, now=10) is True
    store.defer_delivery(delivery, retry_at=20)
    reopened = Store(_db(tmp_path))
    assert reopened.start_delivery(delivery, now=19) is False
    assert reopened.start_delivery(delivery, now=20) is True
