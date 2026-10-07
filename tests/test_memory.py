from __future__ import annotations

import re
from pathlib import Path

import pytest

from ultra_agent.memory import Memory

TITLE = "Continuity probe"
SOURCE = "qa:memory-test"
MARKER = "cobalt-7391"


def test_write_reopen_recall_and_get(tmp_path: Path) -> None:
    mem = Memory(tmp_path)
    path = mem.write(TITLE, f"The restart marker is {MARKER}.", SOURCE)
    assert path.is_file()

    reopened = Memory(tmp_path)
    recall = reopened.recall(MARKER)
    assert MARKER in recall
    assert re.search(r'"[^"]*cobalt-7391[^"]*"', recall) is not None

    full = reopened.get(TITLE)
    assert full is not None
    assert MARKER in full


def test_write_rejects_empty_title_or_source(tmp_path: Path) -> None:
    mem = Memory(tmp_path)
    for title, source in [("", SOURCE), ("   ", SOURCE), (TITLE, ""), (TITLE, " \t")]:
        with pytest.raises(ValueError):
            _ = mem.write(title, "body", source)
    assert mem.get(TITLE) is None
    assert list(tmp_path.rglob("*.md")) == []


def test_recall_is_bounded_and_nonempty(tmp_path: Path) -> None:
    mem = Memory(tmp_path)
    body = "sharedtoken " + ("filler words here " * 40)
    for i in range(60):
        _ = mem.write(f"note {i}", f"{body}{i}", SOURCE)
    recall = mem.recall("sharedtoken", limit=60)
    assert 0 < len(recall) <= 8000
    assert len(recall) > 4000


def test_reads_do_not_add_or_remove_notes(tmp_path: Path) -> None:
    mem = Memory(tmp_path)
    _ = mem.write(TITLE, f"marker {MARKER}", SOURCE)
    before = sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*.md"))
    _ = mem.recall(MARKER)
    _ = mem.get(TITLE)
    after = sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*.md"))
    assert before == after


def test_long_query_cannot_overflow_recall_budget(tmp_path: Path) -> None:
    memory = Memory(tmp_path)
    query = "oversized-query " * 1000
    assert len(memory.recall(query)) <= 8000
