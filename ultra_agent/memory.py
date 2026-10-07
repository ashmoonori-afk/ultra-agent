"""Provenance-safe memory adapter over the installed birkin-mnemosyne vault.

This is the only module that imports Mnemosyne. It exposes a tiny, typed API:
write a note with an explicit source, recall a bounded set of quoted snippets,
and fetch one note by title. Recall is rendered as quoted data, never as
instructions or authority, and reads never mutate the vault beyond the
library's own access accounting.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

from birkin_mnemosyne import VaultMemory

MAX_RECALL_CHARS: Final = 8000


def _quote(text: str) -> str:
    escaped = text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
    return f'"{escaped}"'


class Memory:
    """Small provenance-safe facade over a real Mnemosyne vault directory."""

    def __init__(self, vault: Path) -> None:
        self.vault: Path = vault
        self._vault = VaultMemory({"vault_path": str(vault), "evidence_required": True})

    def write(self, title: str, body: str, source: str) -> Path:
        """Create or update a note. Title and source must be non-empty."""
        if not title.strip():
            raise ValueError("memory write requires a non-empty title")
        if not source.strip():
            raise ValueError("memory write requires a non-empty source")
        return self._vault.write_note(title, body, source=source)

    def get(self, title: str) -> str | None:
        """Full stored note text (frontmatter included), or None if absent."""
        return self._vault.get_note(title)

    def recall(self, query: str, limit: int = 5) -> str:
        """Return up to ``limit`` hits for ``query`` as bounded quoted data."""
        hits = self._vault.search(query, limit=limit)
        out = f"recall: {len(hits)} note(s) for {_quote(query[:1024])} (quoted data)"
        for index, hit in enumerate(hits, 1):
            block = f"\n{index}. {_quote(hit['title'])}"
            if hit["snippet"]:
                block += f"\n   {_quote(hit['snippet'])}"
            if len(out) + len(block) > MAX_RECALL_CHARS:
                break
            out += block
        return out
