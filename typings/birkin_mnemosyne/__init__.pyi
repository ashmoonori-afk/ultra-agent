"""Typed subset of the pinned Mnemosyne 0.4.0 API used by ultra-agent."""

from pathlib import Path
from typing import TypedDict

class SearchHit(TypedDict):
    title: str
    snippet: str

class VaultMemory:
    def __init__(self, cfg: dict[str, str | bool] | None = None) -> None: ...
    def write_note(
        self, title: str, body: str, *, source: str | None = None
    ) -> Path: ...
    def get_note(self, title: str) -> str | None: ...
    def search(self, query: str, limit: int = 8) -> list[SearchHit]: ...
