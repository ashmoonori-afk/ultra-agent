from __future__ import annotations

import os
from pathlib import Path

import pytest

_ROOT = str(Path(__file__).resolve().parents[1])


@pytest.fixture(autouse=True)
def _checkout_on_child_path(monkeypatch: pytest.MonkeyPatch) -> None:
    # An editable install elsewhere must not shadow the code under test in children.
    existing = os.environ.get("PYTHONPATH")
    monkeypatch.setenv("PYTHONPATH", _ROOT if not existing else f"{_ROOT}{os.pathsep}{existing}")
