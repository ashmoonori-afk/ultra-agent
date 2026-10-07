"""An inherited advisory lock excludes overlapping workspace executions."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path
from types import TracebackType


class RunnerBusy(RuntimeError):
    """Another runner or its surviving child still owns this workspace."""


class RunnerLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def fileno(self) -> int:
        if self._fd is None:
            raise RunnerBusy("workspace execution lock is not held")
        return self._fd

    def __enter__(self) -> RunnerLock:
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(descriptor)
            raise RunnerBusy("workspace is owned by another runner or child") from exc
        self._fd = descriptor
        return self

    def __exit__(
        self, kind: type[BaseException] | None,
        value: BaseException | None, traceback: TracebackType | None,
    ) -> None:
        descriptor, self._fd = self._fd, None
        if descriptor is not None:
            # No LOCK_UN: a surviving child must retain the shared open-file lock.
            os.close(descriptor)
