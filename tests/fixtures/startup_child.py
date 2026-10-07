"""Real readiness children and a keeper matching the CLI signal contract."""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
from pathlib import Path
from types import FrameType

from ultra_agent.service import keeper
from ultra_agent.startup import READY_FD_ENV, notify_ready


def _interrupt(_number: int, _frame: FrameType | None) -> None:
    raise KeyboardInterrupt


def main() -> int:
    mode = sys.argv[1]
    if mode == "keeper":
        signal.signal(signal.SIGTERM, _interrupt)
        descriptor = int(os.environ.pop(READY_FD_ENV))
        try:
            return keeper(
                (sys.executable, str(Path(__file__).resolve()), *sys.argv[2:]),
                Path.cwd(),
                ready_fd=descriptor,
                wait=(lambda _timeout: False) if sys.argv[2] == "restart" else None,
            )
        except KeyboardInterrupt:
            return 0
    if mode == "restart":
        counter = Path(sys.argv[2])
        first = not counter.exists()
        with counter.open("a", encoding="utf-8") as output:
            output.write(json.dumps({"ready_fd": os.environ.get(READY_FD_ENV)}) + "\n")
        return 1 if first else 0
    descendant: subprocess.Popen[bytes] | None = None
    if mode in ("tree", "stubborn_tree"):
        reader, writer = os.pipe()
        descendant = subprocess.Popen(
            (sys.executable, "-c",
             "import os,signal,sys; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
             "os.write(int(sys.argv[1]),b'1'); os.close(int(sys.argv[1])); signal.pause()",
             str(writer)),
            pass_fds=(writer,),
        )
        os.close(writer)
        assert os.read(reader, 1) == b"1"
        os.close(reader)
    if mode == "stubborn_tree":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.connect(os.environ["STARTUP_EVENTS"])
        connection.sendall(json.dumps({
            "pid": os.getpid(),
            "descendant": descendant.pid if descendant is not None else None,
        }).encode())
        gate = connection.recv(1)
    if gate != b"1":
        return 2
    if mode == "eof":
        os.close(int(os.environ.pop(READY_FD_ENV)))
        return 0
    if mode == "exit":
        return 7
    if mode in ("ready", "tree"):
        notify_ready()
    signal.pause()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
