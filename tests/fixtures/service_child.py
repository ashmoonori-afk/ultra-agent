"""Deterministic child processes used by the service keeper tests.

Modes:

* ``flaky <counter-file>`` -- increment an integer file and exit ``1`` on the
  first run, ``0`` on every later run, so a keeper must restart it once.
* ``signal <unix-socket-path>`` -- connect, send ``ready <pid>`` and block
  forever, so a test can synchronize on connection instead of a sleep and then
  prove the keeper terminated the process group.
"""

from __future__ import annotations

import os
import socket
import sys
import time


def flaky(counter_path: str) -> int:
    count = 0
    if os.path.exists(counter_path):
        with open(counter_path, encoding="utf-8") as handle:
            count = int(handle.read().strip() or "0")
    count += 1
    with open(counter_path, "w", encoding="utf-8") as handle:
        handle.write(str(count))
    return 0 if count >= 2 else 1


def signal_ready(socket_path: str) -> None:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(socket_path)
        client.sendall(f"ready {os.getpid()}\n".encode())
    while True:
        time.sleep(3600)


def main(argv: list[str]) -> int:
    mode = argv[1]
    if mode == "flaky":
        return flaky(argv[2])
    if mode == "signal":
        signal_ready(argv[2])
        return 0
    raise SystemExit(2)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
