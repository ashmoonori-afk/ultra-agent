"""Fake Codex CLI for runner_codex tests; executed with the test interpreter.

Invoked as ``sys.executable fixture.py <mode> exec ...`` or
``sys.executable fixture.py <mode> mcp list --json``. Every invocation appends
its argv and stdin to the file named by ``CODEX_FIXTURE_LOG`` so tests can
assert the exact protocol. No network, model or credential file is touched.
"""

from __future__ import annotations

import json
import os
import sys
import uuid


def emit(event: dict[str, object]) -> None:
    print(json.dumps(event), flush=True)


def usage() -> dict[str, object]:
    return {
        "input_tokens": 1,
        "cached_input_tokens": 0,
        "output_tokens": 1,
        "reasoning_output_tokens": 0,
    }


def record(argv: list[str], stdin: str) -> None:
    path = os.environ.get("CODEX_FIXTURE_LOG")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({"argv": argv, "stdin": stdin}) + "\n")


def agent_message(text: str) -> None:
    emit({"type": "item.completed", "item": {"id": "1", "type": "agent_message", "text": text}})


def write_inherited_fd() -> None:
    value = os.environ.get("CODEX_FIXTURE_FD")
    if value:
        _ = os.write(int(value), b"fixture-fd")


def mcp_mode(mode: str) -> None:
    if mode == "servers":
        print(json.dumps([{"name": "alpha"}, {"name": "beta"}]), flush=True)
    elif mode == "bad_name":
        print(json.dumps([{"name": "bad name"}]), flush=True)
    elif mode == "malformed":
        print("not json", flush=True)
    elif mode == "nonzero_exit":
        sys.exit(2)
    else:
        print("[]", flush=True)


def main() -> None:
    mode = sys.argv[1]
    args = sys.argv[2:]
    record(args, sys.stdin.read())
    if args[:1] == ["mcp"]:
        mcp_mode(mode)
        return
    resumed = args[:1] == ["exec"] and "resume" in args
    position = args.index("resume") if resumed else -1
    thread_id = (
        str(uuid.uuid4()) if mode == "mismatch"
        else args[position + 1] if resumed and len(args) > position + 1
        else str(uuid.uuid4())
    )
    if mode == "malformed":
        print("not json", flush=True)
        return
    if mode == "unknown_event":
        emit({"type": "thread.started", "thread_id": thread_id})
        emit({"type": "turn.started"})
        emit({"type": "weird.thing"})
        return
    if mode == "slow":
        read_fd, _write_fd = os.pipe()
        _ = os.read(read_fd, 1)  # blocks: this process holds the write end open
        return
    emit({"type": "thread.started", "thread_id": thread_id})
    emit({"type": "turn.started"})
    if mode == "failed":
        emit({"type": "turn.failed", "error": {"message": "turn exploded"}})
        return
    if mode == "context_overflow":
        emit({"type": "turn.failed", "error": {"message": "exceeded the context window"}})
        return
    if mode == "no_completed":
        agent_message("partial")
        return
    if mode == "error_event":
        agent_message("reply")
        emit({"type": "turn.completed", "usage": usage()})
        emit({"type": "error", "message": "late stream failure"})
        return
    text = "" if mode == "empty_reply" else ("resumed reply" if resumed else "hello")
    agent_message(text)
    emit({"type": "turn.completed", "usage": usage()})
    write_inherited_fd()
    if mode == "nonzero_exit":
        sys.exit(3)


if __name__ == "__main__":
    main()
