"""Fake Claude Code CLI: records argv+stdin, emits Claude JSON envelopes.

Invoked as ``sys.executable fixture.py <mode> <claude args...>``. One JSON line
with the received argv and stdin is appended to the file named by
``CLAUDE_FIXTURE_LOG``. A real ``--session-id``/``--resume`` value is echoed so
the runner's expected-session check exercises the true wire.
"""

from __future__ import annotations

import json
import os
import sys

FALLBACK_SESSION = "00000000-0000-4000-8000-000000000000"


def _log(prompt: str) -> None:
    path = os.environ.get("CLAUDE_FIXTURE_LOG")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({"argv": sys.argv, "stdin": prompt}) + "\n")


def _session(argv: list[str]) -> str:
    for flag in ("--session-id", "--resume"):
        if flag in argv:
            return argv[argv.index(flag) + 1]
    return FALLBACK_SESSION


def _envelope(session_id: str, *, is_error: bool, result: str, subtype: str) -> dict[str, object]:
    return {
        "type": "result",
        "subtype": subtype,
        "is_error": is_error,
        "result": result,
        "session_id": session_id,
    }


def main() -> None:
    mode = sys.argv[1]
    argv = sys.argv[2:]
    if mode == "slow":
        _log("")
        read_fd, write_fd = os.pipe()
        # The write end stays open, so this read blocks until the parent kills us.
        _ = os.read(read_fd, 1)
        return
    prompt = sys.stdin.read()
    _log(prompt)
    inherited = os.environ.get("CLAUDE_FIXTURE_FD")
    if inherited:
        _ = os.write(int(inherited), b"child-inherited")
    session = _session(argv)
    if mode == "malformed":
        sys.stdout.write("this is not json\n")
        return
    if mode == "wrong_session":
        envelope = _envelope(
            "11111111-1111-4111-8111-111111111111", is_error=False, result="ok",
            subtype="success",
        )
    elif mode in ("context_overflow", "overflow_exit"):
        envelope = _envelope(
            session, is_error=True, result="Prompt is too long: exceeds context window",
            subtype="error_max_turns",
        )
    elif mode == "error":
        envelope = _envelope(
            session, is_error=True, result="the model failed", subtype="error_during_execution",
        )
    elif mode == "empty_result":
        envelope = _envelope(session, is_error=False, result="", subtype="success")
    else:
        envelope = _envelope(session, is_error=False, result="fixture reply", subtype="success")
    print(json.dumps(envelope), flush=True)
    if mode == "trailing_garbage":
        sys.stdout.write("leftover trailing bytes\n")
    if mode == "nonzero_exit":
        sys.exit(2)
    if mode == "overflow_exit":
        sys.exit(1)


if __name__ == "__main__":
    main()
