"""Original JSON-wire fixture, executed with the current test interpreter."""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
from pathlib import Path


def emit(event: dict[str, object]) -> None:
    print(json.dumps(event), flush=True)


def message(text: str, **extra: object) -> None:
    emit({
        "type": "message_end",
        "message": {
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "private reasoning"},
                {"type": "text", "text": text},
            ],
            "api": "fixture", "provider": "fixture", "model": "fixture",
            "usage": {
                "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 0,
                "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0},
            },
            "timestamp": 0, "stopReason": "stop",
            **extra,
        },
    })


def main() -> None:
    mode = sys.argv[1]
    if mode == "child":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        with socket.socket(socket.AF_UNIX) as connection:
            connection.connect(sys.argv[2])
            connection.sendall(f"{os.getpid()}\n".encode())
            _ = connection.recv(1)
            _ = os.write(int(sys.argv[3]), b"ready")
        while True:
            signal.pause()
    if mode == "backpressure":
        sys.stderr.write("e" * (2 * 1024 * 1024))
        sys.stderr.flush()
    prompt = sys.stdin.read()
    if mode == "command":
        print(json.dumps({"argv": sys.argv[2:], "cwd": os.getcwd(), "env": dict(os.environ)}))
        return
    requested_id = (
        sys.argv[sys.argv.index("--session-id") + 1]
        if "--session-id" in sys.argv else "fixture"
    )
    if mode != "no_session":
        emit({
            "type": "session", "version": 3,
            "id": "different" if mode == "wrong_session" else requested_id,
            "timestamp": "", "cwd": os.getcwd(),
        })
    if mode == "observed_events":
        emit({"type": "settings_source_selected", "scope": "workspace"})
        emit({"type": "entry_appended", "entry": {"type": "message"}})
    emit({"type": "agent_start"})
    if mode == "recovered":
        message("temporary error", stopReason="error")
        emit({"type": "agent_end", "messages": [], "willRetry": True})
        emit({"type": "agent_start"})
    if mode == "cancelled_compaction":
        emit({
            "type": "compaction_end", "reason": "manual", "aborted": True,
            "accepted": False, "rejectionCause": "cancelled-by-extension",
            "willRetry": False,
        })
    if mode in ("timeout", "linger", "child_error", "child_huge"):
        read_fd, write_fd = os.pipe()
        try:
            child = subprocess.Popen(
                (
                    sys.executable, str(Path(__file__).resolve()), "child",
                    os.environ["FIXTURE_SOCKET"], str(write_fd),
                ),
                pass_fds=(write_fd,),
            )
            os.close(write_fd)
            _ = os.read(read_fd, 5)
        finally:
            os.close(read_fd)
        if mode == "timeout":
            _ = child.wait()
        if mode == "child_huge":
            print("x" * (5 * 1024 * 1024), flush=True)
        if mode == "child_error":
            sys.exit(2)
        message("leader finished")
        emit({"type": "agent_end", "messages": [], "willRetry": False})
        return
    if mode in ("huge", "huge_stderr"):
        stream = sys.stdout if mode == "huge" else sys.stderr
        stream.write("x" * (5 * 1024 * 1024))
        stream.flush()
        return
    if mode == "noise":
        print("not JSON", flush=True)
    if mode == "jsonnoise":
        emit({"type": "unexpected-log", "text": "success"})
    if mode in ("compaction_failed", "normal", "external-owner", "retry_compaction"):
        emit({
            "type": "compaction_end", "reason": "threshold", "aborted": False,
            "willRetry": mode == "retry_compaction",
            **({"errorMessage": "failure"} if mode != "normal" else {"result": {}}),
            **({"rejectionCause": "external-owner"} if mode == "external-owner" else {}),
        })
    if mode != "missingusage":
        emit({
            "type": "message_update", "usage": {"input": 999999},
            "assistantMessageEvent": {
                "type": "text_delta", "contentIndex": 0, "delta": "untrusted",
            },
        })
    emit({
        "type": "message_end",
        "message": {"role": "toolResult", "content": [{"type": "text", "text": "tool output"}]},
    })
    if mode != "tooltext":
        message("earlier")
        text = "last authoritative text"
        if mode == "echo":
            text = json.dumps({
                "prompt": prompt, "argv": sys.argv[2:],
                "cwd": os.getcwd(), "env": dict(os.environ),
            })
        if mode == "runtime":
            directory = Path(sys.argv[sys.argv.index("--session-dir") + 1])
            session = directory / f"{requested_id}.jsonl"
            if not session.exists():
                session.write_text(
                    json.dumps({"type": "session", "id": requested_id}) + "\n",
                    encoding="utf-8",
                )
            text = json.dumps({
                "prompt": prompt, "cwd": os.getcwd(), "session_id": requested_id,
            })
        if mode == "empty":
            text = ""
        if mode == "backpressure":
            text = str(len(prompt))
        message(text, **({"stopReason": mode} if mode in ("error", "aborted") else {}),
                **({"errorMessage": "failure"} if mode == "error_message" else {}))
    if mode != "noagentend":
        emit({
            "type": "agent_end", "messages": [],
            **({"willRetry": mode == "retry"} if mode != "missing_retry" else {}),
        })
    if mode == "exit_error":
        sys.exit(3)
    if mode == "observed_events":
        emit({"type": "agent_settled"})
        emit({"type": "agent_idle"})


if __name__ == "__main__":
    main()
