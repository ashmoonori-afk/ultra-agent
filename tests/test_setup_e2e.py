"""Clean-HOME end-to-end setup: documented curl | bash, real install, real daemon."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
TOKEN = "e2e-fixture-token-7d41"
BOT_ID = "99"
USER_ID = "1"
CHANNEL = "10"

FAKE_AGENT = """#!{python}
import os, sys
args = sys.argv[1:]
if args == {status}:
    sys.exit(0)
os.execv({python!r}, [{python!r}, {fixture!r}, "ok", *args])
"""


class Discord(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.lock = threading.Lock()
        self.messages: list[dict[str, object]] = []
        self.polls_after_message = 0
        self.polled_twice = threading.Event()
        self.replies: list[dict[str, object]] = []
        self.replied = threading.Event()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.server_address[1]}"


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args: object) -> None:
        return

    def _send(self, status: int, payload: object, raw: bytes | None = None) -> None:
        body = raw if raw is not None else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        server = self.server
        assert isinstance(server, Discord)
        parts = urlsplit(self.path)
        if parts.path == "/install.sh":
            self._send(200, None, (ROOT / "install.sh").read_bytes())
            return
        if self.headers.get("Authorization") != f"Bot {TOKEN}":
            self._send(401, {"message": "unauthorized"})
            return
        if parts.path == "/users/@me":
            self._send(200, {"id": BOT_ID, "bot": True})
            return
        if parts.path == f"/channels/{CHANNEL}/messages":
            before = parse_qs(parts.query).get("before", [None])[0]
            with server.lock:
                page = [
                    m for m in server.messages
                    if before is None or int(str(m["id"])) < int(before)
                ]
                if server.messages:
                    server.polls_after_message += 1
                    if server.polls_after_message >= 2:
                        server.polled_twice.set()
            self._send(200, sorted(page, key=lambda m: int(str(m["id"])), reverse=True))
            return
        self._send(404, {"message": "not found"})

    def do_POST(self) -> None:
        server = self.server
        assert isinstance(server, Discord)
        length = int(self.headers.get("Content-Length") or 0)
        payload = json.loads(self.rfile.read(length) or b"{}")
        if self.headers.get("Authorization") != f"Bot {TOKEN}":
            self._send(401, {"message": "unauthorized"})
            return
        if urlsplit(self.path).path == f"/channels/{CHANNEL}/messages":
            with server.lock:
                server.replies.append(payload)
            self._send(200, {"id": "5000", "channel_id": CHANNEL})
            server.replied.set()
            return
        self._send(404, {"message": "not found"})


@pytest.fixture
def discord() -> Iterator[Discord]:
    server = Discord()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(10)


def _fake_agents(directory: Path, runner: str) -> None:
    directory.mkdir()
    status = '["login", "status"]' if runner == "codex" else '["auth", "status"]'
    script = directory / runner
    script.write_text(FAKE_AGENT.format(
        python=sys.executable, status=status, fixture=str(FIXTURES / f"{runner}.py"),
    ))
    script.chmod(script.stat().st_mode | stat.S_IXUSR)


@pytest.mark.parametrize(("runner", "reply"), [("codex", "hello"), ("claude", "fixture reply")])
def test_one_command_setup_in_clean_home(
    tmp_path: Path, discord: Discord, runner: str, reply: str
) -> None:
    # Given an empty HOME whose PATH has only the selected agent CLI.
    home = tmp_path / "home"
    home.mkdir()
    fakebin = tmp_path / "bin"
    _fake_agents(fakebin, runner)
    workspace = home / "agent"
    log = tmp_path / f"{runner}.log"
    env = {
        "HOME": str(home),
        "PATH": f"{fakebin}{os.pathsep}/usr/bin{os.pathsep}/bin",
        "ULTRA_AGENT_SOURCE": str(ROOT),
        "ULTRA_AGENT_INSTALL_DIR": str(home / ".local/share/ultra-agent"),
        "UA_WORKSPACE": str(workspace),
        "ULTRA_AGENT_NONINTERACTIVE": "1",
        "ULTRA_AGENT_SERVICE": "process",
        "ULTRA_AGENT_PLATFORM": "discord",
        "ULTRA_AGENT_BOT_TOKEN": TOKEN,
        "ULTRA_AGENT_CHANNEL": CHANNEL,
        "ULTRA_AGENT_USER": USER_ID,
        "ULTRA_AGENT_API_BASE": discord.base_url,
        "CODEX_FIXTURE_LOG": str(log),
        "CLAUDE_FIXTURE_LOG": str(log),
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "PIP_CACHE_DIR": os.environ.get("PIP_CACHE_DIR", str(Path.home() / ".cache/pip")),
    }
    cli = home / ".local/share/ultra-agent/venv/bin/ultra-agent"

    def agent(*args: str) -> dict[str, object]:
        done = subprocess.run(
            [str(cli), "--workspace", str(workspace), *args],
            env=env, capture_output=True, text=True, timeout=60, check=True,
        )
        parsed: object = json.loads(done.stdout.strip().splitlines()[-1])
        assert isinstance(parsed, dict)
        return parsed

    # When the documented one-liner runs once.
    install = subprocess.run(
        ["bash", "-c", f"curl -fsSL {discord.base_url}/install.sh | bash"],
        env=env, capture_output=True, text=True, timeout=900, stdin=subprocess.DEVNULL,
    )
    try:
        # Then setup reports a real start with the selected runner.
        assert install.returncode == 0, install.stderr[-3000:]
        started = json.loads(
            next(line for line in install.stdout.splitlines() if line.startswith("{"))
        )
        assert started["ready"] is True and started["runner"] == runner
        assert TOKEN not in install.stdout + install.stderr
        config_text = (workspace / "ultra-agent.toml").read_text()
        assert TOKEN not in config_text and f'runner = "{runner}"' in config_text
        assert stat.S_IMODE((workspace / "state/service.env").stat().st_mode) == 0o600
        assert stat.S_IMODE(workspace.stat().st_mode) == 0o700

        # And an allowed sender's approved message runs the agent and is answered.
        with discord.lock:
            discord.messages.append({
                "id": "2000", "channel_id": CHANNEL, "content": "hi agent",
                "author": {"id": USER_ID, "bot": False},
            })
        assert discord.polled_twice.wait(60), "daemon never polled the new message"
        pending = agent("status")["pending_approval"]
        assert isinstance(pending, list) and len(pending) == 1
        agent("approve", str(pending[0]))
        assert discord.replied.wait(120), "no reply was delivered"
        assert discord.replies[0]["content"] == reply
        assert discord.replies[0]["message_reference"] == {"message_id": "2000"}
        invocations = [json.loads(line) for line in log.read_text().splitlines()]
        assert any("hi agent" in entry["stdin"] for entry in invocations)

        # And a repeat run without inputs keeps the config and the running agent.
        repeat_env = {k: v for k, v in env.items() if k != "ULTRA_AGENT_BOT_TOKEN"}
        repeat = subprocess.run(
            [str(cli), "--workspace", str(workspace), "setup"],
            env=repeat_env, capture_output=True, text=True, timeout=120,
        )
        assert repeat.returncode == 0, repeat.stdout + repeat.stderr
        assert json.loads(repeat.stdout)["already_running"] is True
        assert (workspace / "ultra-agent.toml").read_text() == config_text
    finally:
        stopped = subprocess.run(
            [str(cli), "--workspace", str(workspace), "stop"],
            env=env, capture_output=True, text=True, timeout=60,
        ) if cli.exists() else None
    assert stopped is not None and json.loads(stopped.stdout)["stopped"] is True


def test_rejected_bot_token_never_reports_ready(tmp_path: Path, discord: Discord) -> None:
    # Given a token the messenger API rejects.
    home = tmp_path / "home"
    home.mkdir()
    fakebin = tmp_path / "bin"
    _fake_agents(fakebin, "codex")
    workspace = home / "agent"
    env = {
        "HOME": str(home),
        "PATH": f"{fakebin}{os.pathsep}/usr/bin{os.pathsep}/bin",
        "ULTRA_AGENT_SOURCE": str(ROOT),
        "ULTRA_AGENT_INSTALL_DIR": str(home / ".local/share/ultra-agent"),
        "UA_WORKSPACE": str(workspace),
        "ULTRA_AGENT_NONINTERACTIVE": "1",
        "ULTRA_AGENT_SERVICE": "process",
        "ULTRA_AGENT_PLATFORM": "discord",
        "ULTRA_AGENT_BOT_TOKEN": "wrong-token",
        "ULTRA_AGENT_CHANNEL": CHANNEL,
        "ULTRA_AGENT_USER": USER_ID,
        "ULTRA_AGENT_API_BASE": discord.base_url,
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "PIP_CACHE_DIR": os.environ.get("PIP_CACHE_DIR", str(Path.home() / ".cache/pip")),
    }
    # When the one-liner runs.
    install = subprocess.run(
        ["bash", "-c", f"curl -fsSL {discord.base_url}/install.sh | bash"],
        env=env, capture_output=True, text=True, timeout=900, stdin=subprocess.DEVNULL,
    )
    # Then setup fails, claims no readiness and leaves no agent running.
    assert install.returncode != 0
    assert "setup complete" not in install.stdout
    assert "wrong-token" not in install.stdout + install.stderr
    assert not (workspace / "state" / "background.json").exists()
