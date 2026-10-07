"""End-to-end tests for the authenticated status relay and its file sender.

Every test drives the production receiver through a real ``ThreadingHTTPServer``
bound to 127.0.0.1 and the production sender through a distinct SQLite
workspace, so the actual HTTP path, bearer authentication, transaction boundary
and durable cursor are exercised rather than mocked. Synchronization uses
thread barriers and bounded readiness probes, never fixed sleeps.
"""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import TracebackType
from urllib.parse import urlsplit

import pytest

from ultra_agent.http import HttpClient, TransportError
from ultra_agent.models import Config, JSONValue, RelayPeer, RelayServerConfig, RelaySource
from ultra_agent.relay import (
    STATUS_PATH,
    RelayError,
    create_relay_server,
    push_status_file,
    serve_relay,
)
from ultra_agent.runner_lock import RunnerBusy
from ultra_agent.store import Store


class _ReceiptClient(HttpClient):
    def __init__(self, wrong: bool = False) -> None:
        super().__init__()
        self.wrong = wrong

    def request(self, method, url, headers, payload=None):
        return {
            "receipt": "fixture-receipt",
            "source": "other" if self.wrong else payload["source"],
            "sequence": payload["sequence"],
        }


def test_wrong_receipt_cannot_advance_sender_cursor(tmp_path, monkeypatch):
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    store = Store(tmp_path / "state" / "runtime.db")
    path = tmp_path / "status.txt"
    path.write_text("PROGRESS marker\n")
    peer = RelayPeer("peer", "http://127.0.0.1", "source", _SOURCE_ENV)
    with pytest.raises(RelayError):
        push_status_file(Config(tmp_path), store, peer, path, _ReceiptClient(wrong=True))
    cursor = json.loads(store.get_checkpoint("relay-push:peer"))
    assert cursor["sequence"] == 0 and cursor["offset"] == 0


def test_corrupt_sender_cursor_does_not_reset_sequence(tmp_path, monkeypatch):
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    store = Store(tmp_path / "state" / "runtime.db")
    store.set_checkpoint("relay-push:peer", "not-json")
    path = tmp_path / "status.txt"
    path.write_text("PROGRESS marker\n")
    peer = RelayPeer("peer", "http://127.0.0.1", "source", _SOURCE_ENV)
    with pytest.raises(RelayError):
        push_status_file(Config(tmp_path), store, peer, path, _ReceiptClient())
    assert store.get_checkpoint("relay-push:peer") == "not-json"


def test_sender_excludes_overlapping_peer_pushes(tmp_path, monkeypatch):
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    entered, release = threading.Event(), threading.Event()

    class BlockingClient(_ReceiptClient):
        def request(self, method, url, headers, payload=None):
            entered.set()
            assert release.wait(5)
            return super().request(method, url, headers, payload)

    store = Store(tmp_path / "state" / "runtime.db")
    path = tmp_path / "status.txt"
    path.write_text("PROGRESS marker\n")
    peer = RelayPeer("peer", "http://127.0.0.1", "source", _SOURCE_ENV)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            push_status_file, Config(tmp_path), store, peer, path, BlockingClient()
        )
        try:
            assert entered.wait(5)
            with pytest.raises(RunnerBusy):
                push_status_file(Config(tmp_path), store, peer, path, _ReceiptClient())
        finally:
            release.set()
        assert future.result(timeout=5) == ["fixture-receipt"]

_TOKEN = "relay-fixture-secret"
_SOURCE_ENV = "RELAY_LAPTOP_TOKEN"


def test_prestopped_server_starts_no_watcher(tmp_path: Path) -> None:
    script = (
        "import sys,threading; from pathlib import Path; "
        "from ultra_agent.models import Config,RelayServerConfig; "
        "from ultra_agent.store import Store; "
        "from ultra_agent.relay import serve_relay; "
        "p=Path(sys.argv[1]); stop=threading.Event(); stop.set(); "
        "before=len(threading.enumerate()); "
        "serve_relay(Config(p,relay_server=RelayServerConfig(port=0)),Store(p/'db'),stop); "
        "assert len(threading.enumerate()) == before"
    )
    subprocess.run([sys.executable, "-c", script, str(tmp_path)], check=True, timeout=10)


def _source(name: str = "laptop") -> RelaySource:
    return RelaySource(
        name=name, platform="discord", channel="chan-1", token_env=_SOURCE_ENV
    )


def _config(
    tmp_path: Path,
    sources: list[RelaySource],
    peers: list[RelayPeer] | None = None,
    *,
    port: int = 0,
) -> Config:
    return Config(
        workspace=tmp_path,
        relay_server=RelayServerConfig(bind="127.0.0.1", port=port, sources=tuple(sources)),
        peers=tuple(peers or []),
    )


class _RelayFixture:
    def __init__(self, config: Config, store: Store) -> None:
        self.httpd = create_relay_server(config, store, port=0)
        self.thread = threading.Thread(
            target=self.httpd.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )

    def __enter__(self) -> _RelayFixture:
        self.thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    @property
    def base_url(self) -> str:
        host, port = self.httpd.server_address[0], self.httpd.server_address[1]
        return f"http://{host}:{port}"


def _post(base_url: str, body: dict[str, JSONValue], token: str | None = _TOKEN) -> JSONValue:
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return HttpClient().request("POST", base_url + STATUS_PATH, headers, body)


def _receipt(response: JSONValue) -> str:
    assert isinstance(response, dict)
    value = response["receipt"]
    assert isinstance(value, str)
    return value


def _raw_request(
    base_url: str, body: bytes, headers: dict[str, str]
) -> tuple[int, dict[str, str], bytes]:
    parts = urlsplit(base_url)
    connection = http.client.HTTPConnection(parts.hostname, parts.port, timeout=5)
    try:
        connection.request("POST", STATUS_PATH, body=body, headers=headers)
        response = connection.getresponse()
        data = response.read()
        return response.status, dict(response.getheaders()), data
    finally:
        connection.close()


def _status_line(sequence: int, text: str = "PROGRESS started") -> dict[str, JSONValue]:
    return {"source": "laptop", "sequence": sequence, "text": text}


def _sender_store(tmp_path: Path) -> Store:
    return Store(tmp_path / "sender" / "runtime.db")


def _receiver_store(tmp_path: Path) -> Store:
    return Store(tmp_path / "receiver" / "runtime.db")


def test_accepts_first_sequence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        response = _post(fixture.base_url, _status_line(1, "PROGRESS started"))
    assert _receipt(response)
    deliveries = receiver.deliveries()
    assert [delivery.content for delivery in deliveries] == ["PROGRESS started"]
    assert deliveries[0].platform == "discord"
    assert deliveries[0].channel_id == "chan-1"


def test_unauthenticated_and_unknown_source_are_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        for token in (None, "wrong-token"):
            with pytest.raises(TransportError) as missing:
                _post(fixture.base_url, _status_line(1), token=token)
            assert missing.value.status == 401
            assert _TOKEN not in str(missing.value)
        unknown = {"source": "ghost", "sequence": 1, "text": "PROGRESS hi"}
        with pytest.raises(TransportError) as unknown_error:
            _post(fixture.base_url, unknown)
        assert unknown_error.value.status == 401
        assert _TOKEN not in str(unknown_error.value)
    assert receiver.deliveries() == []


def test_bad_json_and_shape_are_400(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    auth = {"Content-Type": "application/json", "Authorization": f"Bearer {_TOKEN}"}
    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        status, _, _ = _raw_request(fixture.base_url, b"{not json", auth)
        assert status == 400
        with pytest.raises(TransportError) as boolean_sequence:
            _post(fixture.base_url, {"source": "laptop", "sequence": True, "text": "PROGRESS x"})
        assert boolean_sequence.value.status == 400
        with pytest.raises(TransportError) as too_long:
            _post(fixture.base_url, _status_line(1, "PROGRESS " + "x" * 3000))
        assert too_long.value.status == 400
        with pytest.raises(TransportError) as no_marker:
            _post(fixture.base_url, _status_line(1, "just some text"))
        assert no_marker.value.status == 400
    assert receiver.deliveries() == []


def test_oversize_request_is_413(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    body = b'{"source":"laptop","sequence":1,"text":"PROGRESS ' + b"x" * 70000 + b'"}'
    auth = {"Content-Type": "application/json", "Authorization": f"Bearer {_TOKEN}"}
    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        status, _, _ = _raw_request(fixture.base_url, body, auth)
    assert status == 413
    assert receiver.deliveries() == []


def test_exact_duplicate_returns_same_receipt_and_one_delivery(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        first = _receipt(_post(fixture.base_url, _status_line(1, "PROGRESS a")))
        second = _receipt(_post(fixture.base_url, _status_line(1, "PROGRESS a")))
    assert first == second
    assert len(receiver.deliveries()) == 1


def test_altered_payload_and_out_of_order_are_conflicts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        _post(fixture.base_url, _status_line(1, "PROGRESS a"))
        with pytest.raises(TransportError) as altered:
            _post(fixture.base_url, _status_line(1, "PROGRESS b"))
        assert altered.value.status == 409
        with pytest.raises(TransportError) as gap:
            _post(fixture.base_url, _status_line(3, "PROGRESS c"))
        assert gap.value.status == 409
        with pytest.raises(TransportError) as below_one:
            _post(fixture.base_url, _status_line(0, "PROGRESS d"))
        assert below_one.value.status == 400
    assert len(receiver.deliveries()) == 1


def test_concurrent_duplicate_writes_one_delivery(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        barrier = threading.Barrier(2)
        receipts: list[str] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker() -> None:
            barrier.wait()
            try:
                receipt = _receipt(_post(fixture.base_url, _status_line(1, "PROGRESS same")))
            except Exception as exc:
                with lock:
                    errors.append(exc)
                return
            with lock:
                receipts.append(receipt)

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
    assert errors == []
    assert len(set(receipts)) == 1
    assert len(receiver.deliveries()) == 1


def test_lost_response_resends_safely(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    sender = _sender_store(tmp_path)
    status_file = tmp_path / "status.log"
    status_file.write_text("PROGRESS hello\n", encoding="utf-8")

    class _DropFirstResponse(HttpClient):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def request(
            self,
            method: str,
            url: str,
            headers: Mapping[str, str],
            payload: JSONValue | None = None,
        ) -> JSONValue:
            response = super().request(method, url, headers, payload)
            self.calls += 1
            if self.calls == 1:
                raise TransportError("network request outcome uncertain", ambiguous=True)
            return response

    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        peer = RelayPeer(
            name="laptop", url=fixture.base_url, source="laptop", token_env=_SOURCE_ENV
        )
        client = _DropFirstResponse()
        with pytest.raises(TransportError):
            push_status_file(_config(tmp_path, [_source()]), sender, peer, status_file, http=client)
        cursor = json.loads(sender.get_checkpoint("relay-push:laptop") or "{}")
        assert cursor.get("sequence") == 0
        assert len(receiver.deliveries()) == 1

        receipts = push_status_file(
            _config(tmp_path, [_source()]), sender, peer, status_file, http=client
        )
    assert len(receipts) == 1
    assert len(receiver.deliveries()) == 1
    cursor = json.loads(sender.get_checkpoint("relay-push:laptop") or "{}")
    assert cursor.get("sequence") == 1


def test_partial_line_waits_and_cursor_resumes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    sender_path = tmp_path / "sender" / "runtime.db"
    sender = Store(sender_path)
    status_file = tmp_path / "status.log"
    status_file.write_text("PROGRESS one\nQUESTION two", encoding="utf-8")

    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        peer = RelayPeer(
            name="laptop", url=fixture.base_url, source="laptop", token_env=_SOURCE_ENV
        )
        first = push_status_file(_config(tmp_path, [_source()]), sender, peer, status_file)
        assert len(first) == 1
        assert [delivery.content for delivery in receiver.deliveries()] == ["PROGRESS one"]

        status_file.write_text("PROGRESS one\nQUESTION two done\n", encoding="utf-8")
        reopened = Store(sender_path)
        second = push_status_file(_config(tmp_path, [_source()]), reopened, peer, status_file)
    assert len(second) == 1
    assert [delivery.content for delivery in receiver.deliveries()] == [
        "PROGRESS one",
        "QUESTION two done",
    ]
    cursor = json.loads(reopened.get_checkpoint("relay-push:laptop") or "{}")
    assert cursor.get("sequence") == 2


def test_identical_text_at_new_offset_is_new_sequence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    sender = _sender_store(tmp_path)
    status_file = tmp_path / "status.log"
    status_file.write_text("PROGRESS same\nPROGRESS same\n", encoding="utf-8")

    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        peer = RelayPeer(
            name="laptop", url=fixture.base_url, source="laptop", token_env=_SOURCE_ENV
        )
        receipts = push_status_file(_config(tmp_path, [_source()]), sender, peer, status_file)
    assert len(receipts) == 2
    assert len(set(receipts)) == 2
    deliveries = receiver.deliveries()
    assert [delivery.content for delivery in deliveries] == ["PROGRESS same", "PROGRESS same"]
    cursor = json.loads(sender.get_checkpoint("relay-push:laptop") or "{}")
    assert cursor.get("sequence") == 2


def test_rotation_and_truncation_continue_sequence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    sender = _sender_store(tmp_path)
    status_file = tmp_path / "status.log"
    status_file.write_text("PROGRESS one\n", encoding="utf-8")

    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        peer = RelayPeer(
            name="laptop", url=fixture.base_url, source="laptop", token_env=_SOURCE_ENV
        )
        assert len(push_status_file(_config(tmp_path, [_source()]), sender, peer, status_file)) == 1

        rotated = tmp_path / "status.new"
        rotated.write_text("RESULT rotated\n", encoding="utf-8")
        os.replace(rotated, status_file)
        assert len(push_status_file(_config(tmp_path, [_source()]), sender, peer, status_file)) == 1

        status_file.write_text("RESULT tiny\n", encoding="utf-8")
        assert len(push_status_file(_config(tmp_path, [_source()]), sender, peer, status_file)) == 1

    deliveries = receiver.deliveries()
    assert [delivery.content for delivery in deliveries] == [
        "PROGRESS one",
        "RESULT rotated",
        "RESULT tiny",
    ]
    cursor = json.loads(sender.get_checkpoint("relay-push:laptop") or "{}")
    assert cursor.get("sequence") == 3


def test_sender_rejects_unsafe_peer_and_missing_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sender = _sender_store(tmp_path)
    status_file = tmp_path / "status.log"
    status_file.write_text("PROGRESS one\n", encoding="utf-8")
    remote = RelayPeer(
        name="laptop", url="http://remote.example:8765", source="laptop", token_env=_SOURCE_ENV
    )
    with pytest.raises(RelayError):
        push_status_file(_config(tmp_path, [_source()]), sender, remote, status_file)

    monkeypatch.delenv(_SOURCE_ENV, raising=False)
    local = RelayPeer(
        name="laptop", url="http://127.0.0.1:9", source="laptop", token_env=_SOURCE_ENV
    )
    with pytest.raises(RelayError):
        push_status_file(_config(tmp_path, [_source()]), sender, local, status_file)


def test_raw_artifacts_never_expose_the_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    good = {"Content-Type": "application/json", "Authorization": f"Bearer {_TOKEN}"}
    with _RelayFixture(_config(tmp_path, [_source()]), receiver) as fixture:
        status, headers, body = _raw_request(
            fixture.base_url, b'{"source":"laptop","sequence":1,"text":"PROGRESS ok"}', good
        )
        assert status == 201
        assert _TOKEN not in str(headers)
        assert _TOKEN not in body.decode()
        assert "Bearer" not in body.decode()

        bad = {"Content-Type": "application/json", "Authorization": "Bearer wrong-value"}
        status, headers, body = _raw_request(
            fixture.base_url, b'{"source":"laptop","sequence":2,"text":"PROGRESS ok"}', bad
        )
        assert status == 401
        assert _TOKEN not in str(headers)
        assert "wrong-value" not in str(headers)
        assert "wrong-value" not in body.decode()
        assert json.loads(body) == {"error": "unauthorized"}


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _wait_ready(host: str, port: int, deadline_seconds: float = 5.0) -> bool:
    end = time.monotonic() + deadline_seconds
    while time.monotonic() < end:
        with socket.socket() as probe:
            probe.settimeout(0.05)
            if probe.connect_ex((host, port)) == 0:
                return True
    return False


def test_serve_relay_stops_on_event(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(_SOURCE_ENV, _TOKEN)
    receiver = _receiver_store(tmp_path)
    port = _free_port()
    config = _config(tmp_path, [_source()], port=port)
    stop = threading.Event()
    thread = threading.Thread(target=serve_relay, args=(config, receiver, stop), daemon=True)
    thread.start()
    try:
        assert _wait_ready("127.0.0.1", port)
        response = _post(f"http://127.0.0.1:{port}", _status_line(1, "PROGRESS served"))
        assert _receipt(response)
    finally:
        stop.set()
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(receiver.deliveries()) == 1
