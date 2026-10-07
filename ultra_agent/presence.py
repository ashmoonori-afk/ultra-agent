"""Bounded Discord and Slack presence sessions over ``websocket-client``.

A :class:`PresenceSession` owns exactly one worker thread. The thread opens a
single websocket, performs the platform handshake, then loops on bounded
receives while sending the platform's keepalive frames. :meth:`stop` signals the
loop, closes the socket so an in-flight receive returns, and joins the thread;
the same work happens through the context-manager protocol. No thread or socket
survives a session, including on error.

Two states are reported separately so a caller cannot conflate transport with
visibility:

* :attr:`connected` is true once the socket handshake completed and the
  platform acknowledged the session (Discord ``READY``, Slack ``hello``).
* :attr:`available` is reserved for independently confirmed visible presence.
  Neither a Discord READY nor a Slack hello proves that another user sees the
  account online, so these sessions report connected, not verified availability.

Credentials are read from the environment variables named by
:class:`~ultra_agent.models.MessengerConfig` and never appear in argv, logs or
error text. A missing token or user cookie raises :class:`PresenceAuthError`.
Telegram has no presence implementation and raises
:class:`PresenceUnsupportedError`.
"""

from __future__ import annotations

import json
import logging
import math
import os
import platform
import threading
import time
from typing import Literal, TypeAlias, TypeGuard
from urllib.parse import parse_qsl, urlencode, urlsplit

import websocket

from ultra_agent.http import HttpClient, TransportError
from ultra_agent.models import JSONValue, MessengerConfig

logger = logging.getLogger(__name__)

DISCORD_GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
"""Fixed Discord gateway endpoint; tests may inject a loopback override."""

SLACK_API_BASE_URL = "https://slack.com/api/"
"""Canonical Slack Web API base; every method path is appended to it."""

_OP_DISPATCH = 0
_OP_HEARTBEAT = 1
_OP_IDENTIFY = 2
_OP_RECONNECT = 7
_OP_INVALID_SESSION = 9
_OP_HELLO = 10
_OP_HEARTBEAT_ACK = 11

_DISCORD_INTENTS = 0
_DISCORD_OS = platform.system().lower() or "unknown"

_HANDSHAKE_TIMEOUT_SECONDS = 10.0
_RECV_POLL_SECONDS = 1.0
_SLACK_TICKLE_INTERVAL_SECONDS = 30.0
_STOP_JOIN_TIMEOUT_SECONDS = 5.0
_MAX_RECONNECT_ATTEMPTS = 3
_RECONNECT_BASE_SECONDS = 0.25
_RECONNECT_MAX_SECONDS = 2.0

_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
_SLACK_ORIGIN = "https://app.slack.com"

PresencePhase: TypeAlias = Literal["idle", "connecting", "connected", "stopped", "failed"]
"""Lifecycle phase of a presence session."""


class PresenceError(Exception):
    """Base class for typed presence failures; messages never contain secrets."""


class PresenceUnsupportedError(PresenceError):
    """The requested platform has no presence implementation."""


class PresenceAuthError(PresenceError):
    """A required credential (token or user cookie) is missing or rejected."""


class PresenceConnectionError(PresenceError):
    """The gateway, websocket or Slack endpoint could not be reached."""


class _Reconnect(Exception):
    """Internal signal that the current socket should be re-established."""


class _ConnectFailure(Exception):
    """Internal signal that establishing the socket failed and may be retried."""


def _is_object(value: object) -> TypeGuard[dict[str, object]]:
    """Narrow a decoded JSON value to an object; JSON keys are always strings."""
    return isinstance(value, dict)


def _as_int(value: object) -> int | None:
    """Return ``value`` when it is a genuine integer, never a boolean."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _as_float(value: object) -> float | None:
    """Return ``value`` as a float when it is a genuine number, never a boolean."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


class PresenceSession:
    """One bounded Discord or Slack presence session.

    Construct with a :class:`MessengerConfig` and optionally an injected
    :class:`HttpClient`. ``gateway_url`` overrides the fixed Discord gateway and
    exists only for loopback tests. ``slack_base_url`` defaults to the canonical
    Slack Web API base. Use the context manager or call :meth:`start` and
    :meth:`stop` explicitly.
    """

    def __init__(
        self,
        config: MessengerConfig,
        http: HttpClient | None = None,
        *,
        gateway_url: str | None = None,
        slack_base_url: str = SLACK_API_BASE_URL,
    ) -> None:
        if gateway_url is not None:
            gateway = urlsplit(gateway_url)
            if (
                gateway.username or gateway.password or gateway.fragment
                or not (
                    gateway.scheme in ("ws", "wss")
                    and gateway.hostname in _LOOPBACK_HOSTS
                )
            ):
                raise ValueError("gateway override must be a loopback websocket")
        api = urlsplit(slack_base_url)
        if (
            api.username or api.password or api.query or api.fragment
            or not (
                (api.scheme == "https" and api.hostname == "slack.com")
                or (api.scheme == "http" and api.hostname in _LOOPBACK_HOSTS)
            )
        ):
            raise ValueError("Slack API must use canonical HTTPS or loopback HTTP")
        self._config = config
        self._http = http if http is not None else HttpClient()
        self._gateway_url = gateway_url
        self._slack_base_url = slack_base_url.rstrip("/") + "/"
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._finished = threading.Event()
        self._ws: websocket.WebSocket | None = None
        self._thread: threading.Thread | None = None
        self._phase: PresencePhase = "idle"
        self._connected = False
        self._available = False
        self._error: str | None = None
        self._slack_active = False

    def __enter__(self) -> PresenceSession:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()

    @property
    def phase(self) -> PresencePhase:
        """The current lifecycle phase."""
        with self._lock:
            return self._phase

    @property
    def connected(self) -> bool:
        """True once the socket handshake completed and the session was acked."""
        with self._lock:
            return self._connected

    @property
    def available(self) -> bool:
        """True only when visible availability was independently confirmed."""
        with self._lock:
            return self._available

    @property
    def error(self) -> str | None:
        """A safe failure description, or ``None`` before a failure."""
        with self._lock:
            return self._error

    def start(self) -> None:
        """Validate credentials and launch the single worker thread.

        Raises :class:`PresenceUnsupportedError` for an unsupported platform and
        :class:`PresenceAuthError` when a required credential is missing. Calling
        :meth:`start` on a live session is a no-op.
        """
        if not self._config.enabled:
            raise PresenceError("presence transport is disabled")
        if self._config.platform == "telegram":
            raise PresenceUnsupportedError("telegram presence is not supported")
        if self._config.platform not in ("discord", "slack"):
            raise PresenceUnsupportedError("presence is not supported for this platform")
        token, cookie = self._resolve_credentials()
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._ready.clear()
            self._finished.clear()
            self._phase = "connecting"
            self._error = None
            self._connected = False
            self._available = False
            thread = threading.Thread(
                target=self._run,
                args=(token, cookie),
                name="ultra-agent-presence",
                daemon=True,
            )
            self._thread = thread
        thread.start()

    def stop(self) -> None:
        """Signal the worker, interrupt its receive, and join it. Idempotent."""
        self._stop.set()
        self._interrupt_socket()
        thread = self._thread
        if thread is not None:
            thread.join(
                2 * self._http.timeout + _HANDSHAKE_TIMEOUT_SECONDS + _STOP_JOIN_TIMEOUT_SECONDS
            )
            if thread.is_alive():
                raise PresenceConnectionError("presence worker did not stop")
        with self._lock:
            if self._phase != "failed":
                self._phase = "stopped"
            self._connected = False
            self._available = False
        self._ready.set()

    def wait_ready(self, timeout: float) -> bool:
        """Block until the session is connected or has failed, up to ``timeout``."""
        if not self._ready.wait(timeout):
            return False
        with self._lock:
            return self._connected

    def wait_finished(self, timeout: float | None = None) -> bool:
        """Wait for actual worker teardown, or a caller's bounded hold duration."""
        return self._finished.wait(timeout)

    def _resolve_credentials(self) -> tuple[str, str | None]:
        """Read the named token (and Slack cookie) from the environment, or fail."""
        token = os.environ.get(self._config.token_env, "") if self._config.token_env else ""
        if not token:
            raise PresenceAuthError("presence token is not configured")
        if self._config.platform != "slack":
            return token, None
        cookie_env = self._config.cookie_env
        if not cookie_env:
            raise PresenceAuthError("slack presence requires a configured user cookie")
        cookie = os.environ.get(cookie_env, "")
        if not cookie:
            raise PresenceAuthError("slack presence user cookie is not configured")
        return token, cookie

    def _run(self, token: str, cookie: str | None) -> None:
        attempts = 0
        try:
            while not self._stop.is_set():
                try:
                    if self._config.platform == "discord":
                        self._run_discord(token)
                    else:
                        self._run_slack(token, cookie)
                    return
                except (_Reconnect, _ConnectFailure):
                    attempts += 1
                except PresenceError as exc:
                    if not self._stop.is_set():
                        self._fail(str(exc))
                    return
                except (websocket.WebSocketException, OSError, ValueError):
                    attempts += 1
                except Exception:
                    self._fail("presence session failed")
                    return
                finally:
                    self._close_socket()
                if self._stop.is_set():
                    return
                if attempts > _MAX_RECONNECT_ATTEMPTS:
                    self._fail("presence reconnect limit reached")
                    return
                with self._lock:
                    self._connected = False
                    self._available = False
                    self._phase = "connecting"
                self._ready.clear()
                backoff = min(_RECONNECT_BASE_SECONDS * attempts, _RECONNECT_MAX_SECONDS)
                if self._stop.wait(backoff):
                    return
        finally:
            self._close_socket()
            if self._slack_active and cookie is not None:
                try:
                    self._set_slack_presence(token, cookie, "away")
                except PresenceError:
                    self._fail("slack presence release failed")
                self._slack_active = False
            with self._lock:
                if self._phase != "failed":
                    self._phase = "stopped"
                self._connected = False
                self._available = False

            self._finished.set()

    def _run_discord(self, token: str) -> None:
        url = self._gateway_url or DISCORD_GATEWAY_URL
        ws = self._open(url, _HANDSHAKE_TIMEOUT_SECONDS)
        hello = self._recv_message(ws, _HANDSHAKE_TIMEOUT_SECONDS)
        if _as_int(hello.get("op")) != _OP_HELLO:
            raise PresenceConnectionError("discord gateway did not send hello")
        interval = self._discord_heartbeat_interval(hello) / 1000.0
        self._send(ws, self._discord_identify(token))
        sequence: int | None = None
        awaiting_ack = False
        next_heartbeat = time.monotonic() + interval
        while not self._stop.is_set():
            remaining = next_heartbeat - time.monotonic()
            if remaining <= 0.0:
                if awaiting_ack:
                    raise _Reconnect()
                self._send(ws, {"op": _OP_HEARTBEAT, "d": sequence})
                awaiting_ack = True
                next_heartbeat = time.monotonic() + interval
                continue
            ws.settimeout(min(_RECV_POLL_SECONDS, remaining))
            try:
                message = self._recv_message(ws, None)
            except websocket.WebSocketTimeoutException:
                continue
            except websocket.WebSocketConnectionClosedException:
                raise _Reconnect() from None
            op = _as_int(message.get("op"))
            if op == _OP_HEARTBEAT:
                self._send(ws, {"op": _OP_HEARTBEAT, "d": sequence})
                awaiting_ack = True
                continue
            if op == _OP_HEARTBEAT_ACK:
                awaiting_ack = False
                continue
            if op in (_OP_RECONNECT, _OP_INVALID_SESSION):
                raise _Reconnect()
            if op == _OP_DISPATCH:
                seq = _as_int(message.get("s"))
                if seq is not None:
                    sequence = seq
                if message.get("t") == "READY":
                    self._mark_connected(available=False)

    def _run_slack(self, token: str, cookie: str | None) -> None:
        if cookie is None:
            raise PresenceAuthError("slack presence requires a user cookie")
        if not self._slack_active:
            self._set_slack_presence(token, cookie, "auto")
            self._slack_active = True
        if self._stop.is_set():
            return
        url = self._fetch_slack_ws_url(token, cookie)
        if self._stop.is_set():
            return
        self._validate_slack_url(url)
        ws = self._open(
            url, _HANDSHAKE_TIMEOUT_SECONDS, origin=_SLACK_ORIGIN, cookie=f"d={cookie}"
        )
        try:
            hello = self._recv_message(ws, _HANDSHAKE_TIMEOUT_SECONDS)
            if hello.get("type") != "hello":
                raise PresenceConnectionError("slack websocket did not send hello")
            self._mark_connected(available=False)
            tickle_id = 0
            next_tickle = time.monotonic() + _SLACK_TICKLE_INTERVAL_SECONDS
            while not self._stop.is_set():
                remaining = next_tickle - time.monotonic()
                if remaining <= 0.0:
                    tickle_id += 1
                    self._send(ws, {"type": "tickle", "id": tickle_id})
                    next_tickle = time.monotonic() + _SLACK_TICKLE_INTERVAL_SECONDS
                    continue
                ws.settimeout(min(_RECV_POLL_SECONDS, remaining))
                try:
                    self._recv_message(ws, None)
                except websocket.WebSocketTimeoutException:
                    continue
                except websocket.WebSocketConnectionClosedException:
                    raise _Reconnect() from None
        finally:
            self._close_socket()

    def _open(
        self,
        url: str,
        timeout: float,
        *,
        origin: str | None = None,
        cookie: str | None = None,
    ) -> websocket.WebSocket:
        if self._stop.is_set():
            raise _Reconnect()
        try:
            if origin is None and cookie is None:
                ws = websocket.create_connection(url, timeout=timeout, redirect_limit=0)
            else:
                ws = websocket.create_connection(
                    url, timeout=timeout, origin=origin, cookie=cookie, redirect_limit=0
                )
        except (websocket.WebSocketException, OSError, ValueError):
            raise _ConnectFailure() from None
        with self._lock:
            self._ws = ws
        if self._stop.is_set():
            raise _Reconnect()
        return ws

    def _send(self, ws: websocket.WebSocket, payload: JSONValue) -> None:
        try:
            ws.send(json.dumps(payload))
        except (websocket.WebSocketException, OSError, ValueError, TypeError):
            raise _Reconnect() from None

    def _recv_message(self, ws: websocket.WebSocket, timeout: float | None) -> dict[str, object]:
        if timeout is not None:
            ws.settimeout(timeout)
        raw = ws.recv()
        if raw == "" or raw == b"":
            raise _Reconnect()
        if isinstance(raw, bytes):
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                raise PresenceConnectionError("presence frame is not valid UTF-8") from None
        else:
            text = raw
        try:
            decoded = json.loads(text)
        except (json.JSONDecodeError, ValueError, RecursionError):
            raise PresenceConnectionError("presence frame is not valid JSON") from None
        if not _is_object(decoded):
            raise PresenceConnectionError("presence frame is not an object")
        return decoded

    def _mark_connected(self, *, available: bool) -> None:
        with self._lock:
            self._connected = True
            self._available = available
            self._phase = "connected"
        self._ready.set()

    def _fail(self, message: str) -> None:
        with self._lock:
            self._error = message
            self._phase = "failed"
            self._connected = False
            self._available = False
        self._ready.set()

    def _interrupt_socket(self) -> None:
        with self._lock:
            ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except (websocket.WebSocketException, OSError):
                logger.debug("presence socket close during stop failed")

    def _close_socket(self) -> None:
        with self._lock:
            ws = self._ws
            self._ws = None
        if ws is not None:
            try:
                ws.close()
            except (websocket.WebSocketException, OSError):
                logger.debug("presence socket cleanup failed")

    def _fetch_slack_ws_url(self, token: str, cookie: str) -> str:
        client = self._http
        headers = {
            "Authorization": f"Bearer {token}", "Cookie": f"d={cookie}",
            "Content-Type": "application/json",
        }
        try:
            payload = client.request(
                "POST", f"{self._slack_base_url}client.getWebSocketURL", headers, {}
            )
        except TransportError:
            raise PresenceConnectionError("slack websocket URL request failed") from None
        if not isinstance(payload, dict):
            raise PresenceConnectionError("slack websocket URL response is not an object")
        if payload.get("ok") is not True:
            raise PresenceAuthError("slack websocket URL request was rejected")
        url = payload.get("primary_websocket_url")
        routing = payload.get("routing_context")
        if not isinstance(url, str) or not url or not isinstance(routing, str):
            raise PresenceConnectionError("slack websocket URL response is missing a URL")
        self._validate_slack_url(url)
        parsed = urlsplit(url)
        query = dict(parse_qsl(parsed.query))
        query.update({
            "token": token, "sync_desync": "1", "slack_client": "desktop",
            "start_args": "?agent=client&org_wide_aware=true&connect_only=true&ms_latest=true",
            "no_query_on_subscribe": "1", "flannel": "3", "lazy_channels": "1",
            "gateway_server": routing, "batch_presence_aware": "1",
        })
        return parsed._replace(query=urlencode(query)).geturl()

    def _set_slack_presence(self, token: str, cookie: str, state: str) -> None:
        client = self._http
        headers = {
            "Authorization": f"Bearer {token}", "Cookie": f"d={cookie}",
            "Content-Type": "application/json",
        }
        try:
            response = client.request(
                "POST",
                f"{self._slack_base_url}users.setPresence",
                headers,
                {"presence": state},
            )
        except TransportError:
            raise PresenceConnectionError("slack presence update failed") from None
        if not isinstance(response, dict) or response.get("ok") is not True:
            raise PresenceAuthError("slack presence update was rejected")

    @staticmethod
    def _validate_slack_url(url: str) -> None:
        parts = urlsplit(url)
        if parts.username or parts.password or parts.fragment:
            raise PresenceConnectionError("slack websocket URL contains invalid credentials")
        host = (parts.hostname or "").lower()
        if host in _LOOPBACK_HOSTS and parts.scheme in ("ws", "wss"):
            return
        if parts.scheme != "wss":
            raise PresenceConnectionError("slack websocket URL is not a secure websocket")
        if host == "slack.com" or host.endswith(".slack.com"):
            return
        raise PresenceConnectionError("slack websocket URL host is not a Slack domain")

    @staticmethod
    def _discord_heartbeat_interval(hello: dict[str, object]) -> float:
        data = hello.get("d")
        raw = data.get("heartbeat_interval") if _is_object(data) else None
        interval = _as_float(raw)
        if interval is None or not math.isfinite(interval) or not 0 < interval <= 3_600_000:
            raise PresenceConnectionError("discord hello is missing a heartbeat interval")
        return interval

    @staticmethod
    def _discord_identify(token: str) -> JSONValue:
        return {
            "op": _OP_IDENTIFY,
            "d": {
                "token": token,
                "intents": _DISCORD_INTENTS,
                "properties": {
                    "$os": _DISCORD_OS,
                    "$browser": "ultra-agent",
                    "$device": "ultra-agent",
                },
                "presence": {"status": "online", "afk": False, "since": 0},
            },
        }
