"""Discord REST transport with a durable per-channel history cursor.

This adapter is the Discord wire boundary. It confirms its own bot identity,
polls configured channels for new messages, and sends, edits or marks typing on
outbound messages. It owns no runtime policy: approval, delivery state and retry
scheduling stay in the store and the runtime, and the caller inspects the shared
:class:`~ultra_agent.http.TransportError` (``status``, ``retry_after``,
``ambiguous``) to decide what to do next.

Two properties matter most here. First, the bot token is read only from the
environment variable named by the messenger configuration and is placed only in
the ``Authorization`` header of a request that :class:`HttpClient` builds; no
failure path echoes it, so a Discord token cannot leak through an exception.
Second, inbound progress is a durable per-channel cursor: a first poll records
the newest visible message id as a baseline without enqueuing any history, and
later polls walk newest-first pages backward until the saved id is reached and
then advance the cursor in the same transaction that inserts the accepted
messages. A backlog that would need more than ``MAX_HISTORY_PAGES`` pages fails
visibly instead of silently truncating or advancing the cursor.

Message parsing is strict on identity: only configured channels and users are
accepted, and bot, webhook, self-authored and empty messages never become jobs.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import TypeGuard
from urllib.parse import urlsplit

from ultra_agent.http import HttpClient, TransportError
from ultra_agent.models import Delivery, JSONValue, MessengerConfig, Submission
from ultra_agent.store import Store

DEFAULT_BASE_URL = "https://discord.com/api/v10"
"""Canonical Discord API base; the only non-loopback endpoint accepted."""

CURSOR_PREFIX = "discord:cursor:"
"""Durable checkpoint key prefix holding one channel's newest message id."""

PAGE_LIMIT = 100
"""Messages requested per page; a full page means older messages remain."""

MAX_HISTORY_PAGES = 100
"""Hard cap on pages walked while catching up; exceeding it fails visibly."""

EMPTY_BASELINE = "0"
"""Baseline recorded for an empty channel so the next message is not swallowed."""

USER_AGENT = "DiscordBot (https://github.com/ashmoonori-afk/ultra-agent, 0.1.0)"
"""Fixed User-Agent Discord requires; carries no credentials."""

_DISCORD_HOST = "discord.com"
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


def _is_object(value: object) -> TypeGuard[dict[str, JSONValue]]:
    """Narrow a decoded JSON value to an object with string keys."""
    return isinstance(value, dict)


def _is_array(value: object) -> TypeGuard[list[JSONValue]]:
    """Narrow a decoded JSON value to an array."""
    return isinstance(value, list)


def _snowflake(value: JSONValue) -> str | None:
    """Return a Discord identifier as written, or ``None`` when it is not one.

    Discord renders every identifier as an ASCII decimal string; integers,
    booleans and other shapes are refused rather than coerced.
    """
    if isinstance(value, str) and value.isascii() and value.isdigit():
        return value
    return None


def _identifier(container: dict[str, JSONValue], key: str, what: str) -> str:
    """Read a required identifier field, or raise a safe, redacted error."""
    value = _snowflake(container.get(key))
    if value is None:
        raise TransportError(f"discord {what} has no valid identifier")
    return value


def _validate_base_url(url: str) -> str:
    """Accept only the canonical HTTPS API or an explicit loopback injection."""
    parts = urlsplit(url.strip())
    if parts.username or parts.password or parts.query or parts.fragment:
        raise ValueError("base_url must not embed credentials, a query or a fragment")
    host = parts.hostname
    if not host:
        raise ValueError("base_url must be an absolute URL with a host")
    if parts.scheme == "https" and host == _DISCORD_HOST:
        return url.strip().rstrip("/")
    if parts.scheme == "http" and host in _LOOPBACK_HOSTS:
        return url.strip().rstrip("/")
    raise ValueError("base_url must be https://discord.com or an http loopback address")


class DiscordTransport:
    """A bounded Discord REST adapter over an injected HTTP boundary.

    ``config`` supplies the ``discord`` messenger entry (token environment
    variable and allowlists); ``store`` supplies the durable per-channel cursor
    and the transactional inbox. ``http`` defaults to a redirect-free
    :class:`HttpClient`; ``base_url`` defaults to the canonical API and accepts
    only an HTTP loopback injection for tests.
    """

    def __init__(
        self,
        config: MessengerConfig,
        store: Store,
        http: HttpClient | None = None,
        *,
        base_url: str = DEFAULT_BASE_URL,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if config.platform != "discord":
            raise ValueError("DiscordTransport requires a discord configuration")
        self._config = config
        self._store = store
        self._http = http if http is not None else HttpClient()
        self._base_url = _validate_base_url(base_url)
        self._clock = clock
        self._self_id: str | None = None

    @property
    def base_url(self) -> str:
        """The validated Discord API base URL."""
        return self._base_url

    def now(self) -> float:
        """Current time from the injected clock, for caller-side retry scheduling."""
        return self._clock()

    def identity(self) -> str:
        """Confirm the configured bot via ``users/@me`` and cache its id.

        The response must be an object whose ``bot`` field is exactly true and
        whose ``id`` is a snowflake; anything else raises :class:`TransportError`.
        """
        if self._self_id is not None:
            return self._self_id
        body = self._request("GET", "/users/@me", None)
        if not _is_object(body):
            raise TransportError("discord identity response is not an object")
        if body.get("bot") is not True:
            raise TransportError("discord account is not a bot")
        self._self_id = _identifier(body, "id", "identity")
        return self._self_id

    def poll(self) -> list[str]:
        """Enqueue new messages from configured channels and return their job ids.

        The first poll of a channel records a baseline and enqueues nothing;
        later polls catch up from the durable cursor. Accepted submissions are
        inserted chronologically and the cursor advances atomically.
        """
        if not self._config.enabled:
            return []
        self_id = self.identity()
        job_ids: list[str] = []
        for configured in self._config.channels:
            channel_id = _snowflake(configured)
            if channel_id is None:
                raise TransportError("configured discord channel is not a valid identifier")
            job_ids.extend(self._poll_channel(channel_id, self_id))
        return job_ids

    def send(self, delivery: Delivery) -> str:
        """POST one message and return its remote id.

        The target is the delivery's transport thread when one is set, otherwise
        its channel. ``allowed_mentions.parse`` is emptied so a reply never pings,
        and a ``reply_to`` becomes a ``message_reference``. No retry is attempted.
        """
        channel_id = self._required_identifier(delivery.channel_id, "channel")
        destination = channel_id
        if delivery.thread_id is not None:
            destination = self._required_identifier(delivery.thread_id, "thread")
        payload: dict[str, JSONValue] = {
            "content": delivery.content,
            "allowed_mentions": {"parse": []},
        }
        if delivery.reply_to is not None:
            payload["message_reference"] = {
                "message_id": self._required_identifier(delivery.reply_to, "reply")
            }
        body = self._request("POST", f"/channels/{destination}/messages", payload)
        if not _is_object(body):
            raise TransportError("discord message response is not an object")
        return _identifier(body, "id", "message")

    def edit(self, channel: str, message_id: str, text: str) -> str:
        """PATCH one message's content and return its remote id."""
        channel_id = self._required_identifier(channel, "channel")
        target = self._required_identifier(message_id, "message")
        body = self._request(
            "PATCH",
            f"/channels/{channel_id}/messages/{target}",
            {"content": text},
        )
        if not _is_object(body):
            raise TransportError("discord edit response is not an object")
        return _identifier(body, "id", "message")

    def typing(self, channel: str, thread_id: str | None = None) -> None:
        """POST a typing indicator to a channel or an explicit thread."""
        channel_id = self._required_identifier(channel, "channel")
        destination = channel_id
        if thread_id is not None:
            destination = self._required_identifier(thread_id, "thread")
        self._request("POST", f"/channels/{destination}/typing", None)

    def _poll_channel(self, channel_id: str, self_id: str) -> list[str]:
        key = f"{CURSOR_PREFIX}{channel_id}"
        raw = self._store.get_checkpoint(key)
        if raw is None:
            newest = self._baseline(channel_id)
            self._store.enqueue_batch([], (key, newest))
            return []
        cursor = self._cursor(raw)

        collected: list[dict[str, JSONValue]] = []
        newest_seen = cursor
        before: str | None = None
        pages = 0
        while True:
            pages += 1
            if pages > MAX_HISTORY_PAGES:
                raise TransportError("discord history backlog exceeds the page limit")
            page = self._messages(channel_id, before)
            if not page:
                break
            reached = False
            for message in page:
                message_id = _identifier(message, "id", "message")
                value = int(message_id)
                if value <= cursor:
                    reached = True
                    break
                collected.append(message)
                if value > newest_seen:
                    newest_seen = value
            if reached:
                break
            before = _identifier(page[-1], "id", "message")
            if len(page) < PAGE_LIMIT:
                break

        if newest_seen <= cursor:
            return []
        submissions = [
            submission
            for message in reversed(collected)
            if (submission := self._submission(message, channel_id, self_id)) is not None
        ]
        return self._store.enqueue_batch(submissions, (key, str(newest_seen)))

    def _baseline(self, channel_id: str) -> str:
        page = self._messages(channel_id, None)
        if not page:
            return EMPTY_BASELINE
        return _identifier(page[0], "id", "message")

    def _messages(self, channel_id: str, before: str | None) -> list[dict[str, JSONValue]]:
        query = f"?limit={PAGE_LIMIT}"
        if before is not None:
            query += f"&before={before}"
        body = self._request("GET", f"/channels/{channel_id}/messages{query}", None)
        if not _is_array(body):
            raise TransportError("discord messages response is not an array")
        messages: list[dict[str, JSONValue]] = []
        for item in body:
            if not _is_object(item):
                raise TransportError("discord messages response is malformed")
            messages.append(item)
        return messages

    def _submission(
        self, message: dict[str, JSONValue], channel_id: str, self_id: str
    ) -> Submission | None:
        if channel_id not in self._config.channels:
            return None
        if message.get("webhook_id") is not None:
            return None
        author = message.get("author")
        if not _is_object(author) or author.get("bot") is True:
            return None
        author_id = _snowflake(author.get("id"))
        if author_id is None or author_id == self_id:
            return None
        if author_id not in self._config.users:
            return None
        message_id = _snowflake(message.get("id"))
        if message_id is None:
            return None
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            return None
        return Submission(
            source_key=f"discord:{channel_id}:{message_id}",
            prompt=content,
            approved=False,
            channel_id=channel_id,
            reply_to=message_id,
            thread_id=None,
            platform="discord",
        )

    def _cursor(self, raw: str) -> int:
        if not raw.isascii() or not raw.isdigit():
            raise TransportError("discord cursor checkpoint is invalid")
        return int(raw)

    def _required_identifier(self, value: str, what: str) -> str:
        identifier = _snowflake(value)
        if identifier is None:
            raise TransportError(f"discord {what} identifier is invalid")
        return identifier

    def _request(self, method: str, path: str, payload: dict[str, JSONValue] | None) -> JSONValue:
        headers = self._headers(with_content_type=payload is not None)
        return self._http.request(method, f"{self._base_url}{path}", headers, payload)

    def _headers(self, *, with_content_type: bool) -> dict[str, str]:
        headers = {
            "Authorization": f"Bot {self._token()}",
            "User-Agent": USER_AGENT,
        }
        if with_content_type:
            headers["Content-Type"] = "application/json"
        return headers

    def _token(self) -> str:
        if not self._config.enabled:
            raise TransportError("discord transport is disabled")
        name = self._config.token_env
        value = os.environ.get(name) if name else None
        if value is None or not value.strip():
            raise TransportError("discord token is not configured")
        return value.strip()


__all__ = [
    "CURSOR_PREFIX",
    "DEFAULT_BASE_URL",
    "DiscordTransport",
    "EMPTY_BASELINE",
    "MAX_HISTORY_PAGES",
    "PAGE_LIMIT",
    "USER_AGENT",
]
