"""Slack Web API transport: send/edit, history polling and thread replies.

This adapter speaks the official Slack Web API over the shared
:class:`~ultra_agent.http.HttpClient` boundary. It reads its bearer token (and
an optional user-session cookie) only from environment variables named in the
:class:`~ultra_agent.models.MessengerConfig`; secret values never live in
configuration or in an error message.

Inbound acceptance is deliberately narrow: only configured channels and
configured authors are accepted, the token's own user and any message carrying
a ``bot_id`` are ignored, and a reply inside a tracked thread is accepted
**without** requiring a mention of the agent. Message timestamps stay exact
strings and are compared with :class:`decimal.Decimal`, never a lossy float.

Polling keeps two durable facts per channel in the shared checkpoint table:

* a per-channel history cursor and a per-thread reply cursor, so a restart
  resumes instead of replaying; and
* a JSON list of tracked root message timestamps, so a thread the agent posts
  into keeps being polled for later replies and older roots are never dropped.

Unofficial user-session discovery (``subscriptions.thread.getView``) runs only
when a cookie environment variable is configured and is restricted to the
allowed channels; its failures propagate instead of being replaced by a
fabricated success. Typing indicators are unsupported by both the reference
source and the actual Slack API, so :meth:`SlackTransport.typing` fails with a
typed capability error rather than a silent no-op.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from collections.abc import Callable, Mapping
from decimal import Decimal, InvalidOperation
from typing import NoReturn, TypeGuard
from urllib.parse import urlsplit

from ultra_agent.http import HttpClient
from ultra_agent.models import Delivery, JSONValue, MessengerConfig, Submission
from ultra_agent.store import Store

DEFAULT_BASE_URL = "https://slack.com/api"
"""Official Slack Web API base used when the caller does not choose one."""

DEFAULT_PAGE_LIMIT = 200
"""Page size requested from Slack history and reply endpoints."""

MAX_PAGES = 50
"""Hard cap on pages followed per paginated call before failing visibly."""

THREADS_VIEW_LIMIT = 20
"""Threads requested from the unofficial subscribed-thread view."""

_HISTORY_PREFIX = "slack:history:"
_THREAD_PREFIX = "slack:thread:"
_THREADS_PREFIX = "slack:threads:"


class SlackError(RuntimeError):
    """A Slack call was rejected or returned an unusable response.

    ``str(error)`` always names the method and, when Slack supplied a safe
    short error code, that code; it never contains the token, the cookie or the
    raw response body.
    """


class UnsupportedCapabilityError(RuntimeError):
    """A messenger capability Slack does not offer for this transport.

    Raised instead of pretending the operation succeeded, so callers can report
    the gap rather than assume a typing/presence signal was delivered.
    """


def _is_object_list(value: object) -> TypeGuard[list[object]]:
    """Narrow a decoded JSON value to a list without losing element typing."""
    return isinstance(value, list)


def _decimal_ts(value: str) -> Decimal | None:
    """Parse a Slack timestamp exactly, or return ``None`` when unusable."""
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def _ts_after(left: str, right: str) -> bool:
    """Return whether ``left`` is strictly after ``right`` using exact decimals."""
    left_value = _decimal_ts(left)
    right_value = _decimal_ts(right)
    if left_value is None or right_value is None:
        return False
    return left_value > right_value


def _message_ts(message: Mapping[str, JSONValue]) -> str:
    """Return a message's ``ts`` field as a string, or ``""`` when absent."""
    value = message.get("ts")
    return value if isinstance(value, str) else ""


def _error_code(body: Mapping[str, JSONValue]) -> str:
    """Return a short, safe Slack error code, or a generic description."""
    error = body.get("error")
    if isinstance(error, str) and 0 < len(error) <= 64:
        safe = all(
            character.islower() or character == "_" or character.isdigit()
            for character in error
        )
        if safe:
            return error
    return "request rejected"


def _next_cursor(body: Mapping[str, JSONValue]) -> str | None:
    """Return a non-empty pagination cursor from ``response_metadata``."""
    metadata = body.get("response_metadata")
    if not isinstance(metadata, dict):
        return None
    cursor = metadata.get("next_cursor")
    if isinstance(cursor, str) and cursor:
        return cursor
    return None


def _newest_ts(messages: list[JSONValue]) -> str | None:
    """Return the newest valid timestamp string in a message list."""
    best: tuple[Decimal, str] | None = None
    for item in messages:
        if not isinstance(item, dict):
            continue
        ts = _message_ts(item)
        parsed = _decimal_ts(ts)
        if parsed is None:
            continue
        if best is None or parsed > best[0]:
            best = (parsed, ts)
    return best[1] if best is not None else None


def _format_ts(seconds: float) -> str:
    """Render wall-clock seconds as a Slack-style ``seconds.microseconds`` string."""
    whole = int(seconds)
    micros = int(round((seconds - whole) * 1_000_000))
    if micros >= 1_000_000:
        whole += 1
        micros -= 1_000_000
    return f"{whole}.{micros:06d}"


class SlackTransport:
    """One Slack transport bound to a messenger config and the durable store.

    ``http``, ``base_url`` and ``clock`` are injectable so tests drive the real
    urllib path against a loopback fixture and a fake clock; production callers
    use the defaults. The transport holds no metric of its own beyond the cached
    authenticated user id.
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
        if config.platform != "slack":
            raise ValueError("SlackTransport requires a slack messenger config")
        trimmed = base_url.rstrip("/")
        parsed = urlsplit(trimmed)
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Slack endpoint must not contain credentials or query data")
        canonical = parsed.scheme == "https" and parsed.hostname == "slack.com"
        loopback = parsed.scheme == "http" and parsed.hostname in (
            "127.0.0.1", "::1", "localhost"
        )
        if not canonical and not loopback:
            raise ValueError("Slack endpoint must be canonical HTTPS or loopback HTTP")
        self._config = config
        self._store = store
        self._http = http if http is not None else HttpClient()
        self._base_url = trimmed
        self._clock = clock
        self._self_id: str | None = None

    def identity(self) -> str:
        """Return the authenticated user id from ``auth.test`` (cached)."""
        if self._self_id is None:
            body = self._call("auth.test", {})
            user_id = body.get("user_id")
            if not isinstance(user_id, str) or not user_id:
                raise SlackError("slack auth.test returned no user id")
            self._self_id = user_id
        return self._self_id

    def send(
        self,
        delivery: Delivery,
    ) -> str:
        """Post one message and return its timestamp.

        The message threads under ``thread_id`` or ``reply_to`` when either is
        given; otherwise it starts a new thread. The root timestamp is persisted
        so later replies are polled.
        """
        channel = delivery.channel_id
        if not channel.strip():
            raise ValueError("slack send requires a channel")
        thread_ts = delivery.thread_id or delivery.reply_to
        params: dict[str, JSONValue] = {"channel": channel, "text": delivery.content}
        if thread_ts:
            params["thread_ts"] = thread_ts
        body = self._call("chat.postMessage", params)
        ts = body.get("ts")
        if not isinstance(ts, str) or _decimal_ts(ts) is None:
            raise SlackError("slack chat.postMessage returned no message timestamp")
        out_channel = body.get("channel")
        target = out_channel if isinstance(out_channel, str) and out_channel else channel
        self._track_thread(target, thread_ts or ts)
        return ts

    def edit(self, channel: str, ts: str, text: str) -> str:
        """Replace the text of one posted message via ``chat.update``."""
        if not channel.strip():
            raise ValueError("slack edit requires a channel")
        body = self._call("chat.update", {"channel": channel, "ts": ts, "text": text})
        remote = body.get("ts")
        return remote if isinstance(remote, str) else ts

    def typing(self, channel: str, thread_id: str | None = None) -> NoReturn:
        """Always fail: Slack exposes no typing indicator for this transport."""
        raise UnsupportedCapabilityError(
            "slack transport does not support typing indicators"
        )

    def poll(self) -> list[str]:
        """Run one bounded poll pass and durably enqueue accepted messages.

        Returns the durable id of each enqueued job, in the order channels then
        threads were polled. Accepted inbound messages are inserted with their
        immutable ``slack:channel:ts`` source key and the matching cursor in one
        transaction, so a crash cannot record one without the other.
        """
        if not self._config.enabled:
            return []
        self_id = self.identity()
        allowed_channels = set(self._config.channels)
        if self._config.cookie_env:
            self._discover_threads(allowed_channels)
        job_ids: list[str] = []
        for channel in self._config.channels:
            job_ids.extend(self._poll_history(channel, self_id))
        for channel, root in self._thread_targets(allowed_channels):
            job_ids.extend(self._poll_thread(channel, root, self_id))
        return job_ids

    def _call(self, method: str, params: Mapping[str, JSONValue]) -> dict[str, JSONValue]:
        """POST one method and strictly validate the ``ok`` envelope."""
        url = f"{self._base_url}/{method}"
        body = self._http.request("POST", url, self._headers(), dict(params))
        if not isinstance(body, dict):
            raise SlackError(f"slack {method} returned a non-object response")
        if body.get("ok") is not True:
            raise SlackError(f"slack {method} rejected: {_error_code(body)}")
        return body

    def _headers(self) -> dict[str, str]:
        """Build request headers, adding the user cookie only when configured."""
        headers = {
            "Authorization": f"Bearer {self._token()}",
            "Content-Type": "application/json; charset=utf-8",
        }
        cookie = self._cookie()
        if cookie is not None:
            headers["Cookie"] = f"d={cookie}"
        return headers

    def _token(self) -> str:
        """Read the bearer token from its configured environment variable."""
        if not self._config.enabled:
            raise SlackError("slack transport is disabled")
        name = self._config.token_env
        if not name:
            raise SlackError("slack transport requires a token_env name")
        value = os.environ.get(name)
        if not value:
            raise SlackError(f"slack token environment variable {name!r} is not set")
        return value

    def _cookie(self) -> str | None:
        """Read the optional user-session cookie, if one is configured."""
        name = self._config.cookie_env
        if not name:
            return None
        value = os.environ.get(name)
        if not value:
            raise SlackError(f"slack cookie environment variable {name!r} is not set")
        return value

    def _paginate(
        self, method: str, params: Mapping[str, JSONValue], items_key: str
    ) -> list[dict[str, JSONValue]]:
        """Follow ``next_cursor`` with repeated- and max-page guards."""
        collected: list[dict[str, JSONValue]] = []
        cursor: str | None = None
        seen: set[str] = set()
        pages = 0
        while True:
            call = dict(params)
            if cursor is not None:
                call["cursor"] = cursor
            body = self._call(method, call)
            items = body.get(items_key)
            if items is None:
                page: list[JSONValue] = []
            elif isinstance(items, list):
                page = items
            else:
                raise SlackError(f"slack {method} returned an invalid {items_key} payload")
            for item in page:
                if isinstance(item, dict):
                    collected.append(item)
            next_cursor = _next_cursor(body)
            pages += 1
            if next_cursor is None:
                break
            if next_cursor in seen:
                raise SlackError(f"slack {method} repeated a pagination cursor")
            if pages >= MAX_PAGES:
                raise SlackError(f"slack {method} exceeded the pagination page limit")
            seen.add(next_cursor)
            cursor = next_cursor
        return collected

    def _discover_threads(self, allowed_channels: set[str]) -> None:
        """Track subscribed threads discovered via the unofficial client view."""
        body = self._call("subscriptions.thread.getView", {"limit": THREADS_VIEW_LIMIT})
        threads = body.get("threads")
        if threads is None:
            return
        if not isinstance(threads, list):
            raise SlackError("slack subscriptions.thread.getView returned an invalid payload")
        for thread in threads:
            if not isinstance(thread, dict):
                continue
            root = thread.get("root_msg")
            if not isinstance(root, dict):
                continue
            channel = root.get("channel")
            ts = root.get("ts")
            if not isinstance(channel, str) or not isinstance(ts, str):
                continue
            if channel not in allowed_channels or _decimal_ts(ts) is None:
                continue
            self._track_thread(channel, ts)

    def _thread_targets(self, allowed_channels: set[str]) -> list[tuple[str, str]]:
        """Union explicitly configured and durably tracked thread roots."""
        targets: dict[tuple[str, str], None] = {}
        for ref in self._config.threads:
            if ref.channel_id in allowed_channels and _decimal_ts(ref.thread_id) is not None:
                targets[(ref.channel_id, ref.thread_id)] = None
        for channel in allowed_channels:
            for root in self._read_roots(self._threads_key(channel)):
                if _decimal_ts(root) is not None:
                    targets[(channel, root)] = None
        return list(targets)

    def _poll_history(self, channel: str, self_id: str) -> list[str]:
        """Baseline once, then enqueue new top-level channel messages."""
        key = self._history_key(channel)
        cursor = self._store.get_checkpoint(key)
        if cursor is None or _decimal_ts(cursor) is None:
            body = self._call(
                "conversations.history",
                {"channel": channel, "limit": DEFAULT_PAGE_LIMIT},
            )
            messages = body.get("messages")
            newest = _newest_ts(messages) if isinstance(messages, list) else None
            if newest is None:
                newest = _format_ts(self._clock())
            return self._store.enqueue_batch([], (key, newest))
        messages = self._paginate(
            "conversations.history",
            {
                "channel": channel,
                "oldest": cursor,
                "inclusive": "false",
                "limit": DEFAULT_PAGE_LIMIT,
            },
            "messages",
        )
        fresh = [
            message
            for message in messages
            if self._accepted(message, self_id) and _ts_after(_message_ts(message), cursor)
        ]
        if not fresh:
            return []
        fresh.sort(key=lambda message: _decimal_ts(_message_ts(message)) or Decimal(0))
        submissions = [self._submission(channel, message) for message in fresh]
        return self._store.enqueue_batch(submissions, (key, _message_ts(fresh[-1])))

    def _poll_thread(self, channel: str, root: str, self_id: str) -> list[str]:
        """Enqueue new replies in one thread, starting after its root timestamp."""
        key = self._thread_key(channel, root)
        cursor = self._store.get_checkpoint(key)
        if cursor is None or _decimal_ts(cursor) is None:
            cursor = root
        messages = self._paginate(
            "conversations.replies",
            {
                "channel": channel,
                "ts": root,
                "oldest": cursor,
                "inclusive": "false",
                "limit": DEFAULT_PAGE_LIMIT,
            },
            "messages",
        )
        fresh = [
            message
            for message in messages
            if _message_ts(message) != root
            and self._accepted(message, self_id)
            and _ts_after(_message_ts(message), cursor)
        ]
        if not fresh:
            return []
        fresh.sort(key=lambda message: _decimal_ts(_message_ts(message)) or Decimal(0))
        submissions = [self._submission(channel, message, thread_id=root) for message in fresh]
        return self._store.enqueue_batch(submissions, (key, _message_ts(fresh[-1])))

    def _accepted(self, message: Mapping[str, JSONValue], self_id: str) -> bool:
        """Return whether a message is an allowed, non-self, non-bot inbound."""
        ts = _message_ts(message)
        if _decimal_ts(ts) is None:
            return False
        user = message.get("user")
        if not isinstance(user, str) or not user or user == self_id:
            return False
        if isinstance(message.get("bot_id"), str):
            return False
        if user not in set(self._config.users):
            return False
        text = message.get("text")
        return isinstance(text, str) and bool(text.strip())

    def _submission(
        self,
        channel: str,
        message: Mapping[str, JSONValue],
        *,
        thread_id: str | None = None,
    ) -> Submission:
        """Build the immutable inbound submission for an accepted message."""
        ts = _message_ts(message)
        user = message.get("user")
        author = user if isinstance(user, str) else "unknown"
        text = message.get("text")
        body = text if isinstance(text, str) else ""
        if thread_id is None:
            thread_ts = message.get("thread_ts")
            if (
                isinstance(thread_ts, str)
                and thread_ts != ts
                and _decimal_ts(thread_ts) is not None
            ):
                thread_id = thread_ts
        return Submission(
            source_key=f"slack:{channel}:{ts}",
            prompt=f"Slack message from {author} in {channel}:\n{body}",
            approved=False,
            channel_id=channel,
            reply_to=ts,
            thread_id=thread_id,
            platform="slack",
        )

    def _read_roots(
        self, key: str, *, transaction: sqlite3.Connection | None = None
    ) -> list[str]:
        """Read durable tracked roots; corruption must not silently discard them."""
        raw = self._store.get_checkpoint(key, transaction=transaction)
        if raw is None:
            return []
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            raise SlackError("slack tracked-thread checkpoint is invalid") from None
        if not _is_object_list(data):
            raise SlackError("slack tracked-thread checkpoint is invalid")
        roots: list[str] = []
        for item in data:
            if not isinstance(item, str) or _decimal_ts(item) is None:
                raise SlackError("slack tracked-thread checkpoint is invalid")
            roots.append(item)
        return roots

    def _track_thread(self, channel: str, root_ts: str) -> None:
        """Append a root timestamp to the channel list without dropping others."""
        key = self._threads_key(channel)
        with self._store.connection() as transaction:
            roots = self._read_roots(key, transaction=transaction)
            if root_ts in roots:
                return
            roots.append(root_ts)
            self._store.set_checkpoint(key, json.dumps(roots), transaction=transaction)

    @staticmethod
    def _history_key(channel: str) -> str:
        return f"{_HISTORY_PREFIX}{channel}"

    @staticmethod
    def _thread_key(channel: str, root_ts: str) -> str:
        return f"{_THREAD_PREFIX}{channel}:{root_ts}"

    @staticmethod
    def _threads_key(channel: str) -> str:
        return f"{_THREADS_PREFIX}{channel}"


__all__ = [
    "DEFAULT_BASE_URL",
    "SlackError",
    "SlackTransport",
    "UnsupportedCapabilityError",
]
