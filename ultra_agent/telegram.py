"""Telegram Bot API transport with a durable update offset.

This adapter speaks the same narrow interface as the other messenger
transports: it validates its own bot identity, polls inbound updates, and sends,
edits or marks typing on outbound messages. It owns no runtime policy; approval,
delivery state and retry scheduling stay in the store and the runtime.

Two properties matter most here. First, the bot token lives only inside the
request URL that :class:`~ultra_agent.http.HttpClient` builds; every failure
surfaces as a fixed, token-free :class:`TransportError` from that shared
boundary, so a Telegram token can never appear in an exception or a traceback.
Second, inbound progress is a durable checkpoint: the ``telegram:offset``
cursor advances to one past the highest ``update_id`` in the same transaction
that inserts the accepted messages, so a restart resumes exactly where the last
committed batch ended.

Update parsing is strict on identity and lenient on shape. Telegram may mix
non-message updates into a page; those are ignored safely while still advancing
the cursor. A missing or non-integer ``update_id`` is never skipped: it aborts
the batch, leaving the cursor and the inbox unchanged so the malformed update
cannot be silently lost. ``message.from.id`` and ``message.chat.id`` are
accepted as either integers or strings, and a topic message keeps its
``message_thread_id`` separate from the replied-to ``message_id``.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import TypeGuard
from urllib.parse import urlsplit

from ultra_agent.http import HttpClient
from ultra_agent.models import (
    DEFAULT_LANE,
    Delivery,
    JSONValue,
    MessengerConfig,
    Submission,
)
from ultra_agent.store import Store

DEFAULT_BASE_URL = "https://api.telegram.org"
"""Canonical Telegram Bot API base; the only non-loopback endpoint accepted."""

TELEGRAM_OFFSET_KEY = "telegram:offset"
"""Durable checkpoint key holding the next ``getUpdates`` offset."""

POLL_LIMIT = 100
"""Updates requested per page; a full page means more backlog remains."""

_TELEGRAM_HOST = "api.telegram.org"
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
_ERROR_TEXT: dict[int, str] = {
    400: "telegram rejected the request",
    401: "telegram authorization failed",
    403: "telegram access forbidden",
    404: "telegram method or resource not found",
    409: "telegram conflict",
}


class TelegramError(RuntimeError):
    """A safe, typed Telegram API failure.

    ``code`` is the API ``error_code`` when one was present. ``str(error)`` is a
    fixed description and never contains the bot token, the request URL or
    response bodies.
    """

    def __init__(self, message: str, *, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


def _is_object(value: object) -> TypeGuard[dict[str, JSONValue]]:
    """Narrow a decoded JSON value to an object with string keys."""
    return isinstance(value, dict)


def _is_array(value: object) -> TypeGuard[list[JSONValue]]:
    """Narrow a decoded JSON value to an array."""
    return isinstance(value, list)


def _id_text(value: JSONValue) -> str | None:
    """Render a Telegram integer or string identifier, or ``None`` when absent."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _int_param(value: str) -> int | str:
    """Send a numeric identifier as an integer, keeping any non-numeric form."""
    if value.lstrip("-").isdigit():
        return int(value)
    return value


def _update_id(value: JSONValue) -> int | None:
    """Parse an ``update_id`` as an integer, rejecting booleans and other types."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _validate_base_url(url: str) -> str:
    """Accept only the canonical HTTPS API or an explicit loopback injection."""
    parts = urlsplit(url.strip())
    if parts.username or parts.password or parts.query or parts.fragment:
        raise ValueError("base_url must not embed credentials, a query or a fragment")
    if parts.path.rstrip("/"):
        raise ValueError("base_url must not contain a path")
    host = parts.hostname
    if not host:
        raise ValueError("base_url must be an absolute URL with a host")
    if parts.scheme == "https" and host == _TELEGRAM_HOST:
        return url.strip().rstrip("/")
    if parts.scheme == "http" and host in _LOOPBACK_HOSTS:
        return url.strip().rstrip("/")
    raise ValueError(
        "base_url must be https://api.telegram.org or an http loopback address"
    )


def _error_code(value: JSONValue) -> int | None:
    """Extract a numeric Telegram ``error_code``, rejecting booleans."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _error_text(code: int | None) -> str:
    """Return a fixed, token-free description for an API error code."""
    if code is None:
        return "telegram request failed"
    return _ERROR_TEXT.get(code, "telegram request failed")


def _authorized(messenger: MessengerConfig, user_id: str, chat_id: str) -> bool:
    """Accept a message only when both its sender and channel are configured."""
    return user_id in messenger.users and chat_id in messenger.channels


class TelegramTransport:
    """A bounded Telegram Bot API adapter over an injected HTTP boundary.

    ``config`` supplies the ``telegram`` messenger entry (token environment
    variable and allowlists); ``store`` supplies the durable offset checkpoint
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
        if config.platform != "telegram":
            raise ValueError("TelegramTransport requires a telegram configuration")
        self._config = config
        self._store = store
        self._http = http if http is not None else HttpClient()
        self._base_url = _validate_base_url(base_url)
        self._clock = clock

    @property
    def base_url(self) -> str:
        """The validated Telegram Bot API base URL."""
        return self._base_url

    def now(self) -> float:
        """Current time from the injected clock."""
        return self._clock()

    def identity(self) -> str:
        """Confirm the configured bot via ``getMe``.

        Requires ``ok`` true, an object ``result`` and ``is_bot`` true; anything
        else is a :class:`TelegramError`.
        """
        result = self._unwrap(self._request("GET", self._url("getMe"), None), "getMe")
        if not _is_object(result) or result.get("is_bot") is not True:
            raise TelegramError("getMe did not confirm a bot account")
        bot_id = _id_text(result.get("id"))
        if bot_id is None:
            raise TelegramError("getMe returned no bot id")
        return bot_id

    def poll(self) -> list[str]:
        """Fetch new updates, enqueue accepted work and advance the cursor.

        Returns the durable job IDs accepted in this call. Every full page is
        followed by another request so a backlog is drained, and each page's
        messages are inserted together with the offset that skips them.
        """
        if not self._config.enabled:
            return []
        messenger = self._config
        self_id = self.identity()
        accepted: list[str] = []
        offset = self._load_offset()
        for _ in range(100):
            updates = self._get_updates(offset)
            if not updates:
                break
            batch: list[Submission] = []
            highest: int | None = None
            for update in updates:
                current_id = _update_id(update.get("update_id"))
                if current_id is None:
                    raise TelegramError("telegram update has no valid update_id")
                highest = current_id if highest is None else max(highest, current_id)
                submission = self._submission_for(update, messenger, self_id)
                if submission is not None:
                    batch.append(submission)
            if highest is None:
                break
            next_offset = highest + 1
            if offset is not None and next_offset <= offset:
                raise TelegramError("telegram update cursor did not advance")
            accepted.extend(
                self._store.enqueue_batch(batch, (TELEGRAM_OFFSET_KEY, str(next_offset)))
            )
            offset = next_offset
            if len(updates) < POLL_LIMIT:
                break
        else:
            raise TelegramError("telegram update backlog exceeds the page limit")
        return accepted

    def send(
        self,
        delivery: Delivery,
    ) -> str:
        """Send one message and return its remote id.

        A topic message sets ``message_thread_id``; a reply sets
        ``reply_parameters.message_id``. The two are independent.
        """
        payload: dict[str, JSONValue] = {
            "chat_id": _int_param(delivery.channel_id), "text": delivery.content
        }
        if delivery.thread_id is not None:
            payload["message_thread_id"] = _int_param(delivery.thread_id)
        if delivery.reply_to is not None:
            payload["reply_parameters"] = {"message_id": _int_param(delivery.reply_to)}
        result = self._unwrap(self._post("sendMessage", payload), "sendMessage")
        if not _is_object(result):
            raise TelegramError("sendMessage returned no message")
        remote_id = _id_text(result.get("message_id"))
        if remote_id is None:
            raise TelegramError("sendMessage returned no message id")
        return remote_id

    def edit(self, channel_id: str, message_id: str, text: str) -> str:
        """Replace the text of one message with ``editMessageText``."""
        payload: dict[str, JSONValue] = {
            "chat_id": _int_param(channel_id),
            "message_id": _int_param(message_id),
            "text": text,
        }
        result = self._unwrap(self._post("editMessageText", payload), "editMessageText")
        if result is True:
            return message_id
        remote_id = _id_text(result.get("message_id")) if _is_object(result) else None
        if remote_id is None:
            raise TelegramError("editMessageText returned no message id")
        return remote_id

    def typing(self, channel_id: str, thread_id: str | None = None) -> None:
        """Send the ``typing`` chat action for a channel or topic."""
        payload: dict[str, JSONValue] = {
            "chat_id": _int_param(channel_id),
            "action": "typing",
        }
        if thread_id is not None:
            payload["message_thread_id"] = _int_param(thread_id)
        self._unwrap(self._post("sendChatAction", payload), "sendChatAction")

    def _token(self) -> str:
        if not self._config.enabled:
            raise TelegramError("telegram transport is disabled")
        name = self._config.token_env
        value = os.environ.get(name)
        if value is None or not value.strip():
            raise TelegramError("telegram token is not configured")
        return value.strip()

    def _url(self, method: str, query: str = "") -> str:
        return f"{self._base_url}/bot{self._token()}/{method}{query}"

    def _request(
        self, method: str, url: str, payload: dict[str, JSONValue] | None
    ) -> JSONValue:
        headers = {"Content-Type": "application/json"} if payload is not None else {}
        return self._http.request(method, url, headers, payload)

    def _post(self, method: str, payload: dict[str, JSONValue]) -> JSONValue:
        return self._request("POST", self._url(method), payload)

    def _unwrap(self, body: JSONValue, method: str) -> JSONValue:
        if not _is_object(body):
            raise TelegramError(f"telegram {method} returned no object")
        if body.get("ok") is not True:
            code = _error_code(body.get("error_code"))
            raise TelegramError(_error_text(code), code=code)
        return body.get("result")

    def _get_updates(self, offset: int | None) -> list[dict[str, JSONValue]]:
        query = f"?limit={POLL_LIMIT}"
        if offset is not None:
            query += f"&offset={offset}"
        result = self._unwrap(
            self._request("GET", self._url("getUpdates", query), None), "getUpdates"
        )
        if not _is_array(result):
            raise TelegramError("getUpdates returned no update list")
        updates: list[dict[str, JSONValue]] = []
        for item in result:
            if not _is_object(item):
                raise TelegramError("getUpdates returned a malformed update")
            updates.append(item)
        return updates

    def _submission_for(
        self, update: dict[str, JSONValue], messenger: MessengerConfig, self_id: str
    ) -> Submission | None:
        message = update.get("message")
        if not _is_object(message):
            return None
        sender = message.get("from")
        if not _is_object(sender) or sender.get("is_bot") is True:
            return None
        user_id = _id_text(sender.get("id"))
        chat = message.get("chat")
        chat_id = _id_text(chat.get("id")) if _is_object(chat) else None
        message_id = _id_text(message.get("message_id"))
        text = message.get("text")
        if user_id is None or user_id == self_id or chat_id is None or message_id is None:
            return None
        if not isinstance(text, str) or not text.strip():
            return None
        if not _authorized(messenger, user_id, chat_id):
            return None
        return Submission(
            source_key=f"telegram:{chat_id}:{message_id}",
            prompt=text,
            approved=False,
            channel_id=chat_id,
            reply_to=message_id,
            thread_id=_id_text(message.get("message_thread_id")),
            lane=DEFAULT_LANE,
            platform="telegram",
        )

    def _load_offset(self) -> int | None:
        raw = self._store.get_checkpoint(TELEGRAM_OFFSET_KEY)
        if raw is None:
            return None
        try:
            offset = int(raw)
        except ValueError:
            raise TelegramError("telegram offset checkpoint is invalid") from None
        if offset < 0:
            raise TelegramError("telegram offset checkpoint is invalid")
        return offset


__all__ = [
    "DEFAULT_BASE_URL",
    "POLL_LIMIT",
    "TELEGRAM_OFFSET_KEY",
    "TelegramError",
    "TelegramTransport",
]
