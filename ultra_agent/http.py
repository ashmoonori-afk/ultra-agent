"""Bounded, redaction-safe HTTP JSON transport shared by messenger adapters.

Telegram, Slack and the status relay all speak JSON over HTTP. This module is
their single wire boundary: one request per call, redirects always refused, the
response body capped at one mebibyte and every failure turned into a typed
:class:`TransportError`.

Error text is always a fixed, safe description. Request URLs, headers, query
strings, token-bearing path segments and response bodies are never echoed into
an exception, so a Telegram bot token embedded in a URL cannot leak through a
message or a traceback. A parsed response is validated into ``JSONValue`` at the
boundary instead of being trusted blindly.

The client performs no automatic retries. After a GET failure the caller decides
how to poll; a state-changing request whose outcome is uncertain is surfaced
with ``ambiguous=True`` rather than repeated silently.
"""

from __future__ import annotations

import http.client
import json
import math
import urllib.error
import urllib.request
from collections.abc import Mapping
from email.message import Message
from typing import IO, TypeGuard

from ultra_agent.models import JSONValue

DEFAULT_TIMEOUT_SECONDS = 10.0
"""Request timeout used when the caller does not choose one."""

MAX_TIMEOUT_SECONDS = 30.0
"""Hard upper bound on any request timeout."""

MAX_RESPONSE_BYTES = 1024 * 1024
"""Maximum accepted response body size, one mebibyte."""

MAX_RETRY_AFTER_SECONDS = 3600.0
"""Largest rate-limit delay surfaced to callers, one hour."""

_AMBIGUOUS_METHODS = frozenset({"POST", "PUT", "PATCH"})


class TransportError(Exception):
    """A bounded, redacted failure of one HTTP JSON transaction.

    ``status`` is the HTTP status when a response existed; ``retry_after`` is a
    validated 429 delay in seconds; ``ambiguous`` is true only when a
    state-changing request produced an uncertain outcome the caller must not
    blindly repeat. ``str(error)`` never contains the URL, headers, credentials
    or body.
    """

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        ambiguous: bool = False,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.ambiguous = ambiguous
        self.retry_after = retry_after


class _RejectRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Redirect policy: every 3xx is refused instead of being followed."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> urllib.request.Request | None:
        raise urllib.error.HTTPError(req.full_url, code, "redirect refused", headers, fp)


def _build_opener() -> urllib.request.OpenerDirector:
    """Build an opener whose redirect handler refuses every redirect."""
    return urllib.request.build_opener(_RejectRedirectHandler())


def _parse_retry_after(headers: Message | None) -> float | None:
    """Return a validated 429 delay in seconds, or ``None`` when unusable."""
    if headers is None:
        return None
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        seconds = float(raw.strip())
    except ValueError:
        return None
    if not math.isfinite(seconds) or seconds < 0.0 or seconds > MAX_RETRY_AFTER_SECONDS:
        return None
    return seconds


def _body_retry_after(raw: bytes) -> float | None:
    """Read the bounded Discord/Telegram rate-limit body without exposing it."""
    try:
        value = _parse_json_body(raw, 429)
    except TransportError:
        return None
    if not isinstance(value, dict):
        return None
    delay = value.get("retry_after")
    parameters = value.get("parameters")
    if delay is None and isinstance(parameters, dict):
        delay = parameters.get("retry_after")
    if isinstance(delay, bool) or not isinstance(delay, (int, float)):
        return None
    seconds = float(delay)
    if not math.isfinite(seconds) or not 0 <= seconds <= MAX_RETRY_AFTER_SECONDS:
        return None
    return seconds


def _status_error(status: int, headers: Message | None, ambiguous: bool) -> TransportError:
    """Map a non-2xx status to a safe, typed error without echoing anything."""
    if status == 429:
        return TransportError(
            "rate limited",
            status=status,
            retry_after=_parse_retry_after(headers),
        )
    if 300 <= status < 400:
        return TransportError("redirect refused", status=status)
    if status >= 500:
        return TransportError("server error", status=status, ambiguous=ambiguous)
    return TransportError("request rejected", status=status)


def _transport_error(ambiguous: bool) -> TransportError:
    """Map a transport/connection failure to a safe, typed error."""
    message = "network request outcome uncertain" if ambiguous else "network request failed"
    return TransportError(message, ambiguous=ambiguous)


def _is_json_array(value: object) -> TypeGuard[list[object]]:
    """Narrow a decoded value to a JSON array without losing element typing."""
    return isinstance(value, list)


def _is_json_object(value: object) -> TypeGuard[dict[str, object]]:
    """Narrow a decoded value to a JSON object; JSON keys are always strings."""
    return isinstance(value, dict)


def _as_json_value(value: object, status: int) -> JSONValue:
    """Validate a decoded value into ``JSONValue``, rejecting anything else."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise TransportError("response JSON contains a nonfinite number", status=status)
        return value
    if isinstance(value, str):
        return value
    if _is_json_array(value):
        return [_as_json_value(element, status) for element in value]
    if _is_json_object(value):
        mapping: dict[str, JSONValue] = {}
        for key, item in value.items():
            mapping[key] = _as_json_value(item, status)
        return mapping
    raise TransportError("response JSON is not valid", status=status) from None


def _parse_json_body(raw: bytes, status: int) -> JSONValue:
    """Decode and validate a response body, or raise a safe typed error."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise TransportError("response is not valid UTF-8", status=status) from None
    try:
        decoded = json.loads(text)
    except (json.JSONDecodeError, RecursionError):
        raise TransportError("response is not valid JSON", status=status) from None
    try:
        return _as_json_value(decoded, status)
    except RecursionError:
        raise TransportError("response JSON exceeds nesting limit", status=status) from None


class HttpClient:
    """One bounded, redirect-free HTTP JSON client per messenger adapter.

    Construct with an injected ``opener`` for deterministic tests, or rely on the
    default opener that refuses redirects and use a loopback base URL. The
    timeout is fixed at construction and bounded to (0, 30] seconds, defaulting
    to ten.
    """

    def __init__(
        self,
        *,
        opener: urllib.request.OpenerDirector | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        if not math.isfinite(timeout) or timeout <= 0.0 or timeout > MAX_TIMEOUT_SECONDS:
            raise ValueError("timeout must be a finite number in (0, 30] seconds")
        self._timeout = timeout
        self._opener = opener if opener is not None else _build_opener()

    @property
    def timeout(self) -> float:
        """The bounded request timeout in seconds."""
        return self._timeout

    def request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        payload: JSONValue | None = None,
    ) -> JSONValue:
        """Perform one bounded JSON request and return the parsed response.

        ``payload`` is JSON-encoded with UTF-8 when present; the caller supplies
        the matching ``Content-Type`` header. Any failure raises
        :class:`TransportError` and never returns partial success.
        """
        request_method = method.upper()
        try:
            data = (
                None if payload is None
                else json.dumps(payload, allow_nan=False).encode("utf-8")
            )
            request = urllib.request.Request(
                url=url,
                data=data,
                headers=dict(headers),
                method=request_method,
            )
        except (ValueError, TypeError, UnicodeError):
            raise TransportError("invalid request") from None
        ambiguous_method = request_method in _AMBIGUOUS_METHODS

        try:
            response: http.client.HTTPResponse = self._opener.open(request, timeout=self._timeout)
        except urllib.error.HTTPError as exc:
            error = _status_error(exc.code, exc.headers, ambiguous_method)
            with exc:
                if exc.code == 429 and error.retry_after is None:
                    try:
                        raw = exc.read(MAX_RESPONSE_BYTES + 1)
                    except (OSError, http.client.HTTPException):
                        raw = b""
                    if len(raw) <= MAX_RESPONSE_BYTES:
                        error.retry_after = _body_retry_after(raw)
            raise error from None
        except (urllib.error.URLError, OSError, ValueError):
            raise _transport_error(ambiguous_method) from None

        try:
            with response:
                status = response.status
                body = response.read(MAX_RESPONSE_BYTES + 1)
                response_headers = response.headers
        except (OSError, http.client.HTTPException, urllib.error.URLError):
            raise _transport_error(ambiguous_method) from None

        if len(body) > MAX_RESPONSE_BYTES:
            raise TransportError(
                "response exceeds size limit", status=status, ambiguous=ambiguous_method
            ) from None
        if not 200 <= status < 300:
            raise _status_error(status, response_headers, ambiguous_method) from None
        if status in (204, 205) and not body:
            return None
        try:
            return _parse_json_body(body, status)
        except TransportError as exc:
            raise TransportError(
                str(exc), status=status, ambiguous=ambiguous_method
            ) from None
