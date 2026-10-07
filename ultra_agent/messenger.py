"""Unified messenger dispatch over the durable outbox.

One entry point hides which platform a message belongs to. :class:`Messenger`
resolves an enabled, configured platform to its existing adapter (Discord,
Telegram or Slack), polls enabled platforms for inbound work, and drains the
shared :class:`~ultra_agent.store.Store` outbox exactly once per attempt.

Delivery honesty is the point of this module. A send is written to the durable
outbox first, then attempted at most once per :meth:`Messenger.drain` call:

* a successful adapter response completes the delivery as ``sent`` with its
  remote id;
* a rate-limit response with a validated ``retry_after`` returns the delivery
  to ``pending`` with a persisted backoff, so a later drain retries it and an
  early drain does nothing;
* every other transport failure, an ambiguous state-changing request, or an
  unexpected adapter response is recorded as ``uncertain`` and never retried
  automatically. Nothing is ever reported as a successful send unless the
  platform returned a remote id.

Chunked text is stored as one ordered delivery group: later chunks are blocked
by the store until every earlier chunk is ``sent`` or ``resolved``, so an
uncertain chunk cannot let the rest of a message overtake it. ``reply_to`` and
``thread_id`` stay separate all the way to the adapter.

Capabilities that a platform does not offer fail visibly. Slack typing and
Telegram presence raise :class:`CapabilityError` instead of returning a
successful no-op, and no call ever falls back to a different platform's
transport.

Adapters, the HTTP boundary and presence endpoints are injectable so tests can
drive the real wire path against loopback servers without changing the
configuration schema; token and cookie values stay in the environment named by
:class:`~ultra_agent.models.MessengerConfig`.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from ultra_agent.discord import DEFAULT_BASE_URL as DISCORD_BASE_URL
from ultra_agent.discord import DiscordTransport
from ultra_agent.http import HttpClient, TransportError
from ultra_agent.models import Config, MessengerConfig, Platform, ThreadRef
from ultra_agent.presence import SLACK_API_BASE_URL, PresenceSession
from ultra_agent.slack import DEFAULT_BASE_URL as SLACK_BASE_URL
from ultra_agent.slack import SlackTransport
from ultra_agent.store import Store, chunks
from ultra_agent.telegram import DEFAULT_BASE_URL as TELEGRAM_BASE_URL
from ultra_agent.telegram import TelegramTransport
from ultra_agent.transport import Transport

_DEFAULT_BASE_URLS: dict[Platform, str] = {
    "discord": DISCORD_BASE_URL,
    "slack": SLACK_BASE_URL,
    "telegram": TELEGRAM_BASE_URL,
}
"""Canonical endpoint per platform; a caller may inject a loopback override."""


class MessengerError(RuntimeError):
    """A dispatch was refused: unknown/disabled platform or a disallowed target."""


class CapabilityError(MessengerError):
    """A platform was asked for a capability it does not implement.

    Raised instead of returning a successful no-op, so a caller reports the gap
    rather than assuming a typing indicator or presence session was delivered.
    """


@dataclass(frozen=True, slots=True)
class PlatformStatus:
    """Non-secret configuration and capability data for one messenger entry.

    ``token_env`` and ``cookie_env`` are environment variable *names*, never
    credential values, so this record is safe to surface to a doctor command.
    """

    platform: Platform
    enabled: bool
    typing: bool
    presence: bool
    token_env: str
    cookie_env: str | None
    users: tuple[str, ...]
    channels: tuple[str, ...]
    threads: tuple[ThreadRef, ...]


def _supports_typing(platform: Platform) -> bool:
    """Slack exposes no typing indicator for this transport."""
    return platform != "slack"


def _supports_presence(platform: Platform) -> bool:
    """Telegram exposes no presence session."""
    return platform != "telegram"


class Messenger:
    """Dispatch inbound polling and durable outbound delivery across platforms.

    ``config`` is the parsed :class:`~ultra_agent.models.Config`; ``store`` owns
    the outbox. ``http`` is shared by every adapter and by presence. ``base_urls``
    maps a platform to an explicit endpoint (a loopback URL in tests); ``transports``
    injects a ready transport for a platform, bypassing construction entirely.
    ``discord_gateway_url`` and ``slack_presence_base_url`` override only the
    presence endpoints. Every override exists for wire-level tests; production
    callers use the defaults.
    """

    def __init__(
        self,
        config: Config,
        store: Store,
        *,
        http: HttpClient | None = None,
        transports: Mapping[Platform, Transport] | None = None,
        base_urls: Mapping[Platform, str] | None = None,
        discord_gateway_url: str | None = None,
        slack_presence_base_url: str | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._config = config
        self._store = store
        self._http = http if http is not None else HttpClient()
        self._injected: dict[Platform, Transport] = dict(transports or {})
        self._base_urls: dict[Platform, str] = dict(base_urls or {})
        self._discord_gateway_url = discord_gateway_url
        self._slack_presence_base_url = slack_presence_base_url
        self._clock = clock
        self._messengers: dict[Platform, MessengerConfig] = {
            messenger.platform: messenger for messenger in config.messengers
        }
        self._adapters: dict[Platform, Transport] = {}

    def adapter(self, platform: Platform) -> Transport:
        """Return the transport for one enabled, configured platform.

        Raises :class:`MessengerError` when the platform is not configured or is
        disabled. The result is always that platform's adapter, never another
        platform's, so a missing platform cannot silently fall back.
        """
        messenger = self._enabled_config(platform)
        injected = self._injected.get(platform)
        if injected is not None:
            return injected
        cached = self._adapters.get(platform)
        if cached is not None:
            return cached
        adapter = self._build_adapter(messenger)
        self._adapters[platform] = adapter
        return adapter

    def poll(self, platform: Platform | None = None) -> list[str]:
        """Poll enabled platforms for inbound work and return accepted job ids.

        With ``platform`` given, only that enabled platform is polled; otherwise
        every enabled configured platform is polled in configuration order. An
        adapter failure propagates instead of being reported as an empty result.
        """
        if platform is not None:
            return list(self.adapter(platform).poll())
        job_ids: list[str] = []
        for messenger in self._config.messengers:
            if not messenger.enabled:
                continue
            job_ids.extend(self.adapter(messenger.platform).poll())
        return job_ids

    def drain(self) -> list[str]:
        """Attempt every pending delivery once and return the ids that were sent.

        Only deliveries whose platform is configured and enabled are attempted;
        a delivery with no such platform (for example a relay target that is not
        a messenger) is left untouched. :meth:`Store.start_delivery` claims a
        delivery atomically and refuses one whose persisted backoff has not
        elapsed, so an early drain sends nothing. A rate-limit response defers
        the delivery (pending, with backoff); any other failure, ambiguous
        request or unexpected adapter response closes it ``uncertain``. The
        returned ids are exactly the deliveries the platform confirmed with a
        remote id; a failure is never reported as a successful send.
        """
        delivered: list[str] = []
        for delivery in self._store.deliveries():
            if delivery.status != "pending":
                continue
            platform = delivery.platform
            if platform is None or not self._platform_enabled(platform):
                continue
            if not self._store.start_delivery(delivery.id, now=self._clock()):
                continue
            transport = self.adapter(platform)
            try:
                remote_id = transport.send(delivery)
            except TransportError as exc:
                self._resolve_transport_error(delivery.id, exc)
                continue
            except Exception:
                self._store.finish_delivery(delivery.id, "uncertain")
                continue
            self._store.finish_delivery(delivery.id, "sent", remote_id)
            delivered.append(delivery.id)
        return delivered

    def send(
        self,
        platform: Platform,
        channel: str,
        text: str,
        thread_id: str | None = None,
        reply_to: str | None = None,
    ) -> list[str]:
        """Durably enqueue ``text`` as pending outbox deliveries and return their ids.

        The platform must be enabled and ``channel`` must be an allowed target.
        Text is split into bounded chunks stored as one ordered delivery group,
        preserving ``thread_id`` and ``reply_to`` separately. This method writes
        only durable state: no network request is made, so the returned ids are
        the truthful record that the message is queued. Call :meth:`drain` to
        attempt delivery.
        """
        messenger = self._enabled_config(platform)
        self._require_channel(messenger, channel)
        group = str(uuid.uuid4())
        delivery_ids: list[str] = []
        for chunk in chunks(text):
            delivery_ids.append(
                self._store.add_delivery(
                    platform,
                    channel,
                    chunk,
                    reply_to=reply_to,
                    thread_id=thread_id,
                    job_id=group,
                )
            )
        return delivery_ids

    def edit(self, platform: Platform, channel: str, message_id: str, text: str) -> str:
        """Replace one message's text and return the remote id."""
        messenger = self._enabled_config(platform)
        self._require_channel(messenger, channel)
        return self.adapter(platform).edit(channel, message_id, text)

    def typing(self, platform: Platform, channel: str, thread_id: str | None = None) -> None:
        """Mark typing on ``channel`` (or ``thread_id``) for a supported platform.

        Slack has no typing indicator for this transport, so the call raises
        :class:`CapabilityError` rather than pretending anything was sent.
        """
        messenger = self._enabled_config(platform)
        self._require_channel(messenger, channel)
        if not _supports_typing(platform):
            raise CapabilityError("slack does not support typing indicators")
        self.adapter(platform).typing(channel, thread_id)

    def presence(self, platform: Platform) -> PresenceSession:
        """Return that platform's existing presence session context manager.

        Telegram has no presence implementation, so the call raises
        :class:`CapabilityError`. The returned :class:`PresenceSession` is a real
        context manager; entering it opens the bounded session and exiting it
        stops and reaps the worker.
        """
        messenger = self._enabled_config(platform)
        if not _supports_presence(platform):
            raise CapabilityError("telegram does not support presence")
        slack_base_url = self._slack_presence_base_url or SLACK_API_BASE_URL
        return PresenceSession(
            messenger,
            self._http,
            gateway_url=self._discord_gateway_url,
            slack_base_url=slack_base_url,
        )

    def identity(self, platform: Platform) -> str:
        """Return the adapter-confirmed remote identity for one platform."""
        return self.adapter(platform).identity()

    def doctor(self) -> tuple[PlatformStatus, ...]:
        """Return configured, non-secret status for every messenger entry.

        Includes disabled entries so a doctor command can report them, and never
        contains a token or cookie value: only the environment variable names.
        """
        return tuple(
            PlatformStatus(
                platform=messenger.platform,
                enabled=messenger.enabled,
                typing=_supports_typing(messenger.platform),
                presence=_supports_presence(messenger.platform),
                token_env=messenger.token_env,
                cookie_env=messenger.cookie_env,
                users=messenger.users,
                channels=messenger.channels,
                threads=messenger.threads,
            )
            for messenger in self._config.messengers
        )

    def _build_adapter(self, messenger: MessengerConfig) -> Transport:
        platform = messenger.platform
        base_url = self._base_urls.get(platform, _DEFAULT_BASE_URLS[platform])
        if platform == "discord":
            return DiscordTransport(
                messenger, self._store, self._http, base_url=base_url, clock=self._clock
            )
        if platform == "slack":
            return SlackTransport(
                messenger, self._store, self._http, base_url=base_url, clock=self._clock
            )
        return TelegramTransport(
            messenger, self._store, self._http, base_url=base_url, clock=self._clock
        )

    def _resolve_transport_error(self, delivery_id: str, exc: TransportError) -> None:
        """Honest recovery: defer only an explicit retry delay, else be uncertain."""
        retry_after = exc.retry_after
        if retry_after is not None and not exc.ambiguous:
            self._store.defer_delivery(delivery_id, self._clock() + retry_after)
            return
        self._store.finish_delivery(delivery_id, "uncertain")

    def _enabled_config(self, platform: Platform) -> MessengerConfig:
        messenger = self._messengers.get(platform)
        if messenger is None or not messenger.enabled:
            raise MessengerError(
                f"platform {platform!r} is not configured or is disabled"
            )
        return messenger

    def _platform_enabled(self, platform: Platform) -> bool:
        messenger = self._messengers.get(platform)
        return messenger is not None and messenger.enabled

    @staticmethod
    def _require_channel(messenger: MessengerConfig, channel: str) -> None:
        if channel not in messenger.channels:
            raise MessengerError(
                f"channel {channel!r} is not an allowed target for "
                f"platform {messenger.platform!r}"
            )


__all__ = [
    "CapabilityError",
    "Messenger",
    "MessengerError",
    "PlatformStatus",
]
