"""The shared wire contract used by the unified messenger."""

from typing import Protocol

from ultra_agent.models import Delivery


class Transport(Protocol):
    """A configured transport; policy and delivery retries belong to its caller."""

    def identity(self) -> str: ...

    def poll(self) -> list[str]: ...

    def send(self, delivery: Delivery, /) -> str: ...

    def edit(self, channel: str, message_id: str, text: str, /) -> str: ...

    def typing(self, channel: str, /, thread_id: str | None = None) -> None: ...
