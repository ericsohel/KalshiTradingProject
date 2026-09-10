"""The bus's ports: what a publisher and a subscriber promise, whatever carries the bytes.

Responsibility: define the seam between the recorder, its consumers, and the transport
(ADR 0008). A :class:`Publisher` takes one topic and one payload and never blocks or raises;
a :class:`Subscriber` yields ``(topic, payload)`` pairs for the topic prefixes it asked for.
Neither knows what a payload means; :mod:`tape.bus.envelope` does. ZeroMQ implements both in
:mod:`tape.bus.sockets`, and a replacement transport would implement them again.

Invariants: a publisher's ``sent + dropped + errors`` equals the number of ``publish`` calls
it has received, so every attempted message is accounted for exactly once.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

import msgspec

__all__ = ["Publisher", "PublisherStats", "Subscriber"]


class PublisherStats(msgspec.Struct, frozen=True, kw_only=True):
    """What became of every message a publisher was asked to send.

    Attributes:
        sent: Messages handed to the transport. On ZeroMQ PUB/SUB a message handed over can
            still be lost at one slow subscriber, which that subscriber sees as a sequence gap.
        dropped: Messages the transport refused because it was full.
        errors: Messages that failed for any other reason, including a publisher already closed.
    """

    sent: int
    dropped: int
    errors: int


class Publisher(Protocol):
    """Sends topic-prefixed messages to whoever subscribes, without ever waiting for them."""

    @property
    def stats(self) -> PublisherStats:
        """Counters since the publisher was opened."""
        ...

    def publish(self, topic: bytes, payload: bytes) -> None:
        """Send one message now, or count why it could not be sent.

        Never blocks and never raises: nothing on the bus may stall or stop the process
        that publishes (docs/ARCHITECTURE.md 3.6).

        Args:
            topic: Routing prefix a subscriber filters on, for example ``b"md.KXA-1"``.
            payload: The encoded message.
        """
        ...

    def close(self) -> None:
        """Release the transport, discarding anything unsent. Idempotent."""
        ...


class Subscriber(Protocol):
    """Receives the messages of the topic prefixes it subscribes to."""

    def subscribe(self, topic_prefix: bytes) -> None:
        """Receive every message whose topic starts with ``topic_prefix``; ``b""`` is everything.

        Raises:
            BusError: If the subscription cannot be registered, for example after closing.
        """
        ...

    def messages(self) -> AsyncIterator[tuple[bytes, bytes]]:
        """Yield ``(topic, payload)`` in arrival order until the subscriber is closed.

        Raises:
            BusError: If receiving fails for a reason other than closing.
        """
        ...

    def close(self) -> None:
        """Stop receiving and release the transport. Idempotent."""
        ...
