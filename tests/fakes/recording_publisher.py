"""A bus publisher that keeps every message in memory, or breaks its contract on request.

It satisfies ``tape.bus.ports.Publisher`` so recorder and envelope tests can read exactly what
was published, decoded, without a socket. Setting :attr:`RecordingPublisher.failure` makes every
later ``publish`` raise it, which a real publisher never does, to prove that nothing on the bus
can reach the code that publishes.
"""

from __future__ import annotations

from collections.abc import Callable

from tape.bus import BusEnvelope, PublisherStats, decode_bus_envelope

__all__ = ["RecordingPublisher"]


class RecordingPublisher:
    """Records ``(topic, payload)`` pairs; counts what it kept as sent.

    Args:
        on_close: Called on the first :meth:`close`, for tests that check what else had
            already stopped by then.
    """

    def __init__(self, *, on_close: Callable[[], None] | None = None) -> None:
        self.messages: list[tuple[bytes, bytes]] = []
        self.failure: Exception | None = None
        self.closes = 0
        self._on_close = on_close

    @property
    def stats(self) -> PublisherStats:
        return PublisherStats(sent=len(self.messages), dropped=0, errors=0)

    @property
    def envelopes(self) -> list[BusEnvelope]:
        """Every payload kept, decoded, in publishing order."""
        return [decode_bus_envelope(payload) for _, payload in self.messages]

    @property
    def topics(self) -> list[bytes]:
        return [topic for topic, _ in self.messages]

    def publish(self, topic: bytes, payload: bytes) -> None:
        if self.failure is not None:
            raise self.failure
        self.messages.append((topic, payload))

    def close(self) -> None:
        self.closes += 1
        if self.closes == 1 and self._on_close is not None:
            self._on_close()
