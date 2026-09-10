"""A bus subscriber that a test feeds by hand; it satisfies ``tape.bus.ports.Subscriber``.

Messages pushed with :meth:`FakeSubscriber.push` are yielded by ``messages()`` in order, and
``close()`` ends the stream after whatever was pushed before it, as closing a ZeroMQ subscriber
ends its stream.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from tape.errors import BusError

__all__ = ["FakeSubscriber"]


class FakeSubscriber:
    """Yields pushed ``(topic, payload)`` pairs until closed."""

    def __init__(self) -> None:
        self.prefixes: list[bytes] = []
        self.closes = 0
        self._queue: asyncio.Queue[tuple[bytes, bytes] | None] = asyncio.Queue()

    def subscribe(self, topic_prefix: bytes) -> None:
        if self.closes:
            raise BusError("subscriber is closed")
        self.prefixes.append(topic_prefix)

    async def messages(self) -> AsyncIterator[tuple[bytes, bytes]]:
        while (message := await self._queue.get()) is not None:
            yield message

    def push(self, payload: bytes, topic: bytes = b"md.TEST") -> None:
        self._queue.put_nowait((topic, payload))

    def close(self) -> None:
        self.closes += 1
        if self.closes == 1:
            self._queue.put_nowait(None)
