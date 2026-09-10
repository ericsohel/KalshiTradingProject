"""The bus message: a numbered envelope around one event, its topic, and its encoding.

Responsibility: define what travels on the bus (docs/DATA_FORMATS.md 10, ADR 0022) and number
it. Every payload is a :class:`BusEnvelope`: ``bus_epoch`` is the publisher's start time in
wall nanoseconds, ``bus_seq`` counts from 1 every message the publisher attempts, and
``event`` is one :data:`tape.events.MarketEvent`. :class:`SequencedPublisher` assigns both and
hands the encoded envelope to a :class:`tape.bus.ports.Publisher`. Topics are ``md.<ticker>``
for snapshots, deltas, refresh images, trades, and ticker updates, ``ctl.lifecycle`` for
lifecycle events, and ``ctl.gap`` for sequence gaps.

Payloads are MessagePack, encoded by msgspec from the same tagged structs the recorder uses.
The bus is local and read only by Python consumers built on those structs, so a binary form
costs no interoperability, and it is smaller and faster to encode than JSON for the refresh
images that dominate the traffic. Public API formats stay JSON (docs/FRONTEND.md 4).

Invariants: ``bus_seq`` advances by exactly one per attempted message, including one that fails
to encode or to send, so contiguous numbers within an epoch mean nothing was lost; publishing
never raises; the numbers are shared by every topic, so only a consumer that subscribes to
every topic can tell loss from filtering.
"""

from __future__ import annotations

import logging
from typing import Annotated, Final

import msgspec

from tape.bus.ports import Publisher, PublisherStats
from tape.errors import WireError
from tape.events import GapEvent, Lifecycle, MarketEvent

__all__ = [
    "CONTROL_PREFIX",
    "FIRST_BUS_SEQ",
    "GAP_TOPIC",
    "LIFECYCLE_TOPIC",
    "MARKET_DATA_PREFIX",
    "BusEnvelope",
    "SequencedPublisher",
    "decode_bus_envelope",
    "encode_bus_envelope",
    "topic_for",
]

MARKET_DATA_PREFIX: Final = b"md."
"""Prefix of every per-market topic; the ticker follows it."""

CONTROL_PREFIX: Final = b"ctl."
"""Prefix of every topic that is not about one market's data."""

LIFECYCLE_TOPIC: Final = b"ctl.lifecycle"
GAP_TOPIC: Final = b"ctl.gap"

FIRST_BUS_SEQ: Final = 1
"""The ``bus_seq`` of an epoch's first message."""

_encoder: Final = msgspec.msgpack.Encoder()


class BusEnvelope(msgspec.Struct, frozen=True, kw_only=True):
    """One bus message: an event and the numbers that let a consumer notice loss.

    Attributes:
        bus_epoch: The publisher's start time in wall nanoseconds; a new value means the
            publisher restarted and every earlier number is void.
        bus_seq: Position of the message among every message the publisher attempted in this
            epoch, from :data:`FIRST_BUS_SEQ`.
        event: The event.
    """

    bus_epoch: Annotated[int, msgspec.Meta(ge=0)]
    bus_seq: Annotated[int, msgspec.Meta(ge=FIRST_BUS_SEQ)]
    event: MarketEvent


_envelope_decoder: Final = msgspec.msgpack.Decoder(BusEnvelope)


def topic_for(event: MarketEvent) -> bytes:
    """Return the topic an event is published on.

    Args:
        event: Any bus event.

    Returns:
        ``ctl.lifecycle`` for a lifecycle event, ``ctl.gap`` for a gap, and ``md.<ticker>``
        for everything else.
    """
    if isinstance(event, Lifecycle):
        return LIFECYCLE_TOPIC
    if isinstance(event, GapEvent):
        return GAP_TOPIC
    return MARKET_DATA_PREFIX + event.ticker.encode()


def encode_bus_envelope(envelope: BusEnvelope) -> bytes:
    """Encode an envelope as a bus payload.

    Args:
        envelope: The envelope.

    Returns:
        MessagePack bytes that :func:`decode_bus_envelope` reads back as an equal envelope.
    """
    return _encoder.encode(envelope)


def decode_bus_envelope(payload: bytes) -> BusEnvelope:
    """Decode a bus payload.

    Args:
        payload: Bytes received on the bus.

    Returns:
        The envelope.

    Raises:
        WireError: If the payload is not a well-formed envelope of a known event type.
    """
    try:
        return _envelope_decoder.decode(payload)
    except msgspec.DecodeError as exc:
        # msgspec.ValidationError is a DecodeError, so this covers malformed and mistyped alike.
        raise WireError(f"bad bus envelope: {exc}") from exc


class SequencedPublisher:
    """Numbers events, encodes them, and publishes them, never raising. See the module docstring.

    Not thread-safe; used on one event loop.

    Args:
        publisher: The transport. :meth:`close` closes it.
        epoch: ``bus_epoch`` of every message: the publisher's start time in wall nanoseconds.
        logger: Destination for logs; defaults to this module's logger.

    Raises:
        ValueError: If ``epoch`` is negative.
    """

    def __init__(
        self, publisher: Publisher, *, epoch: int, logger: logging.Logger | None = None
    ) -> None:
        if epoch < 0:
            raise ValueError(f"epoch must be non-negative, got {epoch}")
        self._publisher = publisher
        self._epoch = epoch
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._last_seq = FIRST_BUS_SEQ - 1
        self._failures = 0

    @property
    def epoch(self) -> int:
        """The ``bus_epoch`` of every message this publisher sends."""
        return self._epoch

    @property
    def last_seq(self) -> int:
        """The ``bus_seq`` of the latest attempted message; zero before the first."""
        return self._last_seq

    @property
    def stats(self) -> PublisherStats:
        """The transport's counters, with messages that failed before reaching it as errors.

        ``sent + dropped + errors == last_seq`` for a transport that keeps its contract.
        """
        stats = self._publisher.stats
        return msgspec.structs.replace(stats, errors=stats.errors + self._failures)

    def publish(self, event: MarketEvent) -> None:
        """Publish one event under the next ``bus_seq``, or count it as failed.

        Args:
            event: The event to publish.
        """
        self._last_seq += 1
        envelope = BusEnvelope(bus_epoch=self._epoch, bus_seq=self._last_seq, event=event)
        try:
            self._publisher.publish(topic_for(event), encode_bus_envelope(envelope))
        except Exception as exc:
            # Neither can fail by contract. If one does, the message is lost like any other:
            # its number stays spent, so consumers see a gap, and nothing reaches the recorder.
            self._failures += 1
            if self._failures == 1:
                self._log.exception(
                    "bus message not published; later failures are only counted",
                    extra={"bus_seq": self._last_seq, "error": repr(exc)},
                )

    def close(self) -> None:
        """Close the transport. Idempotent."""
        self._publisher.close()
