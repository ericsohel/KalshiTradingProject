"""Bus envelopes: topics, encoding round trips, and numbering that never raises."""

from __future__ import annotations

import logging
from typing import Final

import msgspec
import pytest

from tape.bus import (
    CATALOG_TOPIC,
    GAP_TOPIC,
    LIFECYCLE_TOPIC,
    STATUS_TOPIC,
    BusEnvelope,
    PublisherStats,
    SequencedPublisher,
    decode_bus_envelope,
    encode_bus_envelope,
    topic_for,
)
from tape.errors import WireError
from tape.events import (
    BookDelta,
    BookRefresh,
    BookSnapshot,
    BusEvent,
    CatalogEntry,
    ConnectionReport,
    GapEvent,
    Level,
    Lifecycle,
    MarketCatalog,
    Receipt,
    Side,
    StatusReport,
    Ticker,
    Trade,
)
from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import Ms, Ns
from tests.fakes import RecordingPublisher

EPOCH: Final = 1_789_000_000_000_000_000
RECEIPT: Final = Receipt(conn_id=2, recv_mono_ns=Ns(5), recv_wall_ns=Ns(EPOCH + 5))


def lvl(price: int, count: int) -> Level:
    return Level(PriceE4(price), CountE2(count))


SNAPSHOT: Final = BookSnapshot(
    ticker="KXA-1",
    ts_ms=Ms(1),
    receipt=RECEIPT,
    sid=1,
    seq=1,
    bids=(lvl(4000, 100),),
    asks=(lvl(6000, 50),),
)
DELTA: Final = BookDelta(
    ticker="KXA-1",
    ts_ms=Ms(2),
    receipt=RECEIPT,
    sid=1,
    seq=2,
    side=Side.ASK,
    price=PriceE4(6000),
    delta=-25,
)
TRADE: Final = Trade(
    ticker="KXA-1",
    ts_ms=Ms(3),
    receipt=RECEIPT,
    sid=2,
    seq=1,
    trade_id="t-1",
    price=PriceE4(6000),
    count=CountE2(25),
    taker_side=Side.BID,
    is_block=False,
)
TICKER: Final = Ticker(
    ticker="KXB-2",
    ts_ms=Ms(4),
    receipt=RECEIPT,
    sid=1,
    last=None,
    bid=PriceE4(100),
    ask=None,
    bid_size=CountE2(1),
    ask_size=None,
    volume=CountE2(700),
    open_interest=CountE2(0),
)
LIFECYCLE: Final = Lifecycle(
    ticker="KXC-3",
    receipt=RECEIPT,
    sid=1,
    seq=9,
    event_type="settled",
    payload_json='{"market_ticker":"KXC-3"}',
)
GAP: Final = GapEvent(receipt=RECEIPT, sid=1, expected_seq=3, got_seq=5)
REFRESH: Final = BookRefresh(
    ticker="KXA-1",
    ts_ms=None,
    receipt=RECEIPT,
    stale=True,
    bids=(lvl(4000, 100), lvl(3900, 1)),
    asks=(),
)
CATALOG: Final = MarketCatalog(
    markets=(
        CatalogEntry(
            ticker="KXA-1",
            series_ticker="KXA",
            event_ticker="KXA",
            volume_24h=CountE2(500_000),
            close_ts=1_800_000_000,
            showcase=True,
        ),
        CatalogEntry(
            ticker="KXB-2",
            series_ticker="KXB",
            event_ticker="KXB",
            volume_24h=CountE2(0),
            close_ts=None,
            showcase=False,
        ),
    )
)
STATUS: Final = StatusReport(
    interval_s=60,
    universe_size=2,
    subscribed_markets=1,
    connections=(
        ConnectionReport(
            conn_id=0, taped=False, frames=9, gaps=0, reconnects=1, stale_books=0, sink_dropped=0
        ),
    ),
)
EVENTS: Final[tuple[BusEvent, ...]] = (
    SNAPSHOT,
    DELTA,
    TRADE,
    TICKER,
    LIFECYCLE,
    GAP,
    REFRESH,
    CATALOG,
    STATUS,
)


@pytest.mark.parametrize(
    ("event", "topic"),
    [
        (SNAPSHOT, b"md.KXA-1"),
        (DELTA, b"md.KXA-1"),
        (TRADE, b"md.KXA-1"),
        (TICKER, b"md.KXB-2"),
        (REFRESH, b"md.KXA-1"),
        (LIFECYCLE, LIFECYCLE_TOPIC),
        (GAP, GAP_TOPIC),
        (CATALOG, CATALOG_TOPIC),
        (STATUS, STATUS_TOPIC),
    ],
    ids=lambda value: type(value).__name__ if not isinstance(value, bytes) else value.decode(),
)
def test_market_data_goes_to_its_ticker_s_topic_and_control_events_to_their_own(
    event: BusEvent, topic: bytes
) -> None:
    assert topic_for(event) == topic


@pytest.mark.parametrize("event", EVENTS, ids=lambda event: type(event).__name__)
def test_an_envelope_round_trips_every_event_type(event: BusEvent) -> None:
    envelope = BusEnvelope(bus_epoch=EPOCH, bus_seq=7, event=event)
    decoded = decode_bus_envelope(encode_bus_envelope(envelope))
    assert decoded == envelope
    assert type(decoded.event) is type(event)


def envelope_bytes(**fields: object) -> bytes:
    return msgspec.msgpack.encode(
        {"bus_epoch": EPOCH, "bus_seq": 1, "event": msgspec.to_builtins(GAP)} | fields
    )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"", "bad bus envelope"),
        (b"\xc1", "bad bus envelope"),
        (msgspec.msgpack.encode([1, 2, 3]), "Expected `object`"),
        (envelope_bytes(bus_seq=0), r"Expected `int` >= 1 - at `\$\.bus_seq`"),
        (envelope_bytes(bus_epoch=-1), r"Expected `int` >= 0 - at `\$\.bus_epoch`"),
        (envelope_bytes(event={"type": "Surprise"}), r"Invalid value 'Surprise' - at `\$\.event"),
    ],
)
def test_a_payload_that_is_not_an_envelope_is_a_wire_error(payload: bytes, message: str) -> None:
    with pytest.raises(WireError, match=message):
        decode_bus_envelope(payload)


def test_events_are_numbered_from_one_in_the_publisher_s_epoch() -> None:
    recording = RecordingPublisher()
    bus = SequencedPublisher(recording, epoch=EPOCH)
    assert (bus.epoch, bus.last_seq) == (EPOCH, 0)

    for event in EVENTS:
        bus.publish(event)

    envelopes = recording.envelopes
    assert [envelope.bus_seq for envelope in envelopes] == list(range(1, len(EVENTS) + 1))
    assert {envelope.bus_epoch for envelope in envelopes} == {EPOCH}
    assert tuple(envelope.event for envelope in envelopes) == EVENTS
    assert recording.topics == [topic_for(event) for event in EVENTS]
    assert bus.last_seq == len(EVENTS)
    assert bus.stats == PublisherStats(sent=len(EVENTS), dropped=0, errors=0)
    bus.close()
    assert recording.closes == 1


def test_a_message_that_cannot_be_sent_or_encoded_spends_its_number_and_never_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    recording = RecordingPublisher()
    bus = SequencedPublisher(recording, epoch=EPOCH)

    bus.publish(SNAPSHOT)
    recording.failure = RuntimeError("socket bug")
    bus.publish(DELTA)
    bus.publish(TRADE)
    recording.failure = None
    # MessagePack has no integer above 64 bits, so this event cannot be encoded.
    bus.publish(msgspec.structs.replace(TICKER, volume=CountE2(2**64)))
    bus.publish(GAP)

    assert [envelope.bus_seq for envelope in recording.envelopes] == [1, 5]
    stats = bus.stats
    assert stats == PublisherStats(sent=2, dropped=0, errors=3)
    assert stats.sent + stats.dropped + stats.errors == bus.last_seq
    (logged,) = [r for r in caplog.records if r.getMessage().startswith("bus message not")]
    assert logged.levelno == logging.ERROR
    assert logged.__dict__["bus_seq"] == 2


def test_an_epoch_is_a_wall_clock_reading() -> None:
    with pytest.raises(ValueError, match="epoch must be non-negative"):
        SequencedPublisher(RecordingPublisher(), epoch=-1)
