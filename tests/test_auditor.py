"""Auditor: window classification, REST batching, staleness, sampling, records, and statistics."""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Final, Literal, cast

import httpx
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tape.book import Book
from tape.client import NullRateLimiter
from tape.client.rest import KalshiRest
from tape.events import BookDelta, BookSnapshot, Level, Receipt, Side
from tape.fixedpoint import CountE2, PriceE4
from tape.recorder.auditor import (
    Auditor,
    AuditOutcome,
    AuditResult,
    AuditStats,
    WindowVerdict,
    classify_window,
    round_robin_choice,
)
from tape.recorder.tap import (
    FAULT_OVERFLOW,
    BookChange,
    BookImage,
    BookTap,
    BookTapOpener,
    LiveBookTap,
    TapWindow,
)
from tape.recorder.writer import RecordSink
from tape.segment import Record, RecordKind
from tape.timeutil import NS_PER_MS, NS_PER_S, FrozenClock, Ns

BASE_URL: Final = "https://api.example.com/trade-api/v2"
LEAD_NS: Final = 250 * NS_PER_MS
SETTLE_NS: Final = 750 * NS_PER_MS
FLIGHT_NS: Final = 100 * NS_PER_MS
"""How long the fake exchange takes to answer a REST request."""
STEP_NS: Final = NS_PER_MS
"""Spacing of scripted book changes inside a phase."""
OPEN_MONO_NS: Final = 5 * NS_PER_S
WALL_OFFSET_NS: Final = 1_700_000_000 * NS_PER_S

type Phase = Literal["lead", "flight", "settle"]


def lvl(price: int, count: int) -> Level:
    """Build a ``Level`` from plain ints, matching ``tests/test_book.py``'s helper."""
    return Level(PriceE4(price), CountE2(count))


def local_book(
    ticker: str, bids: Iterable[Level], asks: Iterable[Level], *, stale: bool = False
) -> Book:
    """Build a local ``Book`` already past its first snapshot, optionally marked stale."""
    book = Book(ticker)
    book.apply_snapshot(bids, asks, ts_ms=None)
    if stale:
        book.mark_stale()
    return book


def standard_book(ticker: str) -> Book:
    """A YES bid of 1.00 at 0.3000 and a YES ask of 0.70 at 0.6000."""
    return local_book(ticker, [lvl(3000, 100)], [lvl(6000, 70)])


def standard(bid_count: str) -> tuple[list[list[str]], list[list[str]]]:
    """REST sides for :func:`standard_book` with the 0.3000 bid resting ``bid_count``."""
    return [["0.3000", bid_count]], [["0.4000", "0.70"]]


class Feed:
    """The live side of an audit: local books, the taps open over them, and scripted changes.

    A change scheduled for a phase (the lead wait, the REST round trip, or the settle wait) is
    applied to its live book one millisecond after the previous one and handed to every open
    tap, as a supervisor would; an action runs arbitrary code at that point instead. Each
    phase's script runs once. ``log`` records the order of taps, waits, and requests.
    """

    def __init__(self, *books: Book, tap_books: Mapping[str, Book] | None = None) -> None:
        self.clock = FrozenClock(mono_ns=OPEN_MONO_NS, wall_ns=OPEN_MONO_NS + WALL_OFFSET_NS)
        self.books = {book.ticker: book for book in books}
        self.tap_books = self.books if tap_books is None else tap_books
        self.taps: list[LiveBookTap] = []
        self.opened: list[tuple[frozenset[str], int]] = []
        self.log: list[str] = []
        self._script: dict[Phase, list[Callable[[], None]]] = {
            "lead": [],
            "flight": [],
            "settle": [],
        }

    def during(self, phase: Phase, ticker: str, change: int) -> None:
        """Schedule a change to the 0.3000 bid of ``ticker``."""
        self._script[phase].append(lambda: self._apply(ticker, change))

    def act(self, phase: Phase, action: Callable[[], None]) -> None:
        self._script[phase].append(action)

    def open_tap(self, tickers: Collection[str], *, max_events: int) -> LiveBookTap:
        self.log.append("open")
        tap = LiveBookTap(
            tickers, books=self.tap_books, max_events=max_events, on_close=self._released
        )
        self.taps.append(tap)
        self.opened.append((tap.tickers, max_events))
        return tap

    async def sleep(self, seconds: float) -> None:
        phase: Phase = "settle" if self.log[-1:] == ["request"] else "lead"
        self.log.append(f"sleep {phase}")
        self.run(phase, round(seconds * NS_PER_S))

    def run(self, phase: Phase, duration_ns: int) -> None:
        started_ns = self.clock.mono_ns()
        steps, self._script[phase] = self._script[phase], []
        for step in steps:
            self.clock.advance(STEP_NS)
            step()
        self.clock.advance(started_ns + duration_ns - self.clock.mono_ns())

    def _apply(self, ticker: str, change: int) -> None:
        receipt = Receipt(
            conn_id=3, recv_mono_ns=self.clock.mono_ns(), recv_wall_ns=self.clock.wall_ns()
        )
        delta = BookDelta(
            ticker=ticker,
            ts_ms=None,
            receipt=receipt,
            sid=1,
            seq=None,
            side=Side.BID,
            price=PriceE4(3000),
            delta=change,
        )
        assert self.books[ticker].apply_delta(delta.side, delta.price, delta.delta, ts_ms=None)
        for tap in self.taps:
            tap.record(delta, was_stale=False)

    def _released(self, tap: LiveBookTap) -> None:
        self.log.append("close")
        self.taps.remove(tap)


class FakeSink:
    """An in-memory ``RecordSink``: records every ``Record`` instead of touching disk."""

    def __init__(self, conn_id: int = 3) -> None:
        self.records: list[Record] = []
        self._conn_id = conn_id

    @property
    def conn_id(self) -> int:
        """Connection this fake stands in for; audit records must carry it."""
        return self._conn_id

    def put(self, record: Record) -> bool:
        """Record ``record`` and report success, like a sink with room to spare."""
        self.records.append(record)
        return True

    def payloads(self) -> list[dict[str, object]]:
        return [json.loads(record.payload) for record in self.records]


class OrderbooksRouter:
    """Answers ``GET /markets/orderbooks`` with queued responses, each taking ``FLIGHT_NS``."""

    def __init__(self, feed: Feed) -> None:
        self.requests: list[httpx.Request] = []
        self._feed = feed
        self._responses: list[httpx.Response] = []

    def queue(self, response: httpx.Response) -> None:
        """Queue the response to return for the next call."""
        self._responses.append(response)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self._feed.log.append("request")
        self._feed.run("flight", FLIGHT_NS)
        if not self._responses:
            raise AssertionError("no queued response for GET /markets/orderbooks")
        return self._responses.pop(0)


def orderbooks_response(
    entries: Mapping[str, tuple[list[list[str]], list[list[str]]]],
) -> httpx.Response:
    """Build a ``GET /markets/orderbooks`` 200 response from ``{ticker: (yes, no)}``."""
    return httpx.Response(
        200,
        json={
            "orderbooks": [
                {"ticker": ticker, "orderbook_fp": {"yes_dollars": yes, "no_dollars": no}}
                for ticker, (yes, no) in entries.items()
            ]
        },
    )


@asynccontextmanager
async def auditing(
    feed: Feed,
    router: OrderbooksRouter,
    *,
    books: Callable[[], Mapping[str, Book]] | None = None,
    sink: FakeSink | None = None,
    sample_size: int = 10,
    tap_max_events: int = 100,
    window_sleep: Callable[[float], Awaitable[None]] | None = None,
    open_tap: BookTapOpener | None = None,
) -> AsyncIterator[Auditor]:
    """Build an ``Auditor`` over ``feed`` and ``router`` on ``httpx.MockTransport``, no network."""

    def sink_for(ticker: str) -> RecordSink | None:
        return sink

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(router), base_url=BASE_URL
    ) as client:
        yield Auditor(
            KalshiRest(BASE_URL, client, NullRateLimiter(), feed.clock),
            (lambda: feed.books) if books is None else books,
            feed.clock,
            sink_for=sink_for,
            open_tap=feed.open_tap if open_tap is None else open_tap,
            sample_size=sample_size,
            lead_ns=LEAD_NS,
            settle_ns=SETTLE_NS,
            tap_max_events=tap_max_events,
            window_sleep=feed.sleep if window_sleep is None else window_sleep,
        )


async def audit_one(
    feed: Feed, bid_count: str, *, sink: FakeSink | None = None, tap_max_events: int = 100
) -> AuditResult:
    """Audit the one book of ``feed`` against a REST book whose 0.3000 bid rests ``bid_count``."""
    (ticker,) = feed.books
    router = OrderbooksRouter(feed)
    router.queue(orderbooks_response({ticker: standard(bid_count)}))
    async with auditing(feed, router, sink=sink, tap_max_events=tap_max_events) as auditor:
        (result,) = await auditor.audit_once()
    return result


# --- classification over the window ------------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    """Changes to a standard book's 0.3000 bid (base 100) by phase, and the REST bid seen."""

    script: tuple[tuple[Phase, int], ...]
    rest_bid: str
    outcome: AuditOutcome
    match_index: int | None


SCENARIOS: Final = {
    "exact at the reply": Scenario(
        (("lead", 10), ("flight", 10), ("settle", 10)), "1.20", "exact", 2
    ),
    "consistent at the starting copy": Scenario((("lead", 10),), "1.00", "consistent", 0),
    "consistent after k changes": Scenario(
        (("lead", 10), ("lead", 10), ("lead", 10), ("flight", 10)), "1.20", "consistent", 2
    ),
    "consistent only after the reply, within the settle": Scenario(
        (("settle", 10),), "1.10", "consistent", 1
    ),
    "inconsistent": Scenario((("lead", 10), ("settle", 10)), "0.90", "inconsistent", None),
}


def scripted_feed(scenario: Scenario) -> Feed:
    feed = Feed(standard_book("T-1"))
    for phase, change in scenario.script:
        feed.during(phase, "T-1", change)
    return feed


@pytest.mark.parametrize("scenario", SCENARIOS.values(), ids=list(SCENARIOS))
async def test_each_outcome_is_decided_from_the_states_in_the_window(scenario: Scenario) -> None:
    result = await audit_one(scripted_feed(scenario), scenario.rest_bid)
    assert (result.outcome, result.match_index) == (scenario.outcome, scenario.match_index)
    assert result.window_events == len(scenario.script)
    assert result.exact is (scenario.outcome == "exact")


async def test_an_exact_match_compares_equal_at_the_reply() -> None:
    result = await audit_one(Feed(standard_book("T-1")), "1.00")
    assert (result.outcome, result.match_index) == ("exact", 0)
    assert (result.mismatched_levels, result.max_abs_diff_e2) == (0, 0)
    assert (result.levels_rest, result.levels_local) == (2, 2)


async def test_no_side_is_complemented_onto_yes_scale() -> None:
    # A REST no_dollars level at 0.4000 is a YES ask at 1 - 0.4000 = 0.6000
    # (docs/DATA_FORMATS.md 1.3): getting this backwards makes every audit mismatch.
    feed = Feed(local_book("T-3", [], [lvl(6000, 50)]))
    router = OrderbooksRouter(feed)
    router.queue(orderbooks_response({"T-3": ([], [["0.4000", "0.50"]])}))
    async with auditing(feed, router) as auditor:
        (result,) = await auditor.audit_once()
    assert result.outcome == "exact"
    assert (result.levels_rest, result.levels_local) == (1, 1)


async def test_more_changes_than_the_tap_holds_make_the_audit_undecidable() -> None:
    feed = Feed(standard_book("T-1"))
    for _ in range(3):
        feed.during("lead", "T-1", 10)
    result = await audit_one(feed, "1.30", tap_max_events=2)
    assert result.outcome == "undecidable"
    assert (result.window_events, result.match_index) == (2, None)
    assert (result.levels_local, result.mismatched_levels, result.max_abs_diff_e2) == (
        None,
        None,
        None,
    )
    assert feed.opened == [(frozenset({"T-1"}), 2)]


async def test_a_book_that_goes_stale_inside_the_window_makes_the_audit_undecidable() -> None:
    book = standard_book("T-1")
    feed = Feed(book)
    feed.during("lead", "T-1", 10)
    feed.act("settle", book.mark_stale)  # a gap after the reply still spoils the window
    router = OrderbooksRouter(feed)
    router.queue(orderbooks_response({"T-1": standard("1.10")}))
    async with auditing(feed, router) as auditor:
        (result,) = await auditor.audit_once()
    assert result.outcome == "undecidable"
    stats = auditor.stats
    assert (stats.books_undecidable, stats.books_sampled, stats.books_missing_local) == (1, 0, 0)


async def test_a_market_without_a_local_book_when_the_tap_opens_is_undecidable() -> None:
    # The book vanished between choosing it and opening the tap.
    feed = Feed(standard_book("T-5"), tap_books={})
    router = OrderbooksRouter(feed)
    router.queue(orderbooks_response({"T-5": standard("1.00")}))
    async with auditing(feed, router) as auditor:
        (result,) = await auditor.audit_once()
    assert (result.outcome, result.window_events) == ("undecidable", 0)
    stats = auditor.stats
    assert (stats.books_undecidable, stats.books_missing_local, stats.books_sampled) == (1, 1, 0)


class ScriptedTap:
    """A tap whose windows are given in advance."""

    def __init__(self, windows: Mapping[str, TapWindow]) -> None:
        self._windows = windows

    def close(self) -> Mapping[str, TapWindow]:
        return self._windows


async def test_a_window_that_does_not_replay_is_undecidable_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    feed = Feed(standard_book("T-1"))
    impossible = BookDelta(
        ticker="T-1",
        ts_ms=None,
        receipt=Receipt(conn_id=3, recv_mono_ns=Ns(1), recv_wall_ns=Ns(1)),
        sid=1,
        seq=None,
        side=Side.BID,
        price=PriceE4(3000),
        delta=-500,
    )
    broken = TapWindow(
        ticker="T-1",
        start=BookImage.of(standard_book("T-1")),
        events=(impossible,),
        fault=None,
    )

    def open_tap(tickers: Collection[str], *, max_events: int) -> BookTap:
        return ScriptedTap({"T-1": broken})

    router = OrderbooksRouter(feed)
    router.queue(orderbooks_response({"T-1": standard("1.00")}))
    # A tap no supervisor could produce.
    async with auditing(feed, router, open_tap=open_tap) as auditor:
        (result,) = await auditor.audit_once()
    assert result.outcome == "undecidable"
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR] == [
        "audit window did not replay"
    ]


# --- classify_window ------------------------------------------------------------------------


def test_a_checksum_collision_never_passes_an_audit() -> None:
    start = BookImage(bids=(lvl(3000, 100),), asks=(lvl(6000, 70),))
    window = TapWindow(ticker="T-1", start=start, events=(), fault=None)
    # Same level counts on each side, different sizes: only diff can tell them apart.
    colliding = local_book("T-1", [lvl(3000, 90)], [lvl(6000, 70)])

    def collide(book: Book) -> int:
        return 0

    verdict = classify_window(window, colliding, reply_mono_ns=0, checksum=collide)
    assert (verdict.outcome, verdict.match_index) == ("inconsistent", None)
    identical = start.to_book("T-1")
    assert classify_window(window, identical, reply_mono_ns=0, checksum=collide) == WindowVerdict(
        outcome="exact", match_index=0, reply_state=start
    )


def test_a_window_with_a_fault_is_undecidable_and_a_foreign_book_is_refused() -> None:
    start = BookImage(bids=(lvl(3000, 100),), asks=())
    window = TapWindow(ticker="T-1", start=start, events=(), fault=FAULT_OVERFLOW)
    assert classify_window(window, start.to_book("T-1"), reply_mono_ns=0) == WindowVerdict(
        outcome="undecidable", match_index=None, reply_state=None
    )
    with pytest.raises(ValueError, match="cannot classify"):
        classify_window(TapWindow.absent("T-2"), start.to_book("T-1"), reply_mono_ns=0)


BID_PRICES: Final = (1000, 2000, 3000, 4000)
ASK_PRICES: Final = (6000, 7000, 8000, 9000)
TICKER: Final = "T-P"


@st.composite
def side_levels(draw: st.DrawFn, prices: tuple[int, ...]) -> tuple[Level, ...]:
    chosen = draw(st.lists(st.sampled_from(prices), unique=True, max_size=len(prices)))
    return tuple(lvl(price, draw(st.integers(1, 300))) for price in chosen)


@st.composite
def histories(draw: st.DrawFn) -> tuple[TapWindow, list[BookImage]]:
    """A fault-free window and every state it spans, the starting copy first."""
    book = Book(TICKER)
    book.apply_snapshot(draw(side_levels(BID_PRICES)), draw(side_levels(ASK_PRICES)), ts_ms=None)
    states = [BookImage.of(book)]
    events: list[BookChange] = []
    mono_ns = 0
    for _ in range(draw(st.integers(0, 12))):
        mono_ns += draw(st.integers(0, 2))
        receipt = Receipt(conn_id=2, recv_mono_ns=Ns(mono_ns), recv_wall_ns=Ns(mono_ns))
        change: BookChange
        if draw(st.integers(0, 5)) == 0:
            change = BookSnapshot(
                ticker=TICKER,
                ts_ms=None,
                receipt=receipt,
                sid=1,
                seq=None,
                bids=draw(side_levels(BID_PRICES)),
                asks=draw(side_levels(ASK_PRICES)),
            )
            book.apply_snapshot(change.bids, change.asks, ts_ms=None)
        else:
            side = draw(st.sampled_from(Side))
            price = PriceE4(draw(st.sampled_from(BID_PRICES if side is Side.BID else ASK_PRICES)))
            amount = draw(st.integers(-book.size_at(side, price), 50).filter(bool))
            change = BookDelta(
                ticker=TICKER,
                ts_ms=None,
                receipt=receipt,
                sid=1,
                seq=None,
                side=side,
                price=price,
                delta=amount,
            )
            assert book.apply_delta(side, price, amount, ts_ms=None)
        events.append(change)
        states.append(BookImage.of(book))
    return TapWindow(ticker=TICKER, start=states[0], events=tuple(events), fault=None), states


@given(data=st.data())
@settings(max_examples=300)
def test_a_rest_book_equal_to_any_state_in_its_window_is_never_inconsistent(
    data: st.DataObject,
) -> None:
    window, states = data.draw(histories())
    matched = data.draw(st.sampled_from(states))
    reply_mono_ns = data.draw(st.integers(-1, 30))
    verdict = classify_window(window, matched.to_book(TICKER), reply_mono_ns=reply_mono_ns)
    assert verdict.outcome in {"exact", "consistent"}
    assert verdict.match_index is not None
    assert states[verdict.match_index] == matched


@given(data=st.data())
@settings(max_examples=300)
def test_classification_agrees_with_a_brute_force_search_of_the_window(
    data: st.DataObject,
) -> None:
    window, states = data.draw(histories())
    drawn = data.draw(
        st.one_of(
            st.sampled_from(states),
            st.builds(
                lambda bids, asks: BookImage(bids=bids, asks=asks),
                side_levels(BID_PRICES),
                side_levels(ASK_PRICES),
            ),
        )
    )
    rest_book = drawn.to_book(TICKER)
    target = BookImage.of(rest_book)
    reply_mono_ns = data.draw(st.integers(-1, 30))

    verdict = classify_window(window, rest_book, reply_mono_ns=reply_mono_ns)

    reply_index = sum(
        1
        for _ in itertools.takewhile(
            lambda event: event.receipt.recv_mono_ns <= reply_mono_ns, window.events
        )
    )
    assert verdict.reply_state == states[reply_index]
    matches = [index for index, state in enumerate(states) if state == target]
    if not matches:
        assert (verdict.outcome, verdict.match_index) == ("inconsistent", None)
        return
    nearest = min(matches, key=lambda index: (abs(index - reply_index), index))
    assert verdict.match_index == nearest
    assert verdict.outcome == ("exact" if nearest == reply_index else "consistent")


# --- records --------------------------------------------------------------------------------

COMMON_FIELDS: Final = {
    "ticker",
    "levels_rest",
    "outcome",
    "send_mono_ns",
    "send_wall_ns",
    "window_open_mono_ns",
    "window_close_mono_ns",
    "window_events",
}
DECIDABLE_FIELDS: Final = {"levels_local", "mismatched_levels", "max_abs_diff_e2"}


@pytest.mark.parametrize(
    ("name", "extra_fields"),
    [
        ("exact at the reply", {"match_index"}),
        ("consistent at the starting copy", {"match_index"}),
        ("inconsistent", {"rest_levels", "local_levels"}),
    ],
)
async def test_a_decidable_audit_record_carries_the_fields_of_its_outcome(
    name: str, extra_fields: set[str]
) -> None:
    scenario = SCENARIOS[name]
    sink = FakeSink()
    await audit_one(scripted_feed(scenario), scenario.rest_bid, sink=sink)
    (record,) = sink.records
    assert record.kind == RecordKind.AUDIT
    # Stamped with the owning connection, never a placeholder shared by all connections.
    assert record.conn_id == sink.conn_id
    payload = json.loads(record.payload)
    assert set(payload) == COMMON_FIELDS | DECIDABLE_FIELDS | extra_fields
    assert (payload["outcome"], payload.get("match_index")) == (
        scenario.outcome,
        scenario.match_index,
    )


async def test_an_undecidable_audit_record_carries_only_what_is_known() -> None:
    sink = FakeSink()
    await audit_one(Feed(standard_book("T-1"), tap_books={}), "1.00", sink=sink)
    (payload,) = sink.payloads()
    assert set(payload) == COMMON_FIELDS | {"fault"}
    assert (
        payload["outcome"],
        payload["levels_rest"],
        payload["window_events"],
        payload["fault"],
    ) == ("undecidable", 2, 0, "no_book")


async def test_an_inconsistent_audit_tapes_both_books_as_of_the_reply() -> None:
    sink = FakeSink()
    result = await audit_one(scripted_feed(SCENARIOS["inconsistent"]), "0.90", sink=sink)
    (payload,) = sink.payloads()
    # The local book as of the reply holds the lead change (110), not the settle one.
    assert payload["rest_levels"] == [
        {"side": 0, "price_e4": 3000, "count_e2": 90},
        {"side": 1, "price_e4": 6000, "count_e2": 70},
    ]
    assert payload["local_levels"] == [
        {"side": 0, "price_e4": 3000, "count_e2": 110},
        {"side": 1, "price_e4": 6000, "count_e2": 70},
    ]
    assert (payload["mismatched_levels"], payload["max_abs_diff_e2"]) == (1, 20)
    assert (result.levels_local, result.mismatched_levels, result.max_abs_diff_e2) == (2, 1, 20)


async def test_a_consistent_audit_reports_its_difference_at_the_reply_without_level_lists() -> None:
    sink = FakeSink()
    await audit_one(scripted_feed(SCENARIOS["consistent at the starting copy"]), "1.00", sink=sink)
    (payload,) = sink.payloads()
    assert (payload["match_index"], payload["mismatched_levels"], payload["max_abs_diff_e2"]) == (
        0,
        1,
        10,
    )


async def test_the_window_opens_before_the_lead_and_closes_after_the_settle() -> None:
    feed = Feed(standard_book("T-1"))
    sink = FakeSink()
    result = await audit_one(feed, "1.00", sink=sink)
    assert feed.log == ["open", "sleep lead", "request", "sleep settle", "close"]
    assert feed.taps == []
    send_ns = OPEN_MONO_NS + LEAD_NS
    recv_ns = send_ns + FLIGHT_NS
    close_ns = recv_ns + SETTLE_NS
    assert (
        result.window_open_mono_ns,
        result.send_mono_ns,
        result.recv_mono_ns,
        result.window_close_mono_ns,
    ) == (OPEN_MONO_NS, send_ns, recv_ns, close_ns)
    assert (result.send_wall_ns, result.recv_wall_ns) == (
        send_ns + WALL_OFFSET_NS,
        recv_ns + WALL_OFFSET_NS,
    )
    (record,) = sink.records
    assert (record.recv_mono_ns, record.recv_wall_ns) == (recv_ns, recv_ns + WALL_OFFSET_NS)
    payload = json.loads(record.payload)
    assert (
        payload["window_open_mono_ns"],
        payload["send_mono_ns"],
        payload["send_wall_ns"],
        payload["window_close_mono_ns"],
    ) == (OPEN_MONO_NS, send_ns, send_ns + WALL_OFFSET_NS, close_ns)


async def test_missing_sink_still_counts_the_result() -> None:
    result = await audit_one(Feed(standard_book("T-9")), "1.00")
    assert result.outcome == "exact"


# --- statistics -----------------------------------------------------------------------------


async def test_statistics_partition_audits_and_keep_the_published_ratio_exact() -> None:
    books = [standard_book(ticker) for ticker in ("T-C", "T-E", "T-I", "T-U")]
    feed = Feed(*books, tap_books={book.ticker: book for book in books if book.ticker != "T-U"})
    feed.during("lead", "T-C", 10)
    router = OrderbooksRouter(feed)
    router.queue(
        orderbooks_response(
            {
                "T-C": standard("1.00"),
                "T-E": standard("1.00"),
                "T-I": standard("0.90"),
                "T-U": standard("1.00"),
            }
        )
    )
    async with auditing(feed, router) as auditor:
        results = await auditor.audit_once()
    assert [(r.ticker, r.outcome) for r in results] == [
        ("T-C", "consistent"),
        ("T-E", "exact"),
        ("T-I", "inconsistent"),
        ("T-U", "undecidable"),
    ]
    stats = auditor.stats
    assert stats == AuditStats(
        rounds=1,
        books_sampled=3,
        books_exact=1,
        books_consistent=1,
        books_inconsistent=1,
        books_mismatched=2,
        books_undecidable=1,
        books_skipped_stale=0,
        books_missing_local=1,
        books_invalid_rest=0,
        levels_mismatched=2,
    )
    assert (stats.exact_ratio, stats.consistency_ratio) == ((1, 3), (2, 3))


def stats_with(**counts: int) -> AuditStats:
    return AuditStats(**(dict.fromkeys(AuditStats.__struct_fields__, 0) | counts))


def test_ratios_are_no_data_when_nothing_was_decided() -> None:
    stats = stats_with(rounds=2, books_undecidable=4, books_skipped_stale=1)
    assert (stats.exact_ratio, stats.consistency_ratio) == ((0, 0), (0, 0))


def test_ratios_are_exact_integer_fractions_that_leave_undecidable_audits_out() -> None:
    stats = stats_with(
        books_sampled=10,
        books_exact=6,
        books_consistent=3,
        books_inconsistent=1,
        books_mismatched=4,
        books_undecidable=5,
    )
    assert stats.exact_ratio == (6, 10)
    assert stats.consistency_ratio == (9, 10)
    assert all(isinstance(value, int) for value in (*stats.exact_ratio, *stats.consistency_ratio))


# --- invalid REST snapshots, staleness, batching ---------------------------------------------


@pytest.mark.parametrize(
    "sides",
    [
        ([["0.9000", "1.00"]], [["0.8000", "1.00"]]),  # crossed
        ([["not-a-price", "1.00"]], []),  # malformed
    ],
    ids=["crossed", "malformed"],
)
async def test_an_invalid_rest_snapshot_is_skipped_not_raised(
    sides: tuple[list[list[str]], list[list[str]]],
) -> None:
    feed = Feed(standard_book("T-11"))
    router = OrderbooksRouter(feed)
    router.queue(orderbooks_response({"T-11": sides}))
    async with auditing(feed, router) as auditor:
        results = await auditor.audit_once()
    assert results == ()
    assert (auditor.stats.books_sampled, auditor.stats.books_invalid_rest) == (0, 1)
    assert feed.taps == []


async def test_stale_local_book_is_skipped_and_counted() -> None:
    feed = Feed(local_book("T-4", [lvl(3000, 100)], [lvl(6000, 70)], stale=True))
    router = OrderbooksRouter(feed)
    async with auditing(feed, router) as auditor:
        results = await auditor.audit_once()
    assert results == ()
    assert (auditor.stats.books_skipped_stale, auditor.stats.books_sampled) == (1, 0)
    assert router.requests == []
    assert feed.opened == []


async def test_batches_of_at_most_100_tickers_each_in_their_own_window() -> None:
    feed = Feed(*(local_book(f"T-{i:03d}", [lvl(3000, 1)], [lvl(6000, 1)]) for i in range(250)))
    router = OrderbooksRouter(feed)
    for _ in range(3):
        router.queue(httpx.Response(200, json={"orderbooks": []}))
    async with auditing(feed, router, sample_size=250) as auditor:
        await auditor.audit_once()
    batch_sizes = [len(req.url.params.get_list("tickers")) for req in router.requests]
    assert batch_sizes == [100, 100, 50]
    assert [len(tickers) for tickers, _ in feed.opened] == [100, 100, 50]
    assert feed.log.count("close") == 3
    assert feed.taps == []


async def test_one_failed_batch_closes_its_window_and_does_not_stop_the_others() -> None:
    tickers = [f"T-{i:03d}" for i in range(150)]
    feed = Feed(*(standard_book(t) for t in tickers))
    router = OrderbooksRouter(feed)
    router.queue(httpx.Response(500))
    router.queue(orderbooks_response({t: standard("1.00") for t in sorted(tickers)[100:]}))
    async with auditing(feed, router, sample_size=150) as auditor:
        results = await auditor.audit_once()
    assert len(results) == 50
    assert {result.outcome for result in results} == {"exact"}
    # A failed request has no reply to settle after.
    assert feed.log == [
        "open",
        "sleep lead",
        "request",
        "close",
        *("open", "sleep lead", "request", "sleep settle", "close"),
    ]
    assert auditor.stats.books_sampled == 50


async def test_the_tap_is_closed_even_when_the_window_is_cancelled() -> None:
    feed = Feed(standard_book("T-1"))

    async def cancelled(seconds: float) -> None:
        raise asyncio.CancelledError

    async with auditing(feed, OrderbooksRouter(feed), window_sleep=cancelled) as auditor:
        with pytest.raises(asyncio.CancelledError):
            await auditor.audit_once()
    assert feed.log == ["open", "close"]
    assert feed.taps == []


async def test_successive_rounds_rotate_through_every_ticker() -> None:
    tickers = ["T-A", "T-B", "T-C"]
    feed = Feed(*(local_book(t, [lvl(3000, 100)], [lvl(6000, 70)]) for t in tickers))
    router = OrderbooksRouter(feed)
    for ticker in sorted(tickers):
        router.queue(orderbooks_response({ticker: standard("1.00")}))
    async with auditing(feed, router, sample_size=1) as auditor:
        seen: list[str] = []
        for _ in range(3):
            results = await auditor.audit_once()
            seen.extend(result.ticker for result in results)
    assert sorted(seen) == sorted(tickers)
    assert len(seen) == len(set(seen))


# --- round_robin_choice ------------------------------------------------------------------


def test_round_robin_choice_wraps_and_is_deterministic() -> None:
    chosen1, cursor1 = round_robin_choice(["C", "A", "B"], 2, 0)
    assert chosen1 == ("A", "B")
    assert cursor1 == 2
    chosen2, cursor2 = round_robin_choice(["C", "A", "B"], 2, cursor1)
    assert chosen2 == ("C", "A")
    assert cursor2 == 1


def test_round_robin_choice_on_empty_input() -> None:
    assert round_robin_choice([], 5, 0) == ((), 0)


def test_round_robin_choice_rejects_negative_count() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        round_robin_choice(["A"], -1, 0)


@given(
    tickers=st.lists(st.text(min_size=1, max_size=4), min_size=1, max_size=12, unique=True),
    count=st.integers(min_value=1, max_value=12),
    start_cursor=st.integers(min_value=0, max_value=10_000),
)
@settings(max_examples=200)
def test_round_robin_covers_every_ticker_once_per_rotation_with_no_repeats(
    tickers: list[str], count: int, start_cursor: int
) -> None:
    unique_sorted = sorted(set(tickers))
    total = len(unique_sorted)
    cursor = start_cursor
    flattened: list[str] = []
    while len(flattened) < total:
        chosen, cursor = round_robin_choice(tickers, count, cursor)
        flattened.extend(chosen)
    first_rotation = flattened[:total]
    assert len(first_rotation) == len(set(first_rotation))
    assert set(first_rotation) == set(unique_sorted)


# --- construction and run() ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"sample_size": 0}, "sample_size must be positive"),
        ({"tap_max_events": 0}, "tap_max_events must be positive"),
        ({"lead_ns": -1}, "lead_ns must be non-negative"),
        ({"settle_ns": -1}, "settle_ns must be non-negative"),
    ],
)
def test_construction_refuses_what_cannot_work(overrides: dict[str, int], match: str) -> None:
    feed = Feed()
    arguments: dict[str, Any] = {  # Any: keyword arguments of the constructor under test
        "sample_size": 1,
        "lead_ns": 0,
        "settle_ns": 0,
        "tap_max_events": 1,
    } | overrides
    with pytest.raises(ValueError, match=match):
        Auditor(
            cast(KalshiRest, None),
            lambda: {},
            feed.clock,
            sink_for=lambda _ticker: None,
            open_tap=feed.open_tap,
            **arguments,
        )


async def test_run_stops_when_event_is_set() -> None:
    feed = Feed(standard_book("T-10"))
    router = OrderbooksRouter(feed)
    router.queue(orderbooks_response({"T-10": standard("1.00")}))
    stop = asyncio.Event()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        stop.set()

    async with auditing(feed, router) as auditor:
        await auditor.run(interval_s=5.0, stop=stop, sleep=fake_sleep)
    assert sleeps == [5.0]
    assert auditor.stats.rounds == 1


async def test_run_returns_without_sleeping_when_stop_fires_mid_round() -> None:
    feed = Feed(standard_book("T-12"))
    router = OrderbooksRouter(feed)
    router.queue(orderbooks_response({"T-12": standard("1.00")}))
    stop = asyncio.Event()

    def books() -> Mapping[str, Book]:
        stop.set()  # something else stopped the recorder while this round was running
        return feed.books

    async def fail_sleep(seconds: float) -> None:
        raise AssertionError("must not sleep once stop fired mid-round")

    async with auditing(feed, router, books=books) as auditor:
        await auditor.run(interval_s=1.0, stop=stop, sleep=fail_sleep)
    assert auditor.stats.rounds == 1


async def test_run_returns_immediately_when_already_stopped() -> None:
    feed = Feed()
    router = OrderbooksRouter(feed)
    stop = asyncio.Event()
    stop.set()

    async def fail_sleep(seconds: float) -> None:
        raise AssertionError("must not sleep when already stopped")

    async with auditing(feed, router) as auditor:
        await auditor.run(interval_s=1.0, stop=stop, sleep=fail_sleep)
    assert auditor.stats.rounds == 0
    assert router.requests == []


async def test_run_rejects_negative_interval() -> None:
    feed = Feed()
    async with auditing(feed, OrderbooksRouter(feed)) as auditor:
        with pytest.raises(ValueError, match="non-negative"):
            await auditor.run(interval_s=-1.0, stop=asyncio.Event())


def test_audit_result_is_a_frozen_struct() -> None:
    result = AuditResult(
        ticker="T-1",
        outcome="consistent",
        window_open_mono_ns=1,
        send_mono_ns=2,
        send_wall_ns=2,
        recv_mono_ns=3,
        recv_wall_ns=3,
        window_close_mono_ns=4,
        window_events=1,
        match_index=0,
        levels_rest=1,
        levels_local=1,
        mismatched_levels=1,
        max_abs_diff_e2=10,
    )
    assert result.exact is False
    with pytest.raises(AttributeError):
        result.outcome = "exact"  # type: ignore[misc]  # verifying immutability itself


async def test_run_ends_promptly_when_stop_fires_during_the_interval_wait() -> None:
    """A shutdown must not wait out the interval between rounds, 300 s in production.

    Before the fix the loop slept the whole interval regardless of ``stop``; a live
    shutdown stalled until that sleep ended.
    """
    feed = Feed(standard_book("T-14"))
    router = OrderbooksRouter(feed)
    router.queue(orderbooks_response({"T-14": standard("1.00")}))
    stop = asyncio.Event()

    async def never_ending_sleep(seconds: float) -> None:
        await asyncio.Event().wait()

    async with auditing(feed, router) as auditor:
        running = asyncio.create_task(
            auditor.run(interval_s=300.0, stop=stop, sleep=never_ending_sleep)
        )
        for _ in range(10_000):
            if auditor.stats.rounds >= 1:
                break
            await asyncio.sleep(0)
        assert auditor.stats.rounds == 1
        stop.set()
        await asyncio.wait_for(running, timeout=1.0)


async def test_stop_during_a_window_cancels_the_batch_closes_its_tap_and_writes_nothing() -> None:
    """A stop that lands inside a window's lead wait abandons that batch cleanly."""
    feed = Feed(standard_book("T-15"))
    router = OrderbooksRouter(feed)
    stop = asyncio.Event()
    sink = FakeSink()
    closed: list[bool] = []
    in_window = asyncio.Event()

    class ClosingTap:
        def __init__(self, inner: BookTap) -> None:
            self._inner = inner

        def close(self) -> Mapping[str, TapWindow]:
            closed.append(True)
            return self._inner.close()

    def open_tap(tickers: Collection[str], *, max_events: int) -> BookTap:
        return ClosingTap(feed.open_tap(tickers, max_events=max_events))

    async def blocking_window_sleep(seconds: float) -> None:
        in_window.set()
        await asyncio.Event().wait()

    async with auditing(
        feed,
        router,
        open_tap=open_tap,
        window_sleep=blocking_window_sleep,
        sink=sink,
    ) as auditor:
        running = asyncio.create_task(auditor.run(interval_s=300.0, stop=stop))
        await asyncio.wait_for(in_window.wait(), timeout=1.0)
        stop.set()
        await asyncio.wait_for(running, timeout=1.0)
    assert closed == [True]
    assert sink.records == []
    assert router.requests == []


async def test_an_undecidable_audit_records_why_in_the_tape() -> None:
    feed = Feed(standard_book("T-16"))
    router = OrderbooksRouter(feed)
    router.queue(orderbooks_response({"T-16": standard("1.00")}))
    sink = FakeSink()
    overflowed = TapWindow(
        ticker="T-16", start=BookImage.of(standard_book("T-16")), events=(), fault="overflow"
    )

    def open_tap(tickers: Collection[str], *, max_events: int) -> BookTap:
        return ScriptedTap({"T-16": overflowed})

    async with auditing(feed, router, open_tap=open_tap, sink=sink) as auditor:
        (result,) = await auditor.audit_once()
    assert result.outcome == "undecidable"
    assert result.fault == "overflow"
    (payload,) = sink.payloads()
    assert payload["outcome"] == "undecidable"
    assert payload["fault"] == "overflow"
