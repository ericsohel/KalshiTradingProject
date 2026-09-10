"""Auditor: REST-vs-local book comparison, batching, staleness, and round-robin sampling."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from typing import cast

import httpx
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tape.book import Book
from tape.client import NullRateLimiter
from tape.client.rest import KalshiRest
from tape.events import Level
from tape.fixedpoint import CountE2, PriceE4
from tape.recorder.auditor import Auditor, AuditResult, AuditStats, round_robin_choice
from tape.recorder.writer import RecordSink
from tape.segment import Record, RecordKind
from tape.timeutil import Clock, FrozenClock

BASE_URL = "https://api.example.com/trade-api/v2"


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


def books_of(*books: Book) -> Callable[[], Mapping[str, Book]]:
    """Return a ``books`` callable backed by a fixed set of ``Book``s, keyed by ticker."""
    mapping = {book.ticker: book for book in books}
    return lambda: mapping


class MutatingBooks:
    """A ``books`` callable whose second call sees fewer tickers than its first.

    Simulates a local book disappearing (market delisted, connection reset) between
    when the auditor chooses a ticker and when it compares the REST response that
    named it, so ``books_missing_local`` can be exercised without a real race.
    """

    def __init__(self, first: Mapping[str, Book]) -> None:
        self._first = dict(first)
        self.calls = 0

    def __call__(self) -> Mapping[str, Book]:
        self.calls += 1
        return dict(self._first) if self.calls == 1 else {}


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


class OrderbooksRouter:
    """Answers ``GET /markets/orderbooks`` with one queued response per call, in order."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self._responses: list[httpx.Response] = []

    def queue(self, response: httpx.Response) -> None:
        """Queue the response to return for the next call."""
        self._responses.append(response)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
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
async def auditor_context(
    router: OrderbooksRouter,
    books: Callable[[], Mapping[str, Book]],
    *,
    sink_for: Callable[[str], RecordSink | None] | None = None,
    sample_size: int = 10,
    clock: Clock | None = None,
) -> AsyncIterator[Auditor]:
    """Build an ``Auditor`` wired to ``router`` over ``httpx.MockTransport``, no network."""
    used_clock = clock if clock is not None else FrozenClock(wall_ns=1_700_000_000_000_000_000)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(router), base_url=BASE_URL
    ) as client:
        rest = KalshiRest(BASE_URL, client, NullRateLimiter(), used_clock)
        yield Auditor(
            rest,
            books,
            used_clock,
            sink_for=(lambda _ticker: None) if sink_for is None else sink_for,
            sample_size=sample_size,
        )


# --- comparison behavior ---------------------------------------------------------------


async def test_exact_match_counts_as_exact() -> None:
    # Counts are CountE2 (hundredths of a contract): 100 == "1.00", 70 == "0.70".
    book = local_book("T-1", [lvl(3000, 100)], [lvl(6000, 70)])
    router = OrderbooksRouter()
    router.queue(orderbooks_response({"T-1": ([["0.3000", "1.00"]], [["0.4000", "0.70"]])}))
    async with auditor_context(router, books_of(book)) as auditor:
        results = await auditor.audit_once()
    assert len(results) == 1
    result = results[0]
    assert result.ticker == "T-1"
    assert result.exact is True
    assert result.mismatched_levels == 0
    assert result.max_abs_diff_e2 == 0
    assert result.levels_rest == 2
    assert result.levels_local == 2
    stats = auditor.stats
    assert stats.rounds == 1
    assert stats.books_sampled == 1
    assert stats.books_exact == 1
    assert stats.books_mismatched == 0
    assert stats.exact_ratio == (1, 1)


async def test_no_side_is_complemented_onto_yes_scale() -> None:
    # A REST no_dollars level at 0.4000 is a YES ask at 1 - 0.4000 = 0.6000
    # (docs/DATA_FORMATS.md 1.3): getting this backwards makes every audit mismatch.
    book = local_book("T-3", [], [lvl(6000, 50)])
    router = OrderbooksRouter()
    router.queue(orderbooks_response({"T-3": ([], [["0.4000", "0.50"]])}))
    async with auditor_context(router, books_of(book)) as auditor:
        results = await auditor.audit_once()
    assert results[0].exact is True
    assert results[0].levels_rest == 1
    assert results[0].levels_local == 1


async def test_mismatch_reports_level_and_writes_both_level_lists() -> None:
    book = local_book("T-2", [lvl(3000, 100)], [lvl(6000, 70)])
    router = OrderbooksRouter()
    # REST reports 90 CountE2 ("0.90") resting where the local book has 100 ("1.00").
    router.queue(orderbooks_response({"T-2": ([["0.3000", "0.90"]], [["0.4000", "0.70"]])}))
    fake_sink = FakeSink()
    sink_for = lambda ticker: fake_sink if ticker == "T-2" else None  # noqa: E731
    async with auditor_context(router, books_of(book), sink_for=sink_for) as auditor:
        results = await auditor.audit_once()
    result = results[0]
    assert result.exact is False
    assert result.mismatched_levels == 1
    assert result.max_abs_diff_e2 == 10
    assert auditor.stats.books_mismatched == 1
    assert auditor.stats.books_exact == 0
    assert auditor.stats.levels_mismatched == 1
    assert len(fake_sink.records) == 1
    record = fake_sink.records[0]
    assert record.kind == RecordKind.AUDIT
    # Stamped with the owning connection, never a placeholder shared by all connections.
    assert record.conn_id == fake_sink.conn_id
    payload = json.loads(record.payload)
    assert payload["ticker"] == "T-2"
    assert payload["mismatched_levels"] == 1
    assert payload["max_abs_diff_e2"] == 10
    assert payload["rest_levels"] == [
        {"side": 0, "price_e4": 3000, "count_e2": 90},
        {"side": 1, "price_e4": 6000, "count_e2": 70},
    ]
    assert payload["local_levels"] == [
        {"side": 0, "price_e4": 3000, "count_e2": 100},
        {"side": 1, "price_e4": 6000, "count_e2": 70},
    ]


async def test_missing_sink_still_counts_the_result() -> None:
    book = local_book("T-9", [lvl(3000, 100)], [lvl(6000, 70)])
    router = OrderbooksRouter()
    router.queue(orderbooks_response({"T-9": ([["0.3000", "1.00"]], [["0.4000", "0.70"]])}))
    async with auditor_context(router, books_of(book)) as auditor:  # sink_for returns None
        results = await auditor.audit_once()
    assert len(results) == 1
    assert results[0].exact is True
    assert auditor.stats.books_sampled == 1


async def test_exact_result_records_no_level_lists() -> None:
    # Only a mismatch needs the full level lists to be diagnosable from the tape.
    book = local_book("T-13", [lvl(3000, 100)], [lvl(6000, 70)])
    router = OrderbooksRouter()
    router.queue(orderbooks_response({"T-13": ([["0.3000", "1.00"]], [["0.4000", "0.70"]])}))
    fake_sink = FakeSink()
    sink_for = lambda ticker: fake_sink if ticker == "T-13" else None  # noqa: E731
    async with auditor_context(router, books_of(book), sink_for=sink_for) as auditor:
        results = await auditor.audit_once()
    assert results[0].exact is True
    payload = json.loads(fake_sink.records[0].payload)
    assert "rest_levels" not in payload
    assert "local_levels" not in payload


async def test_bad_rest_snapshot_is_skipped_not_raised() -> None:
    # A crossed REST snapshot violates the book invariant; it must be logged and
    # skipped, never raised, so one bad book cannot stop a round.
    book = local_book("T-11", [lvl(3000, 100)], [lvl(6000, 70)])
    router = OrderbooksRouter()
    router.queue(orderbooks_response({"T-11": ([["0.9000", "1.00"]], [["0.8000", "1.00"]])}))
    async with auditor_context(router, books_of(book)) as auditor:
        results = await auditor.audit_once()
    assert results == ()
    assert auditor.stats.books_sampled == 0
    assert auditor.stats.books_invalid_rest == 1


# --- staleness and missing-local -------------------------------------------------------


async def test_stale_local_book_is_skipped_and_counted() -> None:
    stale_book = local_book("T-4", [lvl(3000, 100)], [lvl(6000, 70)], stale=True)
    router = OrderbooksRouter()
    async with auditor_context(router, books_of(stale_book)) as auditor:
        results = await auditor.audit_once()
    assert results == ()
    assert auditor.stats.books_skipped_stale == 1
    assert auditor.stats.books_sampled == 0
    assert router.requests == []


async def test_missing_local_book_at_comparison_time_is_counted() -> None:
    book = local_book("T-5", [lvl(3000, 100)], [lvl(6000, 70)])
    books = MutatingBooks({"T-5": book})
    router = OrderbooksRouter()
    router.queue(orderbooks_response({"T-5": ([["0.3000", "100.00"]], [["0.4000", "70.00"]])}))
    async with auditor_context(router, books) as auditor:
        results = await auditor.audit_once()
    assert results == ()
    assert auditor.stats.books_missing_local == 1
    assert auditor.stats.books_sampled == 0
    assert books.calls == 2


# --- batching ----------------------------------------------------------------------------


async def test_batches_of_at_most_100_tickers() -> None:
    tickers = [f"T-{i:03d}" for i in range(250)]
    books = books_of(*(local_book(t, [lvl(3000, 1)], [lvl(6000, 1)]) for t in tickers))
    router = OrderbooksRouter()
    for _ in range(3):
        router.queue(httpx.Response(200, json={"orderbooks": []}))
    async with auditor_context(router, books, sample_size=250) as auditor:
        await auditor.audit_once()
    batch_sizes = [len(req.url.params.get_list("tickers")) for req in router.requests]
    assert batch_sizes == [100, 100, 50]


async def test_one_failed_batch_does_not_stop_the_others() -> None:
    tickers = [f"T-{i:03d}" for i in range(150)]
    books = books_of(*(local_book(t, [lvl(3000, 1)], [lvl(6000, 1)]) for t in tickers))
    router = OrderbooksRouter()
    router.queue(httpx.Response(500))
    second_batch = sorted(tickers)[100:150]
    router.queue(
        orderbooks_response({t: ([["0.3000", "1.00"]], [["0.4000", "1.00"]]) for t in second_batch})
    )
    async with auditor_context(router, books, sample_size=150) as auditor:
        results = await auditor.audit_once()
    assert len(results) == 50
    assert len(router.requests) == 2
    assert auditor.stats.books_sampled == 50


async def test_successive_rounds_rotate_through_every_ticker() -> None:
    tickers = ["T-A", "T-B", "T-C"]
    books = books_of(*(local_book(t, [lvl(3000, 1)], [lvl(6000, 1)]) for t in tickers))
    router = OrderbooksRouter()
    for ticker in sorted(tickers):
        router.queue(orderbooks_response({ticker: ([["0.3000", "1.00"]], [["0.4000", "1.00"]])}))
    async with auditor_context(router, books, sample_size=1) as auditor:
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


# --- AuditStats ----------------------------------------------------------------------------


def test_exact_ratio_is_no_data_when_nothing_sampled() -> None:
    stats = AuditStats(
        rounds=0,
        books_sampled=0,
        books_exact=0,
        books_mismatched=0,
        books_skipped_stale=0,
        books_missing_local=0,
        books_invalid_rest=0,
        levels_mismatched=0,
    )
    assert stats.exact_ratio == (0, 0)


def test_exact_ratio_is_an_exact_integer_fraction() -> None:
    stats = AuditStats(
        rounds=2,
        books_sampled=3,
        books_exact=2,
        books_mismatched=1,
        books_skipped_stale=0,
        books_missing_local=0,
        books_invalid_rest=0,
        levels_mismatched=1,
    )
    assert stats.exact_ratio == (2, 3)


# --- construction and run() ---------------------------------------------------------------


def test_sample_size_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        Auditor(
            cast(KalshiRest, None),
            lambda: {},
            FrozenClock(),
            sink_for=lambda _ticker: None,
            sample_size=0,
        )


async def test_run_stops_when_event_is_set() -> None:
    book = local_book("T-10", [lvl(3000, 100)], [lvl(6000, 70)])
    router = OrderbooksRouter()
    router.queue(orderbooks_response({"T-10": ([["0.3000", "1.00"]], [["0.4000", "0.70"]])}))
    stop = asyncio.Event()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        stop.set()

    async with auditor_context(router, books_of(book)) as auditor:
        await auditor.run(interval_s=5.0, stop=stop, sleep=fake_sleep)
    assert sleeps == [5.0]
    assert auditor.stats.rounds == 1


async def test_run_returns_without_sleeping_when_stop_fires_mid_round() -> None:
    book = local_book("T-12", [lvl(3000, 100)], [lvl(6000, 70)])
    router = OrderbooksRouter()
    router.queue(orderbooks_response({"T-12": ([["0.3000", "1.00"]], [["0.4000", "0.70"]])}))
    stop = asyncio.Event()

    def books() -> Mapping[str, Book]:
        stop.set()  # something else stopped the recorder while this round was running
        return {"T-12": book}

    async def fail_sleep(seconds: float) -> None:
        raise AssertionError("must not sleep once stop fired mid-round")

    async with auditor_context(router, books) as auditor:
        await auditor.run(interval_s=1.0, stop=stop, sleep=fail_sleep)
    assert auditor.stats.rounds == 1


async def test_run_returns_immediately_when_already_stopped() -> None:
    router = OrderbooksRouter()
    stop = asyncio.Event()
    stop.set()

    async def fail_sleep(seconds: float) -> None:
        raise AssertionError("must not sleep when already stopped")

    async with auditor_context(router, books_of()) as auditor:
        await auditor.run(interval_s=1.0, stop=stop, sleep=fail_sleep)
    assert auditor.stats.rounds == 0
    assert router.requests == []


async def test_run_rejects_negative_interval() -> None:
    router = OrderbooksRouter()
    stop = asyncio.Event()
    async with auditor_context(router, books_of()) as auditor:
        with pytest.raises(ValueError, match="non-negative"):
            await auditor.run(interval_s=-1.0, stop=stop)


def test_audit_result_is_a_frozen_struct() -> None:
    result = AuditResult(
        ticker="T-1",
        recv_wall_ns=1,
        levels_rest=1,
        levels_local=1,
        mismatched_levels=0,
        max_abs_diff_e2=0,
        exact=True,
    )
    with pytest.raises(AttributeError):
        result.exact = False  # type: ignore[misc]  # verifying immutability itself
