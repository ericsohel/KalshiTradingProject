"""Sample local books, diff them against Kalshi's REST orderbooks, and tape the result.

Responsibility: implement docs/ARCHITECTURE.md 7.1 step 6 and the ``Auditor`` sketched
in docs/INTERFACES.md 8 — the process that produces one of the three published daily
integrity numbers, ``audits.exact_ratio`` (docs/DATA_FORMATS.md 7, docs/TESTING.md 7).
Every ``interval_s`` it rotates deterministically through the recorder's known local
books, skips ones currently marked stale, fetches REST snapshots for the rest in
batches of at most 100 tickers, converts each into YES space, and compares it against
the corresponding local ``Book`` with :func:`tape.book.diff`.

CRITICAL: the WebSocket order book is subscribed with ``use_yes_price=true``, so local
books are already in YES space, but the REST orderbook endpoints carry no such flag.
``yes_dollars`` are YES bids at YES prices; ``no_dollars`` are NO bids at NO-leg prices,
and a NO bid at price ``q`` is a YES ask at ``1 - q`` (docs/DATA_FORMATS.md 1.3, 2.2).
:func:`tape.wire.convert.rest_orderbook_levels` performs that complement; comparing
REST to local without it would make every audit a mismatch.

Invariants: one bad REST batch, one malformed REST snapshot, or a missing local book
never stops a round or raises past :meth:`Auditor.audit_once`; every counter in
:class:`AuditStats` only grows; :func:`round_robin_choice` is pure and deterministic,
so the same sequence of local books is sampled in the same order on every run.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Final

import msgspec

from tape.book import Book, diff
from tape.client.rest import KalshiRest
from tape.errors import BookInvariantError, KalshiHttpError, KalshiTransportError
from tape.events import Side
from tape.recorder.writer import RecordSink
from tape.segment import Record, RecordKind
from tape.timeutil import Clock
from tape.wire.convert import rest_orderbook_levels
from tape.wire.rest import OrderbookCountFp

__all__ = ["AuditResult", "AuditStats", "Auditor", "round_robin_choice"]

_MAX_BATCH_TICKERS: Final = 100
"""``GET /markets/orderbooks`` accepts at most this many tickers per call.

See docs/DATA_FORMATS.md 2.2.
"""


class AuditResult(msgspec.Struct, frozen=True, kw_only=True):
    """Comparison of one market's REST orderbook against its local book at one instant.

    Attributes:
        ticker: Market compared.
        recv_wall_ns: Local wall-clock time the REST response was received.
        levels_rest: Non-empty price levels in the REST snapshot, both sides.
        levels_local: Non-empty price levels in the local book, both sides.
        mismatched_levels: Number of price levels whose count differs between the two
            books (``tape.book.diff``); zero when ``exact``.
        max_abs_diff_e2: Largest absolute count difference across mismatched levels,
            in ``CountE2`` units; zero when ``exact``.
        exact: True when every level matches.
    """

    ticker: str
    recv_wall_ns: int
    levels_rest: int
    levels_local: int
    mismatched_levels: int
    max_abs_diff_e2: int
    exact: bool


class AuditStats(msgspec.Struct, frozen=True, kw_only=True):
    """Cumulative counters across every completed :meth:`Auditor.audit_once` call.

    No field here is a float: docs/ENGINEERING_STANDARDS.md 3.1 forbids float on any
    path feeding a published number, and ``audits.exact_ratio`` is one of the three
    daily integrity numbers (docs/DATA_FORMATS.md 7). :attr:`exact_ratio` keeps the
    ratio as an exact integer fraction instead of rounding it into a float.

    Attributes:
        rounds: Completed calls to :meth:`Auditor.audit_once`.
        books_sampled: Books actually compared against a REST snapshot.
        books_exact: Of those, the count with zero mismatched levels.
        books_mismatched: Of those, the count with at least one mismatched level.
        books_skipped_stale: Chosen tickers whose local book was stale and so were
            never fetched.
        books_missing_local: REST tickers with no matching local book at the moment
            the response was compared.
        books_invalid_rest: REST snapshots that violated a book invariant, malformed or
            crossed, and were skipped rather than compared.
        levels_mismatched: Sum of ``mismatched_levels`` across every sampled book.
    """

    rounds: int
    books_sampled: int
    books_exact: int
    books_mismatched: int
    books_skipped_stale: int
    books_missing_local: int
    books_invalid_rest: int
    levels_mismatched: int

    @property
    def exact_ratio(self) -> tuple[int, int]:
        """Exact ``(numerator, denominator)`` for ``books_exact / books_sampled``.

        Returns:
            ``(books_exact, books_sampled)``. The caller divides, rounds, or renders
            this however a report needs to; ``(0, 0)`` when nothing has been sampled
            yet means "no data", not "zero ratio", and must be handled as such rather
            than divided.
        """
        return (self.books_exact, self.books_sampled)


def round_robin_choice(
    tickers: Sequence[str], count: int, cursor: int
) -> tuple[tuple[str, ...], int]:
    """Choose the next ``count`` tickers from a sorted, deterministic rotation.

    Pure and reads no clock or randomness, so repeated calls with an advancing
    ``cursor`` visit every distinct ticker exactly once per full rotation, in the same
    order on every run (docs/ARCHITECTURE.md 7.1 step 6).

    Args:
        tickers: Candidate tickers for this rotation. Duplicates are ignored; order
            does not matter, since the tickers are sorted before choosing.
        count: Maximum number of tickers to return.
        cursor: Position to resume from, as returned by a previous call. Any integer
            is accepted; it is wrapped modulo the number of distinct tickers.

    Returns:
        The chosen tickers, ascending from ``cursor``'s wrapped position, and the
        cursor to pass to the next call. Fewer than ``count`` tickers, with no
        repeats, come back when there are fewer than ``count`` distinct tickers;
        ``((), 0)`` when ``tickers`` is empty.

    Raises:
        ValueError: If ``count`` is negative.
    """
    if count < 0:
        raise ValueError(f"count must be non-negative, got {count}")
    unique = tuple(sorted(set(tickers)))
    total = len(unique)
    if total == 0:
        return (), 0
    take = min(count, total)
    start = cursor % total
    chosen = tuple(unique[(start + i) % total] for i in range(take))
    new_cursor = (start + take) % total
    return chosen, new_cursor


def _levels_json(book: Book) -> list[dict[str, int]]:
    """Both sides of ``book`` as JSON-ready level dicts, best first, for a tape record."""
    return [
        {"side": int(side), "price_e4": int(level.price), "count_e2": int(level.count)}
        for side in (Side.BID, Side.ASK)
        for level in book.levels(side)
    ]


class Auditor:
    """Samples local books and diffs them against Kalshi's REST orderbooks. See module docstring.

    Args:
        rest: Client used to fetch REST orderbooks; only ``orderbooks`` is called.
        books: Returns the current local books across every connection, keyed by
            ticker. Called at the start of a round to choose candidates, and again
            after each batch response so a comparison uses the freshest known book
            (a ticker whose local book has since disappeared counts as
            ``books_missing_local``). Each ``Book`` is the live, mutable object the
            recorder updates in place, so a comparison also reflects concurrent
            changes made while a REST call was in flight.
        clock: Source of the wall- and monotonic-clock times stamped on results and
            tape records.
        sink_for: Returns the sink of the connection that owns a ticker, or
            ``None`` if the ticker is unknown or its connection is unrecorded; a
            result is still counted when this returns ``None``, but nothing is
            written to tape for it. A record carries that sink's connection id.
        sample_size: Maximum tickers chosen per round.
        logger: Destination for logs; defaults to this module's logger.

    Raises:
        ValueError: If ``sample_size`` is not positive.
    """

    def __init__(
        self,
        rest: KalshiRest,
        books: Callable[[], Mapping[str, Book]],
        clock: Clock,
        *,
        sink_for: Callable[[str], RecordSink | None],
        sample_size: int,
        logger: logging.Logger | None = None,
    ) -> None:
        if sample_size <= 0:
            raise ValueError(f"sample_size must be positive, got {sample_size}")
        self._rest = rest
        self._books = books
        self._clock = clock
        self._sink_for = sink_for
        self._sample_size = sample_size
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._cursor = 0
        self._rounds = 0
        self._books_sampled = 0
        self._books_exact = 0
        self._books_mismatched = 0
        self._books_skipped_stale = 0
        self._books_missing_local = 0
        self._books_invalid_rest = 0
        self._levels_mismatched = 0

    @property
    def stats(self) -> AuditStats:
        """Current cumulative counters; see :class:`AuditStats`."""
        return AuditStats(
            rounds=self._rounds,
            books_sampled=self._books_sampled,
            books_exact=self._books_exact,
            books_mismatched=self._books_mismatched,
            books_skipped_stale=self._books_skipped_stale,
            books_missing_local=self._books_missing_local,
            books_invalid_rest=self._books_invalid_rest,
            levels_mismatched=self._levels_mismatched,
        )

    async def audit_once(self) -> tuple[AuditResult, ...]:
        """Sample, fetch, and compare one round of books.

        Chooses up to ``sample_size`` tickers with :func:`round_robin_choice` over the
        current local books, skipping (and counting) any that are stale, fetches the
        rest in batches of at most 100 tickers, and compares each batch's response as
        soon as it arrives. A batch that fails outright (transport or HTTP error) is
        logged and skipped; the remaining batches still run.

        Returns:
            One :class:`AuditResult` per book actually compared, in the order their
            batch responses arrived.
        """
        current_books = self._books()
        chosen, self._cursor = round_robin_choice(
            tuple(current_books.keys()), self._sample_size, self._cursor
        )
        to_fetch: list[str] = []
        for ticker in chosen:
            book = current_books.get(ticker)
            if book is None or book.is_stale():
                self._books_skipped_stale += 1
                continue
            to_fetch.append(ticker)
        results: list[AuditResult] = []
        for start in range(0, len(to_fetch), _MAX_BATCH_TICKERS):
            batch = to_fetch[start : start + _MAX_BATCH_TICKERS]
            results.extend(await self._audit_batch(batch))
        self._rounds += 1
        return tuple(results)

    async def run(
        self,
        *,
        interval_s: float,
        stop: asyncio.Event,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Run :meth:`audit_once` on a loop until ``stop`` is set.

        Args:
            interval_s: Seconds to wait between rounds.
            stop: Set (from elsewhere) to end the loop; checked before every round and
                again before every wait, so a round in progress always finishes but a
                pending wait is skipped once ``stop`` fires.
            sleep: Waits the given number of seconds; injected so tests control time
                without a real delay. Defaults to ``asyncio.sleep``.

        Raises:
            ValueError: If ``interval_s`` is negative.
        """
        if interval_s < 0:
            raise ValueError(f"interval_s must be non-negative, got {interval_s}")
        while not stop.is_set():
            await self.audit_once()
            if stop.is_set():
                return
            await sleep(interval_s)

    async def _audit_batch(self, batch: Sequence[str]) -> tuple[AuditResult, ...]:
        """Fetch one batch's REST orderbooks and compare each against its local book.

        Raises nothing: a transport or HTTP failure for the whole batch is logged and
        counted as skipped by returning no results for it.
        """
        try:
            orderbooks = await self._rest.orderbooks(batch)
        except (KalshiHttpError, KalshiTransportError) as exc:
            self._log.warning(
                "audit batch failed", extra={"batch_size": len(batch), "error": repr(exc)}
            )
            return ()
        latest_books = self._books()
        recv_mono_ns = int(self._clock.mono_ns())
        recv_wall_ns = int(self._clock.wall_ns())
        results: list[AuditResult] = []
        for entry in orderbooks:
            local_book = latest_books.get(entry.ticker)
            if local_book is None:
                self._books_missing_local += 1
                continue
            result = self._process_entry(
                entry.ticker,
                entry.orderbook_fp,
                local_book,
                recv_mono_ns=recv_mono_ns,
                recv_wall_ns=recv_wall_ns,
            )
            if result is not None:
                results.append(result)
        return tuple(results)

    def _process_entry(
        self,
        ticker: str,
        rest_book: OrderbookCountFp,
        local_book: Book,
        *,
        recv_mono_ns: int,
        recv_wall_ns: int,
    ) -> AuditResult | None:
        """Compare one ticker's REST snapshot to its local book, and tape the result.

        Returns:
            The comparison result, or ``None`` if the REST snapshot itself violated a
            book invariant (a malformed or crossed response): that ticker is logged
            and skipped rather than raised, so one bad book never stops a round.
        """
        bids, asks = rest_orderbook_levels(rest_book)
        rest_as_book = Book(ticker)
        try:
            rest_as_book.apply_snapshot(bids, asks, ts_ms=None)
        except BookInvariantError as exc:
            self._log.warning(
                "audit rest snapshot invalid", extra={"ticker": ticker, "error": repr(exc)}
            )
            self._books_invalid_rest += 1
            return None
        book_diff = diff(rest_as_book, local_book)
        max_abs_diff = max(
            (abs(level.count_a - level.count_b) for level in book_diff.differences), default=0
        )
        result = AuditResult(
            ticker=ticker,
            recv_wall_ns=recv_wall_ns,
            levels_rest=len(bids) + len(asks),
            levels_local=local_book.level_count(Side.BID) + local_book.level_count(Side.ASK),
            mismatched_levels=len(book_diff.differences),
            max_abs_diff_e2=max_abs_diff,
            exact=book_diff.is_empty,
        )
        self._books_sampled += 1
        if result.exact:
            self._books_exact += 1
        else:
            self._books_mismatched += 1
        self._levels_mismatched += result.mismatched_levels
        self._write_record(result, rest_as_book, local_book, recv_mono_ns=recv_mono_ns)
        return result

    def _write_record(
        self, result: AuditResult, rest_book: Book, local_book: Book, *, recv_mono_ns: int
    ) -> None:
        """Write ``result`` as an ``AUDIT`` record to its ticker's sink, if any."""
        sink = self._sink_for(result.ticker)
        if sink is None:
            return
        payload: dict[str, object] = {
            "ticker": result.ticker,
            "levels_rest": result.levels_rest,
            "levels_local": result.levels_local,
            "mismatched_levels": result.mismatched_levels,
            "max_abs_diff_e2": result.max_abs_diff_e2,
        }
        if not result.exact:
            payload["rest_levels"] = _levels_json(rest_book)
            payload["local_levels"] = _levels_json(local_book)
        record = Record(
            kind=RecordKind.AUDIT,
            conn_id=sink.conn_id,
            recv_mono_ns=recv_mono_ns,
            recv_wall_ns=result.recv_wall_ns,
            payload=msgspec.json.encode(payload),
        )
        sink.put(record)
