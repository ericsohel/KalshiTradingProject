"""What ``tape serve`` knows about the recorded markets, and the REST views built from it.

Responsibility: hold the recorder's latest catalog, what lifecycle events have said about its
markets since, the latest ticker update per market, and the recorder's latest status report, and
turn them, with resolved metadata and the server's books, into the ``MarketRow``,
``MarketDetail``, and ``ServiceStatus`` of docs/FRONTEND.md 4.1. The module is pure: it performs
no I/O and reads no clock, so callers pass times in.

Invariants: an entry exists exactly for each market of the latest catalog, which replaces the
previous one whole, together with everything lifecycle events said about the previous one's
markets; the market list holds only open markets, those neither determined nor settled nor past
their close time, as a ``close_date_updated`` event last moved it (ADR 0029); it ranks by 24-hour
volume, highest first, then by ticker; a value not yet known is ``None``; depth is given exactly
when a book is held, best levels first; and once a catalog has arrived, ticker updates and
lifecycle notes are kept only for its markets, so memory follows the catalog (before the first one
ticker updates follow the markets Kalshi lists, and lifecycle events are not kept).
"""

from __future__ import annotations

from itertools import islice
from typing import Final

import msgspec

from tape.api.contract import (
    DEPTH_LEVELS,
    BookState,
    BusHealth,
    ConnectionHealth,
    Depth,
    MarketDetail,
    MarketRow,
    PriceRange,
    RecorderHealth,
    ServiceStatus,
)
from tape.book import Book
from tape.events import (
    LIFECYCLE_CLOSE_DATE_UPDATED,
    LIFECYCLE_DETERMINED,
    LIFECYCLE_SETTLED,
    CatalogEntry,
    Lifecycle,
    MarketCatalog,
    Side,
    StatusReport,
    Ticker,
)
from tape.timeutil import NS_PER_MS, NS_PER_S

__all__ = [
    "RECORDING_INTERVALS",
    "UNRESOLVED",
    "MarketDirectory",
    "MarketMetadata",
    "book_state",
]

RECORDING_INTERVALS: Final = 2
"""A recorder counts as recording while its latest report arrived within this many intervals."""

_ENDED: Final = frozenset({LIFECYCLE_DETERMINED, LIFECYCLE_SETTLED})
"""Lifecycle events after which a market is closed whatever its close time says."""


class MarketMetadata(msgspec.Struct, frozen=True, kw_only=True):
    """What Kalshi's public event and series endpoints say about one market (ADR 0023).

    Each attribute is ``None`` until resolved.

    Attributes:
        title: The event's title.
        subtitle: The market's YES subtitle.
        category: The series category.
        price_ranges: The market's price grid.
    """

    title: str | None
    subtitle: str | None
    category: str | None
    price_ranges: tuple[PriceRange, ...] | None


UNRESOLVED: Final = MarketMetadata(title=None, subtitle=None, category=None, price_ranges=None)
"""Metadata of a market nothing has been resolved for."""


def book_state(book: Book | None) -> BookState:
    """How far a held book can be trusted.

    Args:
        book: The server's copy of a book, or ``None`` when it holds none.

    Returns:
        ``"unknown"`` without a book, otherwise ``"stale"`` or ``"fresh"``.
    """
    if book is None:
        return "unknown"
    return "stale" if book.is_stale() else "fresh"


class MarketDirectory:
    """The recorded markets and the recorder's health. See the module docstring.

    Mutable and single-owner; not thread-safe.
    """

    def __init__(self) -> None:
        self._entries: dict[str, CatalogEntry] = {}
        self._ranked: tuple[CatalogEntry, ...] = ()
        self._has_catalog = False
        self._tickers: dict[str, Ticker] = {}
        self._ended: set[str] = set()
        self._moved_close_ts: dict[str, int] = {}
        self._report: tuple[StatusReport, int] | None = None

    def __contains__(self, ticker: object) -> bool:
        return ticker in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    # ------------------------------------------------------------------------ updates

    def apply_catalog(self, catalog: MarketCatalog) -> None:
        """Replace the recorded markets with a new catalog, and forget what was said of the old.

        Other markets' ticker updates are dropped, and so is every lifecycle note: the recorder
        leaves a determined or settled market out of every catalog it publishes after the event,
        and gives a moved close time in it (ADR 0029).

        Args:
            catalog: The recorder's latest catalog.
        """
        self._entries = {entry.ticker: entry for entry in catalog.markets}
        self._ranked = tuple(
            sorted(catalog.markets, key=lambda entry: (-entry.volume_24h, entry.ticker))
        )
        self._has_catalog = True
        self._tickers = {
            ticker: update for ticker, update in self._tickers.items() if ticker in self._entries
        }
        self._ended = set()
        self._moved_close_ts = {}

    def apply_ticker(self, update: Ticker) -> None:
        """Keep a market's latest ticker update, if the market is recorded or no catalog came yet.

        Args:
            update: A ticker update from the bus; later updates replace earlier ones.
        """
        if self._has_catalog and update.ticker not in self._entries:
            return
        self._tickers[update.ticker] = update

    def apply_lifecycle(self, event: Lifecycle) -> None:
        """Note what a lifecycle event says about a recorded market's end (ADR 0029).

        A ``determined`` or ``settled`` event closes the market; a ``close_date_updated`` event
        carrying a close time replaces the catalog's. Any other event, or one about a market
        outside the latest catalog, is ignored.

        Args:
            event: A lifecycle event from the bus.
        """
        if event.ticker not in self._entries:
            return
        if event.event_type in _ENDED:
            self._ended.add(event.ticker)
        elif event.event_type == LIFECYCLE_CLOSE_DATE_UPDATED and event.close_ts is not None:
            self._moved_close_ts[event.ticker] = event.close_ts

    def apply_status(self, report: StatusReport, *, received_mono_ns: int) -> None:
        """Keep the recorder's latest status report and when it arrived.

        Args:
            report: The report.
            received_mono_ns: The server's monotonic time when the report arrived.
        """
        self._report = (report, received_mono_ns)

    # -------------------------------------------------------------------------- views

    def entry(self, ticker: str) -> CatalogEntry | None:
        """The catalog entry of a market, open or closed.

        Args:
            ticker: Any string.

        Returns:
            The entry, or ``None`` when the market is not in the latest catalog.
        """
        return self._entries.get(ticker)

    def close_ts(self, entry: CatalogEntry) -> int | None:
        """When a market closes, as the latest ``close_date_updated`` event or the catalog says.

        Args:
            entry: The market's catalog entry.

        Returns:
            Unix seconds, or ``None`` when unknown.
        """
        return self._moved_close_ts.get(entry.ticker, entry.close_ts)

    def is_open(self, entry: CatalogEntry, *, now_ts: int) -> bool:
        """Whether a market is still open: neither determined nor settled, nor past its close.

        Args:
            entry: The market's catalog entry.
            now_ts: Unix seconds now, on the caller's clock.

        Returns:
            ``False`` once the market was determined or settled, or once ``now_ts`` reaches its
            close time; a market whose close time is unknown stays open.
        """
        if entry.ticker in self._ended:
            return False
        close_ts = self.close_ts(entry)
        return close_ts is None or now_ts < close_ts

    def top(self, limit: int, *, now_ts: int) -> tuple[CatalogEntry, ...]:
        """The highest-ranked recorded markets that are still open.

        Args:
            limit: Most entries to return; positive.
            now_ts: Unix seconds now, on the caller's clock; see :meth:`is_open`.

        Returns:
            Open entries by 24-hour volume, highest first, then by ticker.

        Raises:
            ValueError: If ``limit`` is not positive.
        """
        if limit < 1:
            raise ValueError(f"limit must be positive, got {limit}")
        return tuple(
            islice((entry for entry in self._ranked if self.is_open(entry, now_ts=now_ts)), limit)
        )

    def row(self, entry: CatalogEntry, *, metadata: MarketMetadata, book: Book | None) -> MarketRow:
        """One market's row in the market list.

        Args:
            entry: The market's catalog entry.
            metadata: What is resolved about the market so far.
            book: The server's copy of the market's book, or ``None``.

        Returns:
            The row; prices come from the latest ticker update, or are ``None`` without one, and
            the close time is :meth:`close_ts`.
        """
        update = self._tickers.get(entry.ticker)
        return MarketRow(
            ticker=entry.ticker,
            event_ticker=entry.event_ticker,
            series_ticker=entry.series_ticker,
            title=metadata.title,
            subtitle=metadata.subtitle,
            category=metadata.category,
            showcase=entry.showcase,
            volume_24h_e2=entry.volume_24h,
            close_ts=self.close_ts(entry),
            bid_e4=None if update is None else update.bid,
            ask_e4=None if update is None else update.ask,
            last_e4=None if update is None else update.last,
            book=book_state(book),
        )

    def detail(
        self, entry: CatalogEntry, *, metadata: MarketMetadata, book: Book | None
    ) -> MarketDetail:
        """One market's row with its price grid and the top of its book.

        Args:
            entry: The market's catalog entry.
            metadata: What is resolved about the market so far.
            book: The server's copy of the market's book, fresh or stale, or ``None``.

        Returns:
            The detail; ``depth`` holds the best :data:`DEPTH_LEVELS` levels per side when a book
            is held, and is ``None`` otherwise.
        """
        row = self.row(entry, metadata=metadata, book=book)
        depth = (
            None
            if book is None
            else Depth(
                ts_ms=book.last_ts_ms,
                bids=tuple(
                    (level.price, level.count) for level in book.depth(Side.BID, DEPTH_LEVELS)
                ),
                asks=tuple(
                    (level.price, level.count) for level in book.depth(Side.ASK, DEPTH_LEVELS)
                ),
            )
        )
        return MarketDetail(
            **msgspec.structs.asdict(row), price_ranges=metadata.price_ranges, depth=depth
        )

    def service_status(self, *, now_mono_ns: int, bus: BusHealth, clients: int) -> ServiceStatus:
        """The service's health.

        Args:
            now_mono_ns: The server's monotonic time now, on the clock ``apply_status`` used.
            bus: How the server follows the bus.
            clients: Open live connections.

        Returns:
            The status; ``recording`` is true while the latest recorder report arrived within
            :data:`RECORDING_INTERVALS` of its own intervals.
        """
        if self._report is None:
            return ServiceStatus(
                recording=False,
                recorder_status_age_ms=None,
                recorder=None,
                bus=bus,
                clients=clients,
            )
        report, received_mono_ns = self._report
        age_ns = now_mono_ns - received_mono_ns
        return ServiceStatus(
            recording=age_ns <= RECORDING_INTERVALS * report.interval_s * NS_PER_S,
            recorder_status_age_ms=age_ns // NS_PER_MS,
            recorder=RecorderHealth(
                universe_size=report.universe_size,
                subscribed_markets=report.subscribed_markets,
                connections=tuple(
                    ConnectionHealth(
                        conn_id=connection.conn_id,
                        taped=connection.taped,
                        frames=connection.frames,
                        gaps=connection.gaps,
                        reconnects=connection.reconnects,
                        stale_books=connection.stale_books,
                        sink_dropped=connection.sink_dropped,
                    )
                    for connection in report.connections
                ),
            ),
            bus=bus,
            clients=clients,
        )
