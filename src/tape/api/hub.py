"""Follow the recorder's bus and fan live messages out to WebSocket sessions (docs/FRONTEND.md 4.2).

Responsibility: subscribe to every bus topic through a :class:`tape.bus.ports.Subscriber`, decode
each message, apply it to :class:`tape.bus.LiveBooks` and to the :class:`MarketDirectory`, and offer
each session the protocol messages the message means for the markets it follows. It is also the
:class:`tape.api.session.SessionFeed` that answers subscriptions and builds snapshots.

Fan-out, per bus message, after the books observed it:

- a book that became unknown (bus loss, a recorder restart, or a copy that broke an invariant):
  ``resync`` with ``bus_loss``;
- a book that became known, or turned fresh: ``snapshot``; one that turned stale without a
  snapshot: ``book`` with ``stale``;
- an exchange snapshot applied to a book that stayed fresh: ``snapshot``; an applied delta:
  ``delta``;
- trades and ticker updates: ``trade`` and ``ticker``, whatever the book's state;
- catalogs, status reports, and ticker updates also update the directory.

Invariants: messages about one market reach each session in bus order, because every bus message
is observed and offered without an await in between; a session is offered market messages only
for markets it follows; at most ``max_clients`` sessions are attached; and a bus message that does
not decode is counted and skipped, its number then showing as a gap.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from typing import Final

import msgspec

from tape.api.contract import (
    PROTOCOL_VERSION,
    REJECT_TOO_MANY_TICKERS,
    REJECT_UNKNOWN_TICKER,
    RESYNC_BUS_LOSS,
    BookMessage,
    BookSide,
    BusHealth,
    DeltaMessage,
    HelloMessage,
    PriceLevel,
    Rejection,
    ResyncMessage,
    ServerMessage,
    SnapshotMessage,
    SubscribedMessage,
    TickerMessage,
    TradeMessage,
)
from tape.api.directory import MarketDirectory
from tape.api.session import ClientSession, LiveSocket
from tape.book import Book
from tape.bus import (
    BOOK_FRESH,
    BOOK_UNKNOWN,
    LiveBooks,
    Observation,
    Subscriber,
    decode_bus_envelope,
)
from tape.errors import WireError
from tape.events import (
    BookDelta,
    BookSnapshot,
    BusEvent,
    CatalogEntry,
    MarketCatalog,
    Side,
    StatusReport,
    Ticker,
    Trade,
)
from tape.timeutil import Clock

__all__ = ["EVERY_TOPIC", "LiveHub", "ServeConfig"]

EVERY_TOPIC: Final = b""
"""The subscription prefix of every topic; only a subscriber to all of them can tell loss."""


class ServeConfig(msgspec.Struct, frozen=True, kw_only=True):
    """What the live API allows; built from settings by ``tape.cli.serve_config``.

    Attributes:
        allowed_origins: Exact origins that CORS and the WebSocket ``Origin`` check accept.
        max_clients: Most live connections attached at once.
        max_tickers: Most markets one connection follows.
        client_queue_max: Most messages queued for one connection.
        bus_refresh_s: The recorder's refresh interval, announced to clients as the longest wait
            for a snapshot after ``bus_loss``.

    Raises:
        ValueError: If no origin is allowed or a bound is not positive.
    """

    allowed_origins: frozenset[str]
    max_clients: int
    max_tickers: int
    client_queue_max: int
    bus_refresh_s: int

    def __post_init__(self) -> None:
        if not self.allowed_origins:
            raise ValueError("allowed_origins must name at least one origin")
        for name in ("max_clients", "max_tickers", "client_queue_max", "bus_refresh_s"):
            value = getattr(self, name)
            if value < 1:
                raise ValueError(f"{name} must be positive, got {value}")


class LiveHub:
    """The bus follower and session fan-out. See the module docstring.

    Not thread-safe; used on one event loop.

    Args:
        subscriber: The bus; :meth:`run` subscribes it to every topic and :meth:`close` closes it.
        directory: Updated from catalogs, status reports, and ticker updates.
        config: What clients are allowed.
        clock: Time for status arrivals and for every session's limits.
        request_metadata: Called with the markets a subscription names, so their metadata is
            resolved; must not wait or raise.
        logger: Destination for logs; defaults to this module's logger.
    """

    def __init__(
        self,
        subscriber: Subscriber,
        *,
        directory: MarketDirectory,
        config: ServeConfig,
        clock: Clock,
        request_metadata: Callable[[Iterable[CatalogEntry]], None],
        logger: logging.Logger | None = None,
    ) -> None:
        self._subscriber = subscriber
        self._directory = directory
        self._config = config
        self._clock = clock
        self._request_metadata = request_metadata
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._live = LiveBooks()
        # Dictionaries used as ordered sets, so fan-out order does not depend on hashing.
        self._sessions: dict[ClientSession, None] = {}
        self._followers: dict[str, dict[ClientSession, None]] = {}
        self._malformed = 0

    # ------------------------------------------------------------------- read-only views

    @property
    def config(self) -> ServeConfig:
        """What clients are allowed."""
        return self._config

    @property
    def directory(self) -> MarketDirectory:
        """The recorded markets and the recorder's health."""
        return self._directory

    @property
    def clients(self) -> int:
        """Live connections attached now."""
        return len(self._sessions)

    @property
    def malformed(self) -> int:
        """Bus messages that did not decode."""
        return self._malformed

    def book(self, ticker: str) -> Book | None:
        """The server's copy of a market's book, fresh or stale; read it, never change it.

        Args:
            ticker: Any string.

        Returns:
            The book, or ``None`` when none is held.
        """
        return self._live.books().get(ticker)

    def bus_health(self) -> BusHealth:
        """How the hub follows the bus, for ``GET /status``."""
        stats = self._live.stats
        return BusHealth(
            epoch=self._live.epoch,
            last_seq=self._live.last_seq,
            messages=stats.messages,
            resets=stats.resets,
            missed=stats.missed,
            books_known=len(self._live.books()),
        )

    # ------------------------------------------------------------------------ sessions

    def admit(self, socket: LiveSocket) -> ClientSession | None:
        """Attach a session for a new connection and queue its ``hello``.

        Args:
            socket: The connection, about to be accepted.

        Returns:
            The session, or ``None`` when ``max_clients`` sessions are attached.
        """
        if len(self._sessions) >= self._config.max_clients:
            return None
        session = ClientSession(
            socket, self, clock=self._clock, queue_max=self._config.client_queue_max
        )
        self._sessions[session] = None
        session.offer(
            [
                HelloMessage(
                    protocol=PROTOCOL_VERSION,
                    max_tickers=self._config.max_tickers,
                    bus_refresh_s=self._config.bus_refresh_s,
                )
            ]
        )
        return session

    def detach(self, session: ClientSession) -> None:
        """Forget a session whose connection ended. Idempotent.

        Args:
            session: The session.
        """
        self._sessions.pop(session, None)
        for ticker in session.subscriptions:
            self._unfollow(ticker, session)

    def close_sessions(self, code: int) -> None:
        """Close every attached session with a code, for example at shutdown.

        Args:
            code: The WebSocket close code.
        """
        for session in list(self._sessions):
            session.close(code)

    def subscribe(self, session: ClientSession, tickers: Sequence[str]) -> None:
        """Replace a session's subscription set and offer the reply and the new snapshots.

        Snapshots go to known books the session did not follow before. A repeated ticker counts
        once. A market that is not in the catalog is rejected with
        ``unknown_ticker``, and every market beyond ``max_tickers`` with ``too_many_tickers``.

        Args:
            session: The session.
            tickers: The markets it asked for, in its order.
        """
        accepted: list[str] = []
        rejected: list[Rejection] = []
        for ticker in dict.fromkeys(tickers):
            if ticker not in self._directory:
                rejected.append(Rejection(ticker=ticker, code=REJECT_UNKNOWN_TICKER))
            elif len(accepted) >= self._config.max_tickers:
                rejected.append(Rejection(ticker=ticker, code=REJECT_TOO_MANY_TICKERS))
            else:
                accepted.append(ticker)
        previous = frozenset(session.subscriptions)
        for ticker in previous.difference(accepted):
            self._unfollow(ticker, session)
        for ticker in accepted:
            self._followers.setdefault(ticker, {})[session] = None
        session.replace_subscriptions(accepted)
        messages: list[ServerMessage] = [
            SubscribedMessage(tickers=tuple(accepted), rejected=tuple(rejected))
        ]
        for ticker in accepted:
            snapshot = None if ticker in previous else self.snapshot(ticker)
            if snapshot is not None:
                messages.append(snapshot)
        session.offer(messages)
        self._request_metadata(
            entry for ticker in accepted if (entry := self._directory.entry(ticker)) is not None
        )

    def snapshot(self, ticker: str) -> SnapshotMessage | None:
        """The whole book of a market as the hub holds it now.

        Args:
            ticker: Any string.

        Returns:
            The snapshot, or ``None`` when no book is held.
        """
        book = self._live.books().get(ticker)
        if book is None:
            return None
        return SnapshotMessage(
            ticker=ticker,
            book="stale" if book.is_stale() else "fresh",
            ts_ms=book.last_ts_ms,
            bids=_levels(book, Side.BID),
            asks=_levels(book, Side.ASK),
        )

    # ------------------------------------------------------------------------------ bus

    async def run(self) -> None:
        """Follow the bus until :meth:`close`.

        Raises:
            BusError: If subscribing or receiving fails.
        """
        self._subscriber.subscribe(EVERY_TOPIC)
        async for _, payload in self._subscriber.messages():
            self.receive(payload)

    def close(self) -> None:
        """Stop following the bus, ending :meth:`run`. Idempotent."""
        self._subscriber.close()

    def receive(self, payload: bytes) -> None:
        """Account for one bus message, in arrival order, and offer what it means to sessions.

        Args:
            payload: The message's payload frame.
        """
        try:
            envelope = decode_bus_envelope(payload)
        except WireError as exc:
            self._malformed += 1
            if self._malformed == 1:
                self._log.warning(
                    "bus message not decoded; later ones are only counted",
                    extra={"error": repr(exc)},
                )
            return
        observation = self._live.observe(envelope)
        event = envelope.event
        self._update_directory(event)
        self._fan_out(observation, event)

    def _update_directory(self, event: BusEvent) -> None:
        if isinstance(event, Ticker):
            self._directory.apply_ticker(event)
        elif isinstance(event, MarketCatalog):
            self._directory.apply_catalog(event)
        elif isinstance(event, StatusReport):
            self._directory.apply_status(event, received_mono_ns=int(self._clock.mono_ns()))

    def _fan_out(self, observation: Observation, event: BusEvent) -> None:
        """Offer each follower, in one batch, the messages one bus message means for it."""
        outbox: dict[ClientSession, list[ServerMessage]] = {}

        def send(ticker: str, message: ServerMessage) -> None:
            for session in self._followers.get(ticker, ()):
                outbox.setdefault(session, []).append(message)

        snapshotted: set[str] = set()
        for change in observation.changes:
            ticker = change.ticker
            if ticker not in self._followers:
                continue
            if change.after == BOOK_UNKNOWN:
                send(ticker, ResyncMessage(ticker=ticker, reason=RESYNC_BUS_LOSS))
            elif change.before == BOOK_UNKNOWN or change.after == BOOK_FRESH:
                snapshot = self.snapshot(ticker)
                if snapshot is not None:
                    send(ticker, snapshot)
                    snapshotted.add(ticker)
            else:
                send(ticker, BookMessage(ticker=ticker, book="stale"))
        message = self._event_message(observation, event, snapshotted)
        if message is not None:
            send(message.ticker, message)
        for session, messages in outbox.items():
            session.offer(messages)

    def _event_message(
        self, observation: Observation, event: BusEvent, snapshotted: set[str]
    ) -> SnapshotMessage | DeltaMessage | TradeMessage | TickerMessage | None:
        """The message an event itself carries to its market's followers, beyond status changes."""
        if isinstance(event, Trade | Ticker) and event.ticker in self._followers:
            return _market_message(event)
        if (
            isinstance(event, BookDelta | BookSnapshot)
            and observation.applied
            and event.ticker in self._followers
        ):
            return self._book_message(event, snapshotted)
        return None

    def _book_message(
        self, event: BookDelta | BookSnapshot, snapshotted: set[str]
    ) -> DeltaMessage | SnapshotMessage | None:
        """The message a book event applied to a held copy carries."""
        if isinstance(event, BookDelta):
            return DeltaMessage(
                ticker=event.ticker,
                ts_ms=event.ts_ms,
                side=_side(event.side),
                price_e4=event.price,
                delta_e2=event.delta,
            )
        # An exchange snapshot that freshened a stale book went out with that status change; one
        # that replaced a book that stayed fresh goes out now.
        return None if event.ticker in snapshotted else self.snapshot(event.ticker)

    def _unfollow(self, ticker: str, session: ClientSession) -> None:
        followers = self._followers.get(ticker)
        if followers is None:
            return
        followers.pop(session, None)
        if not followers:
            del self._followers[ticker]


def _market_message(event: Trade | Ticker) -> TradeMessage | TickerMessage:
    if isinstance(event, Trade):
        return TradeMessage(
            ticker=event.ticker,
            ts_ms=event.ts_ms,
            price_e4=event.price,
            count_e2=event.count,
            taker_side=_side(event.taker_side),
        )
    return TickerMessage(
        ticker=event.ticker,
        ts_ms=event.ts_ms,
        bid_e4=event.bid,
        ask_e4=event.ask,
        last_e4=event.last,
        volume_e2=event.volume,
    )


def _side(side: Side) -> BookSide:
    return "bid" if side is Side.BID else "ask"


def _levels(book: Book, side: Side) -> tuple[PriceLevel, ...]:
    return tuple((level.price, level.count) for level in book.levels(side))
