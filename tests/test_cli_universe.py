"""``tape universe preview``: the groups applied to public data and printed, and nothing else."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import httpx
import pytest

from tape.cli import main, preview_universe, render_universe_preview
from tape.client.ratelimit import BucketLimits, BucketRateLimiter
from tape.config import Settings, load_settings
from tape.timeutil import NS_PER_MS, NS_PER_S, Clock, FrozenClock
from tests.fakes.rest_payloads import market_payload, series_payload

NOON: Final = int(datetime(2030, 1, 1, 12, tzinfo=UTC).timestamp())
"""The preview's clock; far enough ahead that no market below has closed by the real clock."""

HEAD: Final = """\
[kalshi]
env = "prod"

[recorder]
data_dir = "data"

[recorder.universe]
min_volume_24h = "10.00"
max_l2_markets = 3
"""
CRYPTO_GROUP: Final = """
[[recorder.universe.groups]]
name = "crypto 15-minute"
series = ["KXBTC15M", "KXGONE"]
events = 1
markets_per_event = 1
"""
UNIVERSE: Final = HEAD + CRYPTO_GROUP
SPORTS_GROUP: Final = """
[[recorder.universe.groups]]
name = "sports"
category = "Sports"
events = 2
markets_per_event = 2
max_markets = 3
"""
PAGES: Final = (
    (
        market_payload(
            "KXBTC15M-30JAN011215-15", volume_24h="900.00", close_time="2030-01-01T12:15:00Z"
        ),
        market_payload(
            "KXBTC15M-30JAN011230-30", volume_24h="5000.00", close_time="2030-01-01T12:30:00Z"
        ),
        market_payload(
            "KXNFLGAME-30JAN01NYJBUF-NYJ", volume_24h="7000.00", close_time="2030-01-01T20:00:00Z"
        ),
        market_payload(
            "KXNFLGAME-30JAN01NYJBUF-BUF", volume_24h="6000.00", close_time="2030-01-01T20:00:00Z"
        ),
    ),
    (
        market_payload(
            "KXMLBGAME-30JAN01NYYBOS-NYY", volume_24h="3000.00", close_time="2030-01-01T23:00:00Z"
        ),
        market_payload(
            "KXMLBGAME-30JAN01NYYBOS-BOS", volume_24h="2500.00", close_time="2030-01-01T23:00:00Z"
        ),
        market_payload(
            "KXNHLGAME-30JAN02BOSNYR-BOS", volume_24h="5.00", close_time="2030-01-02T01:00:00Z"
        ),
    ),
)
SPORTS: Final = ("KXNFLGAME", "KXMLBGAME", "KXNHLGAME")
EXPECTED: Final = "\n".join(
    (
        "Universe at 2030-01-01T12:00:00Z: 7 open markets listed in 500 ms",
        "Series per category: Sports 3",
        "Series with no open market: KXGONE",
        "Markets: ticker, series, 24-hour volume, close time (UTC)",
        "",
        "1. crypto 15-minute: series KXBTC15M, KXGONE; events 1 per series; markets_per_event 1",
        "   admitted 1, events 1, skipped for budget 0",
        "   KXBTC15M-30JAN011215",
        "     KXBTC15M-30JAN011215-15      KXBTC15M    900.00  2030-01-01T12:15:00Z",
        "",
        "2. sports: category Sports; events 2; markets_per_event 2; max_markets 3",
        "   admitted 2, events 1, skipped for budget 1",
        "   KXNFLGAME-30JAN01NYJBUF",
        "     KXNFLGAME-30JAN01NYJBUF-NYJ  KXNFLGAME  7000.00  2030-01-01T20:00:00Z",
        "     KXNFLGAME-30JAN01NYJBUF-BUF  KXNFLGAME  6000.00  2030-01-01T20:00:00Z",
        "",
        "Total: admitted 3 of max_l2_markets 3, skipped for budget 1",
        "Not recorded: duplicate 0, not_active 0, mve 0, closed 0, beyond_horizon 0, no_group 0, "
        "event_beyond_horizon 0, below_volume 1, event_not_chosen 1, over_markets_per_event 0, "
        "over_max_markets 1, over_cap 1",
        "",
    )
)


class Exchange:
    """Kalshi's public endpoints: two pages of open markets and the Sports series.

    Each request moves the clock, when there is one, by 250 ms.
    """

    def __init__(
        self,
        clock: FrozenClock | None = None,
        *,
        markets_status: int = 200,
        series_status: int = 200,
    ) -> None:
        self.requests: list[httpx.Request] = []
        self._clock = clock
        self._markets_status = markets_status
        self._series_status = series_status

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self._clock is not None:
            self._clock.advance(250 * NS_PER_MS)
        path = request.url.path.removeprefix("/trade-api/v2")
        if path == "/markets" and self._markets_status == 200:
            last = request.url.params.get("cursor") == "page-2"
            body = {"markets": list(PAGES[1 if last else 0]), "cursor": "" if last else "page-2"}
            return httpx.Response(200, json=body)
        if path == "/series" and self._series_status == 200:
            body = {"series": [series_payload(ticker, "Sports") for ticker in SPORTS]}
            return httpx.Response(200, json=body)
        return httpx.Response(self._markets_status if path == "/markets" else self._series_status)

    def paths(self) -> list[str]:
        return [request.url.path.removeprefix("/trade-api/v2") for request in self.requests]


@pytest.fixture
def root_logger() -> Iterator[logging.Logger]:
    """Restore the root logger that the command configures."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield root
    for handler in list(root.handlers):
        root.removeHandler(handler)
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(level)


def write_config(tmp_path: Path, text: str = UNIVERSE + SPORTS_GROUP) -> Path:
    path = tmp_path / "tape.toml"
    path.write_text(text)
    return path


async def preview_with(settings: Settings, exchange: Exchange, clock: FrozenClock) -> str:
    transport = httpx.MockTransport(exchange)
    rest_url = settings.kalshi.endpoints.rest_url
    async with httpx.AsyncClient(transport=transport, base_url=rest_url) as http:
        return render_universe_preview(await preview_universe(settings, http=http, clock=clock))


async def test_the_preview_applies_the_groups_to_public_data_without_signing(
    tmp_path: Path,
) -> None:
    settings = load_settings(write_config(tmp_path), environ={})
    clock = FrozenClock(mono_ns=1, wall_ns=NOON * NS_PER_S)
    exchange = Exchange(clock)

    assert await preview_with(settings, exchange, clock) == EXPECTED
    assert [(request.url.path, dict(request.url.params)) for request in exchange.requests] == [
        ("/trade-api/v2/markets", {"status": "open", "limit": "1000", "mve_filter": "exclude"}),
        (
            "/trade-api/v2/markets",
            {"status": "open", "limit": "1000", "mve_filter": "exclude", "cursor": "page-2"},
        ),
        ("/trade-api/v2/series", {"category": "Sports"}),
    ]
    assert not any(
        name.startswith("kalshi-access") for r in exchange.requests for name in r.headers
    )
    assert not (tmp_path / "data").exists()


async def test_the_preview_paces_itself_for_a_client_without_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without credentials the preview cannot read its tier, and unsigned pages sent at five a
    second were refused, so it asks for two requests a second."""
    built: list[tuple[BucketLimits, BucketLimits]] = []

    def limiter(clock: Clock, *, read: BucketLimits, write: BucketLimits) -> BucketRateLimiter:
        built.append((read, write))
        return BucketRateLimiter(clock, read=read, write=write)

    monkeypatch.setattr("tape.cli.BucketRateLimiter", limiter)
    settings = load_settings(write_config(tmp_path), environ={})
    clock = FrozenClock(mono_ns=1, wall_ns=NOON * NS_PER_S)

    await preview_with(settings, Exchange(clock), clock)
    two_per_second = BucketLimits(refill_per_s=20, capacity=20)
    assert built == [(two_per_second, two_per_second)]


async def test_unknown_categories_are_reported_and_category_groups_admit_nothing(
    tmp_path: Path,
) -> None:
    settings = load_settings(write_config(tmp_path), environ={})
    clock = FrozenClock(mono_ns=1, wall_ns=NOON * NS_PER_S)

    text = await preview_with(settings, Exchange(clock, series_status=500), clock)
    assert "Series categories unknown: category groups admit nothing\n" in text
    assert "Series per category" not in text
    assert "   admitted 0, events 0, skipped for budget 0\n" in text
    # Five sports markets without a category, and the later of the two 15-minute events.
    assert "no_group 5, event_beyond_horizon 0, below_volume 0, event_not_chosen 1," in text


async def test_a_group_horizon_is_shown_and_sets_aside_events_closing_later(
    tmp_path: Path,
) -> None:
    config = write_config(tmp_path, UNIVERSE + SPORTS_GROUP + "max_hours_to_close = 10\n")
    settings = load_settings(config, environ={})
    clock = FrozenClock(mono_ns=1, wall_ns=NOON * NS_PER_S)

    text = await preview_with(settings, Exchange(clock), clock)
    assert (
        "2. sports: category Sports; events 2; markets_per_event 2; max_markets 3; "
        "max_hours_to_close 10\n"
        "   admitted 2, events 1, skipped for budget 0\n"
    ) in text
    # The game at 23:00 and the one after midnight close more than ten hours after noon.
    assert "no_group 0, event_beyond_horizon 3, below_volume 0, event_not_chosen 1," in text


async def test_a_universe_without_groups_says_no_market_would_be_recorded(tmp_path: Path) -> None:
    settings = load_settings(write_config(tmp_path, HEAD), environ={})
    clock = FrozenClock(mono_ns=1, wall_ns=NOON * NS_PER_S)
    exchange = Exchange(clock)

    text = await preview_with(settings, exchange, clock)
    assert text.splitlines()[1] == (
        "Warning: recorder.universe has no groups, so tape record would record no market; "
        "add [[recorder.universe.groups]] tables (ADR 0028)"
    )
    assert "Total: admitted 0 of max_l2_markets 3, skipped for budget 0\n" in text
    assert exchange.paths() == ["/markets", "/markets"]


async def test_without_a_category_group_no_series_are_requested(tmp_path: Path) -> None:
    settings = load_settings(write_config(tmp_path, UNIVERSE), environ={})
    clock = FrozenClock(mono_ns=1, wall_ns=NOON * NS_PER_S)
    exchange = Exchange(clock)

    text = await preview_with(settings, exchange, clock)
    assert exchange.paths() == ["/markets", "/markets"]
    assert "Series per category" not in text
    assert "Series categories unknown" not in text
    assert "Total: admitted 1 of max_l2_markets 3, skipped for budget 0\n" in text


@pytest.mark.usefixtures("root_logger")
def test_the_command_prints_the_preview_and_needs_no_credentials(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    exchange = Exchange()

    def build_client(base_url: str, *, timeout_s: float) -> httpx.AsyncClient:
        assert timeout_s == 10
        return httpx.AsyncClient(transport=httpx.MockTransport(exchange), base_url=base_url)

    monkeypatch.setattr("tape.cli.build_client", build_client)
    assert main(["universe", "preview", "--config", str(write_config(tmp_path))]) == 0

    out = capsys.readouterr().out
    assert "7 open markets listed in" in out
    assert "     KXNFLGAME-30JAN01NYJBUF-NYJ  KXNFLGAME  7000.00  2030-01-01T20:00:00Z\n" in out
    assert out.endswith("over_max_markets 1, over_cap 1\n")
    assert str(exchange.requests[0].url).startswith(
        "https://external-api.kalshi.com/trade-api/v2/markets?"
    )
    assert not (tmp_path / "data").exists()


@pytest.mark.usefixtures("root_logger")
def test_a_listing_that_fails_exits_1_and_prints_no_preview(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    exchange = Exchange(markets_status=503)

    def build_client(base_url: str, *, timeout_s: float) -> httpx.AsyncClient:
        _ = timeout_s
        return httpx.AsyncClient(transport=httpx.MockTransport(exchange), base_url=base_url)

    monkeypatch.setattr("tape.cli.build_client", build_client)
    assert main(["universe", "preview", "--config", str(write_config(tmp_path))]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "universe preview failed" in captured.err
    assert exchange.paths() == ["/markets"]
