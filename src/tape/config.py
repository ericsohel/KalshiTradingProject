"""Load, override, and validate the settings every ``tape`` process reads.

Responsibility: turn a TOML file plus ``TAPE_<SECTION>__<KEY>`` environment overrides into
one frozen :class:`Settings` tree, and refuse anything that would make a process misbehave
later (docs/INTERFACES.md 17). This is a shell module: it reads the file system, but it
takes the environment as an argument, so nothing here reads ``os.environ`` behind the
caller's back.

Invariants: a returned ``Settings`` has passed every check in this module, so consumers never
re-validate it, except the signing credentials, which only a command that signs requests needs
and :func:`signing_credentials` checks; Kalshi endpoints derive only from ``kalshi.env``, never
from free text, so a demo key cannot be pointed at production by a typo; every path is absolute,
with ``~`` expanded from the injected ``HOME`` and relative paths resolved against the
configuration file's directory, so the result does not depend on the working directory;
and secrets are never values, only paths to files that no other user can read
(docs/OPERATIONS.md 2).
"""

from __future__ import annotations

import os
import re
import stat
import tomllib
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Any, Final, Literal

import msgspec
import msgspec.inspect

from tape.bake.bake import DEFAULT_MAX_PART_ROWS
from tape.bus.sockets import DEFAULT_SEND_HWM, check_endpoint
from tape.errors import ConfigError, FixedPointError
from tape.fixedpoint import parse_count
from tape.recorder.recorder import (
    DEFAULT_BUS_REFRESH_S,
    MAX_GROUP_SIZE,
    check_book_capacity,
    check_connection_budget,
    check_keyframe_interval,
)
from tape.recorder.universe import (
    MARKET_ORDER_VOLUME,
    MarketOrder,
    UniverseGroup,
    UniversePolicy,
)
from tape.recorder.writer import DEFAULT_MAX_QUEUED_RECORDS

__all__ = [
    "ENDPOINTS",
    "ENV_PREFIX",
    "ENV_SEPARATOR",
    "BakeSettings",
    "Env",
    "KalshiEndpoints",
    "KalshiSettings",
    "RecorderSettings",
    "ServeSettings",
    "Settings",
    "SigningCredentials",
    "UniverseGroupSettings",
    "UniverseSettings",
    "load_settings",
    "redacted",
    "signing_credentials",
]

Env = Literal["prod", "demo"]
"""The Kalshi environment a process talks to. Demo and production credentials are separate."""

ENV_PREFIX: Final = "TAPE_"
"""Prefix of an environment override, for example ``TAPE_RECORDER__GROUP_SIZE``."""

ENV_SEPARATOR: Final = "__"
"""Separates the section and key names of an environment override."""

_PRIVATE_KEY_FORBIDDEN_BITS: Final = stat.S_IRWXG | stat.S_IRWXO
"""A private key readable, writable, or executable by anyone but its owner is refused."""

_SIGNING_NEEDS_IT: Final = "tape record signs every Kalshi request with it; tape serve does not"

_MAX_KEEPALIVE_S: Final = 60
"""Ceiling on the keepalive interval and pong timeout. A dead peer goes unnoticed for up to
their sum, so beyond a minute each a silently dead connection loses more than the
reconnect, capped at 30 seconds of backoff, that the long wait would avoid."""

_MAX_AUDIT_ALLOWANCE_MS: Final = 5_000
"""Ceiling on the audit window's lead and settle allowances (ADR 0021). They absorb the
milliseconds by which the live feed leads or trails a REST snapshot; allowances of seconds
would let a busy market's snapshot match states far from the reply, so a consistent audit
would say little, and every batch would hold its round open that long."""

_MAX_AUDIT_TAP_EVENTS: Final = 50_000
"""Ceiling on the book changes an audit tap holds per market. At about 160 bytes a change, a
full batch of 100 markets at the cap holds under a gigabyte; the widest window the allowances
permit, about eleven seconds, fills under 8,000 at the busiest rate observed (738 a second)."""

_MAX_BUS_REFRESH_S: Final = 60
"""Ceiling on the bus refresh interval. A consumer that lost a message or has just started
serves a market again only at that market's next refresh image (ADR 0022), so the interval is
how long a viewer can wait on a resync; a minute is already long for a live view."""

_MIN_BUS_SEND_HWM: Final = 1_000
"""Floor on the messages queued for one bus subscriber. Every book connection that reconnects
sends up to 500 snapshots at once, and a healthy subscriber should absorb that burst."""

_MAX_BUS_SEND_HWM: Final = 100_000
"""Ceiling on the messages queued for one bus subscriber. The queue lives in the recorder's
memory; at up to a few kilobytes a refresh image, a stalled subscriber at the cap holds
hundreds of megabytes, and more would put the recorder's memory budget at a consumer's mercy."""

_MAX_PORT: Final = 65_535

_MAX_SERVE_CLIENTS: Final = 1_000
"""Ceiling on live WebSocket connections. Each may hold ``client_queue_max`` messages in the API's
memory, on a host it shares with the recorder; the viewer plans for 200 (docs/FRONTEND.md 6)."""

_MAX_TICKERS_PER_CLIENT: Final = 50
"""Ceiling on the markets one live connection follows. A viewer watches a handful
(docs/FRONTEND.md 1), and every market adds a resync and a snapshot to each recovery from lag."""

_MIN_CLIENT_QUEUE: Final = 100
"""Floor on the messages queued for one live connection: room for the resync and the snapshot of
each of the most markets a connection may follow, which a ``client_lag`` recovery queues at once."""

_MAX_CLIENT_QUEUE: Final = 100_000
"""Ceiling on the messages queued for one live connection. At a few hundred bytes a message, a
stalled viewer at the cap holds tens of megabytes before it is resynchronized."""

_MAX_METADATA_REQUESTS_PER_S: Final = 10
"""Ceiling on public metadata requests per second. They leave the recorder's host for titles
that only the viewer uses, so they stay a trickle beside capture (ADR 0023)."""

_MIN_METADATA_TTL_S: Final = 60
"""Floor on how long resolved metadata is served; below a minute the cache would mostly refetch."""

_MAX_METADATA_TTL_S: Final = 86_400
"""Ceiling on how long resolved metadata is served; a corrected title appears within a day."""

_MIN_BAKE_GRACE_S: Final = 60
"""Floor on the bake grace period. A segment sink closes an hour's file within a poll interval of
the hour ending and flushes every second (docs/INTERFACES.md 8.4); a minute leaves room for a
drain that a slow disk delays."""

_MAX_BAKE_GRACE_S: Final = 86_400
"""Ceiling on the bake grace period. Beyond a day, hours wait so long that raw retention and disk
space run out before they are baked."""

_MIN_RAW_RETENTION_HOURS: Final = 24
"""Floor on raw retention. Raw segments are the only way to repair a baker bug (ADR 0001); a day
leaves time to notice one in the day's manifest before its hours are pruned (ADR 0025)."""

_MAX_RAW_RETENTION_HOURS: Final = 8_760
"""Ceiling on raw retention: a year. A host meant to keep raw data longer does not run prune."""

_MIN_PART_ROWS: Final = 10_000
"""Floor on rows per part file; below it an hour of a busy market becomes hundreds of tiny files."""

_MAX_PART_ROWS: Final = 50_000_000
"""Ceiling on rows per part file. A bake sorts one part in memory at a time, at about 80 bytes a
delta row, several copies deep; far beyond the default no host of this project has the memory."""

_INTEGER: Final = re.compile(r"[+-]?[0-9]+")
_TRUE: Final = frozenset({"true", "1", "yes"})
_FALSE: Final = frozenset({"false", "0", "no"})
_HOME: Final = "~"

PositiveInt = Annotated[int, msgspec.Meta(gt=0)]
NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]
Word = Annotated[str, msgspec.Meta(pattern=r"^\S+$")]
Label = Annotated[str, msgspec.Meta(pattern=r"^\S(.*\S)?$")]
"""Text that may hold spaces, such as ``Climate and Weather``, but not start or end with one."""
KeepaliveSeconds = Annotated[int, msgspec.Meta(ge=1, le=_MAX_KEEPALIVE_S)]
AuditAllowanceMs = Annotated[int, msgspec.Meta(ge=1, le=_MAX_AUDIT_ALLOWANCE_MS)]
AuditTapEvents = Annotated[int, msgspec.Meta(ge=1, le=_MAX_AUDIT_TAP_EVENTS)]
BusRefreshSeconds = Annotated[int, msgspec.Meta(ge=1, le=_MAX_BUS_REFRESH_S)]
BusHwm = Annotated[int, msgspec.Meta(ge=_MIN_BUS_SEND_HWM, le=_MAX_BUS_SEND_HWM)]
ListenPort = Annotated[int, msgspec.Meta(ge=1, le=_MAX_PORT)]
Origin = Annotated[str, msgspec.Meta(pattern=r"^https?://[^/?#\s]+$")]
ServeClients = Annotated[int, msgspec.Meta(ge=1, le=_MAX_SERVE_CLIENTS)]
TickersPerClient = Annotated[int, msgspec.Meta(ge=1, le=_MAX_TICKERS_PER_CLIENT)]
ClientQueueMax = Annotated[int, msgspec.Meta(ge=_MIN_CLIENT_QUEUE, le=_MAX_CLIENT_QUEUE)]
MetadataRequestsPerS = Annotated[int, msgspec.Meta(ge=1, le=_MAX_METADATA_REQUESTS_PER_S)]
MetadataTtlSeconds = Annotated[int, msgspec.Meta(ge=_MIN_METADATA_TTL_S, le=_MAX_METADATA_TTL_S)]
BakeGraceSeconds = Annotated[int, msgspec.Meta(ge=_MIN_BAKE_GRACE_S, le=_MAX_BAKE_GRACE_S)]
RawRetentionHours = Annotated[
    int, msgspec.Meta(ge=_MIN_RAW_RETENTION_HOURS, le=_MAX_RAW_RETENTION_HOURS)
]
PartRows = Annotated[int, msgspec.Meta(ge=_MIN_PART_ROWS, le=_MAX_PART_ROWS)]


class KalshiEndpoints(msgspec.Struct, frozen=True, kw_only=True):
    """The REST and WebSocket roots of one Kalshi environment.

    Attributes:
        rest_url: REST root including the version prefix.
        ws_url: WebSocket endpoint.
    """

    rest_url: str
    ws_url: str


ENDPOINTS: Final[Mapping[str, KalshiEndpoints]] = MappingProxyType(
    {
        "prod": KalshiEndpoints(
            rest_url="https://external-api.kalshi.com/trade-api/v2",
            ws_url="wss://external-api-ws.kalshi.com/trade-api/ws/v2",
        ),
        "demo": KalshiEndpoints(
            rest_url="https://external-api.demo.kalshi.co/trade-api/v2",
            ws_url="wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
        ),
    }
)
"""Endpoints by :data:`Env`. The only place a Kalshi URL is chosen."""


class KalshiSettings(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``[kalshi]``: which exchange to talk to, as whom, and how patiently.

    Attributes:
        env: ``"prod"`` or ``"demo"``; the endpoints follow from it.
        key_id: API key id shown when the key was created. Not a secret on its own. Only commands
            that sign requests need it (:func:`signing_credentials`); ``None`` when not set.
        private_key_path: PEM file holding the private key; readable by its owner only. Only
            commands that sign requests need it and check the file; ``None`` when not set.
        rest_timeout_s: Timeout for each REST request.
        ws_ping_interval_s: Seconds between keepalive pings on every WebSocket (ADR 0019).
        ws_ping_timeout_s: Seconds to wait for a keepalive pong before closing the socket.
            The keepalive is the only liveness check; no connection has a data-silence
            timeout, because a quiet connection is healthy (ADR 0019, ADR 0027).
    """

    env: Env
    key_id: Word | None = None
    private_key_path: Path | None = None
    rest_timeout_s: PositiveInt = 10
    ws_ping_interval_s: KeepaliveSeconds = 10
    ws_ping_timeout_s: KeepaliveSeconds = 20

    @property
    def endpoints(self) -> KalshiEndpoints:
        """The REST and WebSocket endpoints of :attr:`env`."""
        return ENDPOINTS[self.env]


class UniverseGroupSettings(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``[[recorder.universe.groups]]``: one rule of the recorded universe (ADR 0028).

    Attributes:
        name: Names the group in the universe log and ``tape universe preview``; unique.
        series: Series tickers the group selects from, in the order their events are admitted.
            Set exactly one of ``series`` and ``category``.
        category: Kalshi series category the group selects from, for example ``"Sports"``.
        events: Events admitted: per series, the nearest, for a series group; across the group,
            the busiest by 24-hour volume summed over the event, for a category group.
        markets_per_event: Most markets admitted from one event, first in ``market_order``.
        max_markets: Most markets the group admits in all; unset, only the other caps apply.
        max_hours_to_close: Only events whose earliest close is within this many hours, inclusive,
            are eligible for the group; unset, events of any horizon are.
        market_order: ``"volume"``, the default, takes an event's busiest markets first;
            ``"near_price"`` takes those nearest the current price first (ADR 0029).

    Raises:
        ValueError: If the group sets both selectors or neither, or lists a series twice; the
            message names the group.
    """

    name: Label
    series: Annotated[tuple[Word, ...], msgspec.Meta(min_length=1)] | None = None
    category: Label | None = None
    events: PositiveInt
    markets_per_event: PositiveInt
    max_markets: PositiveInt | None = None
    max_hours_to_close: PositiveInt | None = None
    market_order: MarketOrder = MARKET_ORDER_VOLUME

    def __post_init__(self) -> None:
        self.group()

    def group(self) -> UniverseGroup:
        """Convert this table into the selector's group.

        Returns:
            The group ``tape.recorder.universe.select`` applies.

        Raises:
            ValueError: If the group sets both selectors or neither, or lists a series twice.
        """
        return UniverseGroup(
            name=self.name,
            events=self.events,
            markets_per_event=self.markets_per_event,
            series=self.series,
            category=self.category,
            max_markets=self.max_markets,
            max_hours_to_close=self.max_hours_to_close,
            market_order=self.market_order,
        )


class UniverseSettings(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``[recorder.universe]``: which markets earn full order-book capture (ADR 0028).

    Attributes:
        min_volume_24h: Floor on an event's 24-hour volume, summed over its markets, for a
            category group to choose it; a fixed-point count string, for example
            ``"1000.00"``. A string, never a TOML float, so the value is exact.
        max_l2_markets: Budget of order-book subscriptions; groups apply in order until it is
            reached.
        exclude_mve: Leave legs of multivariate event collections out.
        groups: The ``[[recorder.universe.groups]]`` tables, in the order they apply. With
            none, nothing is recorded.

    Raises:
        ValueError: If ``min_volume_24h`` is not an exact, non-negative fixed-point count, or
            two groups share a name.
    """

    min_volume_24h: str = "1000.00"
    max_l2_markets: NonNegativeInt = 2000
    exclude_mve: bool = True
    groups: tuple[UniverseGroupSettings, ...] = ()

    def __post_init__(self) -> None:
        self.policy()

    def policy(self) -> UniversePolicy:
        """Convert this section into the selector's policy; the one place the floor is parsed.

        Returns:
            The policy ``tape.recorder.universe.select`` applies.

        Raises:
            ValueError: If ``min_volume_24h`` is not an exact, non-negative count, or two groups
                share a name.
        """
        try:
            floor = parse_count(self.min_volume_24h)
        except FixedPointError as exc:
            raise ValueError(
                f"min_volume_24h must be a fixed-point count such as '1000.00': {exc}"
            ) from exc
        return UniversePolicy(
            min_volume_24h=floor,
            max_l2_markets=self.max_l2_markets,
            groups=tuple(group.group() for group in self.groups),
            exclude_mve=self.exclude_mve,
        )


class RecorderSettings(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``[recorder]``: where the tape goes and how capture is laid out and paced.

    Attributes:
        data_dir: Root of ``raw/`` and ``keyframes/``; created on first write.
        max_connections: Ceiling on WebSocket connections the recorder opens.
        book_connections: Connections carrying order-book groups, after the live-only
            ticker connection and the taped control connection. Together they must carry
            ``universe.max_l2_markets`` at ``group_size`` markets each.
        group_size: Most markets on one book connection (ADR 0020).
        keyframe_interval_s: Seconds between keyframes; whole minutes dividing an hour.
        audit_interval_s: Seconds between REST audits.
        audit_sample: Books sampled per audit.
        audit_lead_ms: How long an audit window is open before its REST request is sent
            (ADR 0021); 1 to 5000.
        audit_settle_ms: How long an audit window stays open after the REST reply; 1 to 5000.
        audit_tap_max_events: Most book changes an audit window holds per market before that
            audit is undecidable; 1 to 50000.
        writer_queue_max: Records one segment sink holds before refusing more.
        universe_refresh_s: Seconds between market listings and replans.
        status_interval_s: Seconds between status log lines.
        bus_endpoint: Where live consumers read the recorder's events and book refresh images
            (ADR 0008, ADR 0022): ``ipc:///absolute/path`` or ``tcp://host:port``. Absent means
            no bus.
        bus_refresh_s: Seconds between refresh images of each book on the bus; 1 to 60.
        bus_send_hwm: Messages queued for one slow bus subscriber before its copies are
            dropped; 1000 to 100000.
        universe: The ``[recorder.universe]`` section.

    Raises:
        ValueError: If the connection layout does not fit ``max_connections``, the book
            connections cannot carry ``universe.max_l2_markets``, the keyframe interval
            does not tile an hour, or the bus endpoint is malformed.
    """

    data_dir: Path
    max_connections: PositiveInt = 16
    book_connections: PositiveInt = 4
    group_size: Annotated[int, msgspec.Meta(ge=1, le=MAX_GROUP_SIZE)] = MAX_GROUP_SIZE
    keyframe_interval_s: PositiveInt = 300
    audit_interval_s: PositiveInt = 300
    audit_sample: PositiveInt = 200
    audit_lead_ms: AuditAllowanceMs = 250
    audit_settle_ms: AuditAllowanceMs = 750
    audit_tap_max_events: AuditTapEvents = 5000
    writer_queue_max: PositiveInt = DEFAULT_MAX_QUEUED_RECORDS
    universe_refresh_s: PositiveInt = 300
    status_interval_s: PositiveInt = 60
    bus_endpoint: str | None = None
    bus_refresh_s: BusRefreshSeconds = DEFAULT_BUS_REFRESH_S
    bus_send_hwm: BusHwm = DEFAULT_SEND_HWM
    universe: UniverseSettings = UniverseSettings()

    def __post_init__(self) -> None:
        check_connection_budget(
            book_connections=self.book_connections, max_connections=self.max_connections
        )
        check_book_capacity(
            max_l2_markets=self.universe.max_l2_markets,
            group_size=self.group_size,
            book_connections=self.book_connections,
        )
        check_keyframe_interval(self.keyframe_interval_s)
        if self.bus_endpoint is not None:
            check_endpoint(self.bus_endpoint)


class ServeSettings(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``[serve]``: where the live API listens, whom it serves, and how it bounds them (ADR 0023).

    The API follows the recorder's bus at ``recorder.bus_endpoint``; there is no second setting
    for it, so the two processes cannot disagree.

    Attributes:
        listen_host: Address to bind; localhost, behind a reverse proxy, in production.
        listen_port: TCP port to bind; 1 to 65535.
        allowed_origins: Exact origins, such as ``https://tape.pages.dev``, that CORS and the
            WebSocket ``Origin`` check accept; at least one.
        max_clients: Live WebSocket connections served at once; 1 to 1000.
        max_tickers_per_client: Markets one live connection may follow; 1 to 50.
        client_queue_max: Messages queued for one live connection before it is resynchronized
            with ``client_lag``; 100 to 100000.
        bus_receive_hwm: Bus messages queued in the API before ZeroMQ drops the API's copies,
            which it then sees as a gap; 1000 to 100000.
        metadata_requests_per_s: Public Kalshi requests per second for titles, categories, and
            price grids; 1 to 10.
        metadata_ttl_s: Seconds resolved metadata is served before it is fetched again; 60 to
            86400.
    """

    listen_host: Word = "127.0.0.1"
    listen_port: ListenPort = 8080
    allowed_origins: Annotated[tuple[Origin, ...], msgspec.Meta(min_length=1)] = (
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    )
    max_clients: ServeClients = 200
    max_tickers_per_client: TickersPerClient = 10
    client_queue_max: ClientQueueMax = 5000
    bus_receive_hwm: BusHwm = DEFAULT_SEND_HWM
    metadata_requests_per_s: MetadataRequestsPerS = 2
    metadata_ttl_s: MetadataTtlSeconds = 3600


class BakeSettings(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``[bake]``: when hours are baked, how long raw data stays, and what a bake holds (ADR 0025).

    ``tape bake`` and ``tape prune`` read the archive under ``recorder.data_dir``.

    Attributes:
        grace_s: Seconds after an hour ends before ``tape bake`` bakes it, so that no segment of it
            is still being written; 60 to 86400.
        raw_retention_hours: Hours after an hour ends before ``tape prune`` may delete its raw
            segments, and then only after a verified bake; 24 to 8760.
        max_part_rows: Rows a part file holds before a bake starts another, which bounds the rows
            sorted in memory at once; 10000 to 50000000.
    """

    grace_s: BakeGraceSeconds = 600
    raw_retention_hours: RawRetentionHours = 72
    max_part_rows: PartRows = DEFAULT_MAX_PART_ROWS


class Settings(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Every setting of a ``tape`` process (docs/INTERFACES.md 17).

    Attributes:
        kalshi: The ``[kalshi]`` section.
        recorder: The ``[recorder]`` section.
        serve: The ``[serve]`` section; every key has a default.
        bake: The ``[bake]`` section; every key has a default.
    """

    kalshi: KalshiSettings
    recorder: RecorderSettings
    serve: ServeSettings = ServeSettings()
    bake: BakeSettings = BakeSettings()


class SigningCredentials(msgspec.Struct, frozen=True, kw_only=True):
    """What a command that signs Kalshi requests needs, checked by :func:`signing_credentials`.

    Attributes:
        key_id: API key id.
        private_key_path: Absolute path of a regular file that only its owner can access.
    """

    key_id: str
    private_key_path: Path


def load_settings(path: Path, *, environ: Mapping[str, str]) -> Settings:
    """Read, override, and validate the settings in a TOML file.

    Environment variables named ``TAPE_<SECTION>__<KEY>`` replace the file's value, for
    example ``TAPE_RECORDER__UNIVERSE__MAX_L2_MARKETS=500``; a list takes comma-separated
    items. Relative paths resolve against the directory holding ``path``, and a leading
    ``~`` expands to ``environ["HOME"]``.

    Args:
        path: The TOML file.
        environ: Environment to take overrides and ``HOME`` from; the caller passes
            ``os.environ`` or a test's own mapping.

    Returns:
        The validated settings, with absolute paths.

    The signing credentials are not required here: ``tape serve`` signs nothing and holds no
    credentials (ADR 0023). A command that signs calls :func:`signing_credentials`.

    Raises:
        ConfigError: If the file cannot be read or parsed, an override names no setting
            or does not parse, a value has the wrong type or range, or the data directory
            fails its file-system check. The message names the setting.
    """
    raw = _read_toml(path)
    applied = _apply_overrides(raw, environ)
    try:
        parsed = msgspec.convert(raw, Settings, dec_hook=_decode_path)
    except msgspec.ValidationError as exc:
        overrides = f" (environment overrides: {', '.join(applied)})" if applied else ""
        raise ConfigError(f"{path}: {exc}{overrides}") from exc
    base = path.absolute().parent
    kalshi = parsed.kalshi
    recorder = parsed.recorder
    key_path = kalshi.private_key_path
    settings = Settings(
        kalshi=msgspec.structs.replace(
            kalshi,
            private_key_path=None
            if key_path is None
            else _resolve(key_path, base, environ, "kalshi.private_key_path"),
        ),
        recorder=msgspec.structs.replace(
            recorder, data_dir=_resolve(recorder.data_dir, base, environ, "recorder.data_dir")
        ),
        serve=parsed.serve,
        bake=parsed.bake,
    )
    _check_data_dir(settings.recorder.data_dir)
    return settings


def signing_credentials(settings: Settings) -> SigningCredentials:
    """Require the key id and private key file that signing Kalshi requests needs.

    Only commands that sign call this: ``tape record``, and ``tape config check`` on its behalf.

    Args:
        settings: Loaded settings.

    Returns:
        The key id and the checked private key path.

    Raises:
        ConfigError: If ``kalshi.key_id`` or ``kalshi.private_key_path`` is not set, or the key
            file is missing, not a regular file, or open to group or others. The message names
            the setting.
    """
    kalshi = settings.kalshi
    if kalshi.key_id is None:
        raise ConfigError(f"kalshi.key_id is not set; {_SIGNING_NEEDS_IT}")
    if kalshi.private_key_path is None:
        raise ConfigError(f"kalshi.private_key_path is not set; {_SIGNING_NEEDS_IT}")
    _check_private_key(kalshi.private_key_path)
    return SigningCredentials(key_id=kalshi.key_id, private_key_path=kalshi.private_key_path)


def redacted(settings: Settings) -> dict[str, Any]:  # Any: nested TOML-shaped builtins
    """Render the effective settings for display, with the endpoints ``env`` selects.

    Nothing is masked because nothing secret is a value: the private key appears only as
    the path to its file, and the key id alone cannot sign a request. Unset credentials show as
    ``None``.

    Args:
        settings: Loaded settings.

    Returns:
        JSON-compatible builtins mirroring the TOML layout, plus ``kalshi.rest_url`` and
        ``kalshi.ws_url``.
    """
    shown: dict[str, Any] = msgspec.to_builtins(settings, enc_hook=_encode_path)  # Any: JSON tree
    endpoints = settings.kalshi.endpoints
    shown["kalshi"]["rest_url"] = endpoints.rest_url
    shown["kalshi"]["ws_url"] = endpoints.ws_url
    return shown


def _read_toml(path: Path) -> dict[str, Any]:  # Any: TOML values of every type
    """Parse a TOML file.

    Raises:
        ConfigError: If the file cannot be read or is not valid TOML.
    """
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except OSError as exc:
        raise ConfigError(f"cannot read configuration {path}: {exc.strerror}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{path}: invalid TOML: {exc}") from exc


def _apply_overrides(raw: dict[str, Any], environ: Mapping[str, str]) -> list[str]:
    """Write every ``TAPE_<SECTION>__<KEY>`` variable into the parsed file, in name order.

    Variables with the prefix but no separator, such as ``TAPE_TEST_ENV``, are not
    overrides and are left alone.

    Args:
        raw: The parsed TOML, mutated in place.
        environ: The environment.

    Returns:
        The names of the variables applied, for error messages.

    Raises:
        ConfigError: If a variable names no setting, names a section, or does not parse.
    """
    schema = msgspec.inspect.type_info(Settings)
    applied: list[str] = []
    for variable in sorted(environ):
        if not variable.startswith(ENV_PREFIX) or ENV_SEPARATOR not in variable:
            continue
        keys = variable.removeprefix(ENV_PREFIX).lower().split(ENV_SEPARATOR)
        target = _setting_type(schema, keys, variable)
        _assign(raw, keys, _coerce(environ[variable], target, variable), variable)
        applied.append(variable)
    return applied


def _setting_type(
    schema: msgspec.inspect.Type, keys: Sequence[str], variable: str
) -> msgspec.inspect.Type:
    """Find the type of the setting an override names.

    Raises:
        ConfigError: If the keys name no setting, a whole section, or a list of tables such as
            ``recorder.universe.groups``, which only the file can express.
    """
    node = schema
    for depth, key in enumerate(keys):
        dotted = ".".join(keys[: depth + 1])
        if _is_table_list(node):
            raise ConfigError(_table_list_message(variable, keys[:depth]))
        if not isinstance(node, msgspec.inspect.StructType):
            raise ConfigError(f"{variable}: {'.'.join(keys[:depth])} is a setting, not a section")
        field = next((f for f in node.fields if f.encode_name == key), None)
        if field is None:
            raise ConfigError(f"{variable}: there is no setting {dotted}")
        node = field.type
    if isinstance(node, msgspec.inspect.StructType):
        raise ConfigError(f"{variable}: {'.'.join(keys)} is a section, not a setting")
    if _is_table_list(node):
        raise ConfigError(_table_list_message(variable, keys))
    return node


def _is_table_list(node: msgspec.inspect.Type) -> bool:
    """Whether a setting is a list of tables, such as ``recorder.universe.groups``."""
    return isinstance(node, msgspec.inspect.VarTupleType) and isinstance(
        node.item_type, msgspec.inspect.StructType
    )


def _table_list_message(variable: str, keys: Sequence[str]) -> str:
    return (
        f"{variable}: {'.'.join(keys)} is a list of tables, which only the configuration file "
        "can set"
    )


def _coerce(text: str, target: msgspec.inspect.Type, variable: str) -> object:
    """Turn an override's text into the TOML value it stands for.

    Strings, literals, and paths stay text and are checked by the conversion that follows.

    Raises:
        ConfigError: If an integer, boolean, or list override does not parse.
    """
    if isinstance(target, msgspec.inspect.IntType):
        if not _INTEGER.fullmatch(text):
            raise ConfigError(f"{variable}: expected an integer, got {text!r}")
        return int(text)
    if isinstance(target, msgspec.inspect.BoolType):
        lowered = text.lower()
        if lowered in _TRUE:
            return True
        if lowered in _FALSE:
            return False
        raise ConfigError(f"{variable}: expected true or false, got {text!r}")
    if isinstance(target, msgspec.inspect.VarTupleType):
        return [item.strip() for item in text.split(",") if item.strip()]
    return text


def _assign(raw: dict[str, Any], keys: Sequence[str], value: object, variable: str) -> None:
    """Set a nested key, creating tables that the file left out.

    Raises:
        ConfigError: If the file holds a non-table value where a table is needed.
    """
    table = raw
    for key in keys[:-1]:
        child = table.setdefault(key, {})
        if not isinstance(child, dict):
            raise ConfigError(f"{variable}: {key} in the file is not a table")
        table = child
    table[keys[-1]] = value


def _decode_path(target: type, value: object) -> object:
    """Decode the one type msgspec does not know, ``Path``, from a TOML string.

    Raises:
        TypeError: If a path is not a string, or another unsupported type is requested;
            msgspec reports it with the setting's location.
    """
    if target is Path and isinstance(value, str):
        return Path(value)
    raise TypeError(f"Expected `str`, got `{type(value).__name__}`")


def _encode_path(value: object) -> object:
    """Encode a ``Path`` as its text for display.

    Raises:
        NotImplementedError: For any other type, as msgspec's hook protocol requires.
    """
    if isinstance(value, Path):
        return str(value)
    raise NotImplementedError(type(value).__name__)


def _resolve(value: Path, base: Path, environ: Mapping[str, str], setting: str) -> Path:
    """Expand ``~`` from the injected ``HOME`` and anchor a relative path at ``base``.

    Raises:
        ConfigError: If ``~`` is used without ``HOME``, or names another user's home.
    """
    text = str(value)
    if text == _HOME or text.startswith(_HOME + "/"):
        home = environ.get("HOME")
        if not home:
            raise ConfigError(f"{setting}: {text} starts with ~ but HOME is not set")
        value = Path(home) / text.removeprefix(_HOME).lstrip("/")
    elif text.startswith(_HOME):
        raise ConfigError(f"{setting}: {text}: only ~ for the current user is supported")
    return value if value.is_absolute() else base / value


def _check_private_key(path: Path) -> None:
    """Require a regular file that only its owner can access (docs/OPERATIONS.md 2).

    Raises:
        ConfigError: If the file is missing, not a regular file, or open to group or others.
    """
    setting = "kalshi.private_key_path"
    try:
        status = path.stat()
    except FileNotFoundError as exc:
        raise ConfigError(f"{setting}: {path} does not exist") from exc
    except OSError as exc:
        raise ConfigError(f"{setting}: cannot inspect {path}: {exc.strerror}") from exc
    if not stat.S_ISREG(status.st_mode):
        raise ConfigError(f"{setting}: {path} is not a regular file")
    if status.st_mode & _PRIVATE_KEY_FORBIDDEN_BITS:
        mode = stat.S_IMODE(status.st_mode)
        raise ConfigError(
            f"{setting}: {path} has mode {mode:04o}, so other users can read or change it; "
            f"run chmod 600 {path}"
        )


def _check_data_dir(path: Path) -> None:
    """Require a writable directory, or a writable directory to create it in.

    Raises:
        ConfigError: If the path or its nearest existing ancestor is not a writable directory.
    """
    setting = "recorder.data_dir"
    try:
        existing = path
        while not existing.exists():
            existing = existing.parent
        if not existing.is_dir():
            raise ConfigError(f"{setting}: {existing} exists and is not a directory")
        if not os.access(existing, os.W_OK | os.X_OK):
            raise ConfigError(f"{setting}: {existing} is not writable")
    except OSError as exc:
        raise ConfigError(f"{setting}: cannot inspect {path}: {exc.strerror}") from exc
