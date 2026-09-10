"""The ``tape`` command line and the composition root of every process.

Responsibility: parse arguments, configure logging, load settings, and construct the real
dependencies (clock, signer, rate limiter, REST client, WebSocket sessions, segment sinks, the
bus publisher and subscriber, the live API and its server) that the adapters receive already
built (docs/ENGINEERING_STANDARDS.md 2.2). It is the only module that constructs ``SystemClock``,
reads ``os.environ``, installs signal handlers, binds the API's listening socket, or writes to
standard output.

Invariants: exit status 0 means the command did what it was asked and, for ``record`` and
``serve``, shut down cleanly; 1 means a configuration error or a failure, with the reason on
standard error or in the log; 2 is argparse's usage error. Logging is configured once per
invocation, before any adapter is built.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import secrets
import signal
import socket
import sys
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import httpx
import msgspec
import uvicorn
from starlette.applications import Starlette

from tape.api import (
    LiveHub,
    MarketDirectory,
    MetadataResolver,
    ServeConfig,
    create_app,
    metadata_limits,
)
from tape.api.contract import CLOSE_GOING_AWAY
from tape.book import Book
from tape.bus.ports import Subscriber
from tape.bus.sockets import ZmqPublisher, ZmqSubscriber
from tape.client.auth import RsaPssSigner
from tape.client.ratelimit import BucketRateLimiter
from tape.client.rest import KalshiRest, build_client
from tape.client.ws import WsSession
from tape.config import Settings, load_settings, redacted, signing_credentials
from tape.errors import ConfigError
from tape.recorder.auditor import Auditor
from tape.recorder.recorder import Recorder, RecorderConfig
from tape.recorder.tap import BookTap
from tape.recorder.writer import HeaderFactory, SegmentSink
from tape.timeutil import NS_PER_MS, NS_PER_S, Clock, SystemClock

__all__ = [
    "CHECK_TARGETS",
    "LOG_FORMATS",
    "SERVE_SHUTDOWN_TIMEOUT_S",
    "AuditTask",
    "JsonLogFormatter",
    "LiveApi",
    "TextLogFormatter",
    "build_api",
    "build_recorder",
    "configure_logging",
    "listen_socket",
    "main",
    "recorder_config",
    "serve_api",
    "serve_config",
]

LOG_FORMATS: Final = ("text", "json")
"""``--log-format`` choices: human-readable lines, or one JSON object per line."""

CHECK_TARGETS: Final = ("record", "serve")
"""``config check --for`` choices: the command whose requirements are checked. ``record`` needs
the signing credentials; ``serve`` holds none and needs ``recorder.bus_endpoint``."""

_LOG_LEVELS: Final = ("DEBUG", "INFO", "WARNING", "ERROR")
_EXIT_OK: Final = 0
_EXIT_FAILURE: Final = 1
_STOP_SIGNALS: Final = (signal.SIGINT, signal.SIGTERM)
# uvicorn attaches a colored copy of some messages as ``color_message``; it is not a field.
_RESERVED_RECORD_FIELDS: Final = frozenset(
    vars(logging.LogRecord("", logging.INFO, "", 0, "", None, None))
) | {"message", "asctime", "color_message"}

SERVE_SHUTDOWN_TIMEOUT_S: Final = 10
"""Deadline for each stage of the live API's shutdown before stragglers are cancelled."""

_WS_MAX_FRAME_BYTES: Final = 64 * 1024
"""Frames uvicorn accepts before closing with 1009. The contract's 4 KB limit, enforced with 1008
by the session, sits well below it; this bound only keeps a hostile frame out of memory."""

_LISTEN_BACKLOG: Final = 128

_log = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    """Run one ``tape`` command.

    Args:
        argv: Arguments after the program name; ``None`` reads ``sys.argv``.

    Returns:
        The process exit status.
    """
    args = _parser().parse_args(argv)
    command: Callable[[argparse.Namespace], int] = args.command
    return command(args)


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", type=Path, required=True, help="path to tape.toml")
    common.add_argument("--log-format", choices=LOG_FORMATS, default="text")
    common.add_argument("--log-level", choices=_LOG_LEVELS, default="INFO")

    parser = argparse.ArgumentParser(prog="tape", description="Kalshi order-book flight recorder.")
    commands = parser.add_subparsers(title="commands", required=True)
    config = commands.add_parser("config", help="inspect the configuration")
    config_commands = config.add_subparsers(title="config commands", required=True)
    check = config_commands.add_parser(
        "check", parents=[common], help="validate and print the effective settings"
    )
    check.add_argument(
        "--for",
        dest="target",
        choices=CHECK_TARGETS,
        default="record",
        help="also check what this command needs (default: record, which needs credentials)",
    )
    check.set_defaults(command=_config_check)
    record = commands.add_parser(
        "record", parents=[common], help="record market data until SIGINT or SIGTERM"
    )
    record.set_defaults(command=_record)
    serve = commands.add_parser(
        "serve", parents=[common], help="serve the live API until SIGINT or SIGTERM"
    )
    serve.set_defaults(command=_serve)
    return parser


def _config_check(args: argparse.Namespace) -> int:
    settings = _load(args.config)
    if settings is None:
        return _EXIT_FAILURE
    try:
        if args.target == "serve":
            _require_bus_endpoint(settings)
        else:
            signing_credentials(settings)
    except ConfigError as exc:
        _report_config_error(exc)
        return _EXIT_FAILURE
    shown = msgspec.json.format(msgspec.json.encode(redacted(settings)), indent=2)
    sys.stdout.write(f"{shown.decode()}\n")
    return _EXIT_OK


def _record(args: argparse.Namespace) -> int:
    configure_logging(log_format=args.log_format, level=args.log_level)
    settings = _load(args.config)
    if settings is None:
        return _EXIT_FAILURE
    try:
        signing_credentials(settings)
    except ConfigError as exc:
        _report_config_error(exc)
        return _EXIT_FAILURE
    try:
        asyncio.run(_run_recorder(settings))
    except ConfigError as exc:
        _report_config_error(exc)
        return _EXIT_FAILURE
    except Exception:
        _log.exception("recorder stopped by a failure")
        return _EXIT_FAILURE
    return _EXIT_OK


def _serve(args: argparse.Namespace) -> int:
    configure_logging(log_format=args.log_format, level=args.log_level)
    settings = _load(args.config)
    if settings is None:
        return _EXIT_FAILURE
    try:
        _require_bus_endpoint(settings)
    except ConfigError as exc:
        _report_config_error(exc)
        return _EXIT_FAILURE
    try:
        asyncio.run(_run_api(settings))
    except ConfigError as exc:
        _report_config_error(exc)
        return _EXIT_FAILURE
    except Exception:
        _log.exception("live API stopped by a failure")
        return _EXIT_FAILURE
    return _EXIT_OK


_NO_BUS_ENDPOINT: Final = (
    "recorder.bus_endpoint is not set; tape serve follows the recorder's bus at that endpoint"
)


def _require_bus_endpoint(settings: Settings) -> str:
    """The bus endpoint ``tape serve`` follows.

    Raises:
        ConfigError: If ``recorder.bus_endpoint`` is not set.
    """
    endpoint = settings.recorder.bus_endpoint
    if endpoint is None:
        raise ConfigError(_NO_BUS_ENDPOINT)
    return endpoint


def _load(path: Path) -> Settings | None:
    """Load settings, printing the reason and returning ``None`` if they are invalid."""
    try:
        return load_settings(path, environ=os.environ)
    except ConfigError as exc:
        _report_config_error(exc)
        return None


def _report_config_error(exc: ConfigError) -> None:
    sys.stderr.write(f"tape: configuration error: {exc}\n")


async def _run_recorder(settings: Settings) -> None:
    """Build the recorder on real I/O and run it until a stop signal.

    Raises:
        ConfigError: If the signing credentials are not set or the private key cannot be loaded.
        Exception: Whatever :meth:`Recorder.run` raised.
    """
    clock = SystemClock()
    timeout_s = settings.kalshi.rest_timeout_s
    # The composition root opened the transport, so it closes it, on every exit path.
    async with build_client(settings.kalshi.endpoints.rest_url, timeout_s=timeout_s) as http:
        recorder = build_recorder(settings, http=http, clock=clock, host=socket.gethostname())
        loop = asyncio.get_running_loop()

        def on_signal(signum: signal.Signals) -> None:
            _log.info("stop signal received; shutting down", extra={"signal": signum.name})
            recorder.request_stop()
            # A second signal gets the default behavior, so a stuck shutdown can be forced.
            for stop_signal in _STOP_SIGNALS:
                loop.remove_signal_handler(stop_signal)

        for stop_signal in _STOP_SIGNALS:
            loop.add_signal_handler(stop_signal, on_signal, stop_signal)
        try:
            await recorder.run()
        finally:
            for stop_signal in _STOP_SIGNALS:
                loop.remove_signal_handler(stop_signal)


async def _run_api(settings: Settings) -> None:
    """Serve the live API on real I/O until a stop signal.

    Raises:
        ConfigError: If ``recorder.bus_endpoint`` is not set.
        OSError: If the listening socket cannot be bound.
        Exception: Whatever :func:`serve_api` raised.
    """
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def on_signal(signum: signal.Signals) -> None:
        _log.info("stop signal received; shutting down", extra={"signal": signum.name})
        stop.set()
        # A second signal gets the default behavior, so a stuck shutdown can be forced.
        for stop_signal in _STOP_SIGNALS:
            loop.remove_signal_handler(stop_signal)

    serve = settings.serve
    with listen_socket(serve.listen_host, serve.listen_port) as sock:
        timeout_s = settings.kalshi.rest_timeout_s
        async with build_client(settings.kalshi.endpoints.rest_url, timeout_s=timeout_s) as http:
            for stop_signal in _STOP_SIGNALS:
                loop.add_signal_handler(stop_signal, on_signal, stop_signal)
            try:
                await serve_api(settings, http=http, clock=SystemClock(), stop=stop, sock=sock)
            finally:
                for stop_signal in _STOP_SIGNALS:
                    loop.remove_signal_handler(stop_signal)


def listen_socket(host: str, port: int) -> socket.socket:
    """Bind the live API's listening socket, so a taken port fails before anything else starts.

    Args:
        host: Address or host name to bind.
        port: TCP port; 0 picks a free one.

    Returns:
        A bound, not yet listening, TCP socket; the caller closes it.

    Raises:
        OSError: If the address does not resolve or cannot be bound.
    """
    family, kind, proto, _, address = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)[0]
    sock = socket.socket(family, kind, proto)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(address)
    except OSError:
        sock.close()
        raise
    return sock


async def serve_api(
    settings: Settings,
    *,
    http: httpx.AsyncClient,
    clock: Clock,
    stop: asyncio.Event,
    sock: socket.socket,
) -> None:
    """Run the live API until ``stop`` is set or a part of it fails, then shut down in order.

    Shutdown closes every live connection with 1001, lets the server finish its handlers within
    :data:`SERVE_SHUTDOWN_TIMEOUT_S`, closes the bus subscriber, and stops the metadata resolver.
    The caller owns and closes ``http`` and ``sock``.

    Args:
        settings: Loaded settings; ``recorder.bus_endpoint`` must be set.
        http: REST transport for public metadata at ``settings.kalshi.endpoints.rest_url``.
        clock: The process clock.
        stop: Set to shut down.
        sock: The bound listening socket.

    Raises:
        ConfigError: If ``recorder.bus_endpoint`` is not set.
        BusError: If the bus endpoint cannot be connected or followed.
        RuntimeError: If the server or the bus follower ended before ``stop`` was set.
        Exception: Whatever else ended the server, the hub, or the resolver.
    """
    endpoint = _require_bus_endpoint(settings)
    subscriber = ZmqSubscriber(endpoint, receive_hwm=settings.serve.bus_receive_hwm)
    try:
        api = build_api(settings, http=http, clock=clock, subscriber=subscriber)
        server = _EmbeddedServer(
            uvicorn.Config(
                api.app,
                ws="websockets-sansio",
                ws_max_size=_WS_MAX_FRAME_BYTES,
                lifespan="off",
                log_config=None,
                access_log=False,
                server_header=False,
                backlog=_LISTEN_BACKLOG,
                timeout_graceful_shutdown=SERVE_SHUTDOWN_TIMEOUT_S,
            )
        )
        address = sock.getsockname()
        _log.info("live API listening", extra={"address": address, "bus_endpoint": endpoint})
        await _supervise_api(api, server, sock=sock, stop=stop)
    finally:
        subscriber.close()
    _log.info("live API stopped")


async def _supervise_api(
    api: LiveApi, server: uvicorn.Server, *, sock: socket.socket, stop: asyncio.Event
) -> None:
    """Run the server, the hub, and the resolver until a stop or a failure, then stop them."""
    tasks = {
        "http-server": asyncio.create_task(server.serve(sockets=[sock]), name="http-server"),
        "live-hub": asyncio.create_task(api.hub.run(), name="live-hub"),
        "metadata-resolver": asyncio.create_task(api.resolver.run(), name="metadata-resolver"),
    }
    stopping = asyncio.ensure_future(stop.wait())
    try:
        await asyncio.wait([*tasks.values(), stopping], return_when=asyncio.FIRST_COMPLETED)
    finally:
        stopping.cancel()
        api.hub.close_sessions(CLOSE_GOING_AWAY)
        server.should_exit = True
        await asyncio.wait({tasks["http-server"]}, timeout=2 * SERVE_SHUTDOWN_TIMEOUT_S)
        api.hub.close()
        tasks["metadata-resolver"].cancel()
        _, pending = await asyncio.wait(tasks.values(), timeout=SERVE_SHUTDOWN_TIMEOUT_S)
        for task in pending:
            task.cancel()
    for name, task in tasks.items():
        if not task.done() or task.cancelled():
            continue
        failure = task.exception()
        if failure is not None:
            raise failure
        if not stop.is_set():
            raise RuntimeError(f"{name} ended before the live API was stopped")


class _EmbeddedServer(uvicorn.Server):
    """A uvicorn server that leaves SIGINT and SIGTERM to the composition root."""

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        """Install no signal handlers; ``tape serve`` installs its own and sets the stop event."""
        yield


@dataclass(frozen=True, slots=True)
class LiveApi:
    """The live API's parts, as :func:`build_api` wires them.

    Attributes:
        app: The ASGI application.
        hub: The bus follower; run it beside the application.
        resolver: The metadata resolver; run it beside the application.
    """

    app: Starlette
    hub: LiveHub
    resolver: MetadataResolver


def serve_config(settings: Settings) -> ServeConfig:
    """Translate settings into what the live API allows.

    Args:
        settings: Loaded settings.

    Returns:
        The configuration :class:`tape.api.LiveHub` serves clients with; ``bus_refresh_s`` is the
        recorder's.
    """
    serve = settings.serve
    return ServeConfig(
        allowed_origins=frozenset(serve.allowed_origins),
        max_clients=serve.max_clients,
        max_tickers=serve.max_tickers_per_client,
        client_queue_max=serve.client_queue_max,
        bus_refresh_s=settings.recorder.bus_refresh_s,
    )


def build_api(
    settings: Settings, *, http: httpx.AsyncClient, clock: Clock, subscriber: Subscriber
) -> LiveApi:
    """Wire the live API to the bus and to Kalshi's public metadata.

    The metadata client has no signer, so the API holds no credentials, and a limiter of its own
    at ``serve.metadata_requests_per_s``.

    Args:
        settings: Loaded settings.
        http: REST transport for ``settings.kalshi.endpoints.rest_url``; the caller owns and
            closes it.
        clock: The process clock.
        subscriber: The bus; the hub subscribes it to every topic and closes it.

    Returns:
        The application, the hub, and the resolver.
    """
    serve = settings.serve
    limits = metadata_limits(serve.metadata_requests_per_s)
    limiter = BucketRateLimiter(clock, read=limits, write=limits)
    rest = KalshiRest(settings.kalshi.endpoints.rest_url, http, limiter, clock)
    resolver = MetadataResolver(rest, clock=clock, ttl_s=serve.metadata_ttl_s)
    hub = LiveHub(
        subscriber,
        directory=MarketDirectory(),
        config=serve_config(settings),
        clock=clock,
        request_metadata=resolver.request,
    )
    return LiveApi(
        app=create_app(hub=hub, resolver=resolver, clock=clock), hub=hub, resolver=resolver
    )


def recorder_config(settings: Settings, *, host: str) -> RecorderConfig:
    """Translate settings into the recorder's own configuration.

    Args:
        settings: Loaded settings.
        host: Host name for segment headers.

    Returns:
        The configuration :class:`Recorder` runs.
    """
    kalshi = settings.kalshi
    recorder = settings.recorder
    return RecorderConfig(
        env=kalshi.env,
        ws_url=kalshi.endpoints.ws_url,
        data_dir=recorder.data_dir,
        host=host,
        universe=recorder.universe.policy(),
        max_connections=recorder.max_connections,
        book_connections=recorder.book_connections,
        group_size=recorder.group_size,
        keyframe_interval_s=recorder.keyframe_interval_s,
        universe_refresh_s=recorder.universe_refresh_s,
        status_interval_s=recorder.status_interval_s,
        ticker_silence_timeout_s=kalshi.ws_silence_timeout_s,
        bus_refresh_s=recorder.bus_refresh_s,
    )


def build_recorder(
    settings: Settings, *, http: httpx.AsyncClient, clock: Clock, host: str
) -> Recorder:
    """Wire a :class:`Recorder` to the real exchange.

    Args:
        settings: Loaded settings.
        http: REST transport for ``settings.kalshi.endpoints.rest_url``; the caller owns
            and closes it.
        clock: The process clock.
        host: Host name for segment headers.

    Returns:
        A recorder ready to :meth:`Recorder.run`.

    Raises:
        ConfigError: If the signing credentials are not set, or the private key file fails its
            check or is not a usable RSA key.
        BusError: If ``recorder.bus_endpoint`` is set and cannot be bound.
    """
    kalshi = settings.kalshi
    credentials = signing_credentials(settings)
    signer = RsaPssSigner(credentials.key_id, credentials.private_key_path)
    limiter = BucketRateLimiter(clock)
    rest = KalshiRest(kalshi.endpoints.rest_url, http, limiter, clock, signer)
    ping_interval_ns = kalshi.ws_ping_interval_s * NS_PER_S
    ping_timeout_ns = kalshi.ws_ping_timeout_s * NS_PER_S

    def session(url: str, *, conn_id: int, silence_timeout_ns: int | None) -> WsSession:
        return WsSession(
            url,
            signer,
            clock,
            conn_id=conn_id,
            silence_timeout_ns=silence_timeout_ns,
            ping_interval_ns=ping_interval_ns,
            ping_timeout_ns=ping_timeout_ns,
        )

    def sink(*, conn_id: int, header_factory: HeaderFactory) -> SegmentSink:
        return SegmentSink(
            settings.recorder.data_dir,
            conn_id=conn_id,
            header_factory=header_factory,
            clock=clock,
            max_queued_records=settings.recorder.writer_queue_max,
        )

    # The auditor reads the recorder's books and sinks, and the recorder runs the auditor.
    # These closures bind late, so they reach the recorder assigned just below.
    def books() -> Mapping[str, Book]:
        return recorder.books()

    def sink_for(ticker: str) -> SegmentSink | None:
        return recorder.sink_for(ticker)

    def open_tap(tickers: Collection[str], *, max_events: int) -> BookTap:
        return recorder.open_book_tap(tickers, max_events=max_events)

    auditor = Auditor(
        rest,
        books,
        clock,
        sink_for=sink_for,
        open_tap=open_tap,
        sample_size=settings.recorder.audit_sample,
        lead_ns=settings.recorder.audit_lead_ms * NS_PER_MS,
        settle_ns=settings.recorder.audit_settle_ms * NS_PER_MS,
        tap_max_events=settings.recorder.audit_tap_max_events,
        window_sleep=asyncio.sleep,
    )
    # Bound last, so that a configuration error above leaves no socket behind.
    endpoint = settings.recorder.bus_endpoint
    publisher: ZmqPublisher | None = None
    if endpoint is not None:
        publisher = ZmqPublisher(endpoint, send_hwm=settings.recorder.bus_send_hwm)
        _log.info("bus bound", extra={"endpoint": endpoint})
    recorder = Recorder(
        recorder_config(settings, host=host),
        clock=clock,
        rest=rest,
        limiter=limiter,
        session_builder=session,
        sink_builder=sink,
        sleep=asyncio.sleep,
        jitter=secrets.SystemRandom().random,
        periodic_tasks=(AuditTask(auditor, interval_s=settings.recorder.audit_interval_s),),
        publisher=publisher,
    )
    return recorder


class AuditTask:
    """Runs the book auditor on its interval as one of the recorder's periodic tasks.

    Args:
        auditor: The auditor to run.
        interval_s: Seconds between audit rounds.
    """

    def __init__(self, auditor: Auditor, *, interval_s: int) -> None:
        self._auditor = auditor
        self._interval_s = interval_s

    @property
    def interval_s(self) -> int:
        """Seconds between audit rounds."""
        return self._interval_s

    async def run(self, *, stop: asyncio.Event) -> None:
        """Audit every ``interval_s`` until ``stop`` is set."""
        await self._auditor.run(interval_s=self._interval_s, stop=stop)


def configure_logging(*, log_format: str, level: str) -> None:
    """Send every log record to standard error in the chosen format.

    Args:
        log_format: ``"text"`` or ``"json"``.
        level: Minimum level name, for example ``"INFO"``.

    Raises:
        ValueError: If ``log_format`` is not one of :data:`LOG_FORMATS`.
    """
    if log_format not in LOG_FORMATS:
        raise ValueError(f"log_format must be one of {LOG_FORMATS}, got {log_format!r}")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonLogFormatter() if log_format == "json" else TextLogFormatter())
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)


def _fields(record: logging.LogRecord) -> dict[str, object]:
    """The structured fields a call passed through ``extra``."""
    return {k: v for k, v in vars(record).items() if k not in _RESERVED_RECORD_FIELDS}


def _timestamp(record: logging.LogRecord) -> str:
    return datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds")


class JsonLogFormatter(logging.Formatter):
    """One JSON object per record: time, level, logger, message, then every ``extra`` field."""

    def format(self, record: logging.LogRecord) -> str:
        """Render a record as one line of JSON.

        Args:
            record: The record.

        Returns:
            The JSON text. A field that is not JSON-serializable is rendered with ``repr``.
        """
        entry: dict[str, object] = {
            **_fields(record),
            "ts": _timestamp(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return msgspec.json.encode(entry, enc_hook=repr).decode()


class TextLogFormatter(logging.Formatter):
    """``time LEVEL logger: message key=value ...`` for a person reading a terminal."""

    def format(self, record: logging.LogRecord) -> str:
        """Render a record as one human-readable line, plus a traceback if it has one.

        Args:
            record: The record.

        Returns:
            The text.
        """
        fields = " ".join(f"{key}={value!r}" for key, value in _fields(record).items())
        line = f"{_timestamp(record)} {record.levelname} {record.name}: {record.getMessage()}"
        if fields:
            line = f"{line} {fields}"
        if record.exc_info:
            line = f"{line}\n{self.formatException(record.exc_info)}"
        return line
