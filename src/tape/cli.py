"""The ``tape`` command line and the composition root of every process.

Responsibility: parse arguments, configure logging, load settings, and construct the real
dependencies (clock, signer, rate limiter, REST client, WebSocket sessions, segment sinks, the
bus publisher) that the adapters receive already built (docs/ENGINEERING_STANDARDS.md 2.2). It
is the only module that constructs ``SystemClock``, reads ``os.environ``, installs signal
handlers, or writes to standard output.

Invariants: exit status 0 means the command did what it was asked and, for ``record``,
shut down cleanly; 1 means a configuration error or a failure, with the reason on
standard error or in the log; 2 is argparse's usage error. Logging is configured once per
invocation, before any adapter is built.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import secrets
import signal
import socket
import sys
from collections.abc import Callable, Collection, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import httpx
import msgspec

from tape.book import Book
from tape.bus.sockets import ZmqPublisher
from tape.client.auth import RsaPssSigner
from tape.client.ratelimit import BucketRateLimiter
from tape.client.rest import KalshiRest, build_client
from tape.client.ws import WsSession
from tape.config import Settings, load_settings, redacted
from tape.errors import ConfigError
from tape.recorder.auditor import Auditor
from tape.recorder.recorder import Recorder, RecorderConfig
from tape.recorder.tap import BookTap
from tape.recorder.writer import HeaderFactory, SegmentSink
from tape.timeutil import NS_PER_MS, NS_PER_S, Clock, SystemClock

__all__ = [
    "LOG_FORMATS",
    "AuditTask",
    "JsonLogFormatter",
    "TextLogFormatter",
    "build_recorder",
    "configure_logging",
    "main",
    "recorder_config",
]

LOG_FORMATS: Final = ("text", "json")
"""``--log-format`` choices: human-readable lines, or one JSON object per line."""

_LOG_LEVELS: Final = ("DEBUG", "INFO", "WARNING", "ERROR")
_EXIT_OK: Final = 0
_EXIT_FAILURE: Final = 1
_STOP_SIGNALS: Final = (signal.SIGINT, signal.SIGTERM)
_RESERVED_RECORD_FIELDS: Final = frozenset(
    vars(logging.LogRecord("", logging.INFO, "", 0, "", None, None))
) | {"message", "asctime"}

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
    check.set_defaults(command=_config_check)
    record = commands.add_parser(
        "record", parents=[common], help="record market data until SIGINT or SIGTERM"
    )
    record.set_defaults(command=_record)
    return parser


def _config_check(args: argparse.Namespace) -> int:
    settings = _load(args.config)
    if settings is None:
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
        asyncio.run(_run_recorder(settings))
    except ConfigError as exc:
        _report_config_error(exc)
        return _EXIT_FAILURE
    except Exception:
        _log.exception("recorder stopped by a failure")
        return _EXIT_FAILURE
    return _EXIT_OK


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
        ConfigError: If the private key cannot be loaded.
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
        ConfigError: If the private key file is not a usable RSA key.
        BusError: If ``recorder.bus_endpoint`` is set and cannot be bound.
    """
    kalshi = settings.kalshi
    signer = RsaPssSigner(kalshi.key_id, kalshi.private_key_path)
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
