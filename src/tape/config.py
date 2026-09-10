"""Load, override, and validate the settings every ``tape`` process reads.

Responsibility: turn a TOML file plus ``TAPE_<SECTION>__<KEY>`` environment overrides into
one frozen :class:`Settings` tree, and refuse anything that would make a process misbehave
later (docs/INTERFACES.md 17). This is a shell module: it reads the file system, but it
takes the environment as an argument, so nothing here reads ``os.environ`` behind the
caller's back.

Invariants: a returned ``Settings`` has passed every check in this module, so consumers
never re-validate it; Kalshi endpoints derive only from ``kalshi.env``, never from free
text, so a demo key cannot be pointed at production by a typo; every path is absolute,
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

from tape.errors import ConfigError, FixedPointError
from tape.fixedpoint import parse_count
from tape.recorder.recorder import (
    MAX_GROUP_SIZE,
    check_book_capacity,
    check_connection_budget,
    check_keyframe_interval,
)
from tape.recorder.universe import UniversePolicy
from tape.recorder.writer import DEFAULT_MAX_QUEUED_RECORDS

__all__ = [
    "DEFAULT_SHOWCASE_SERIES",
    "ENDPOINTS",
    "ENV_PREFIX",
    "ENV_SEPARATOR",
    "Env",
    "KalshiEndpoints",
    "KalshiSettings",
    "RecorderSettings",
    "Settings",
    "UniverseSettings",
    "load_settings",
    "redacted",
]

Env = Literal["prod", "demo"]
"""The Kalshi environment a process talks to. Demo and production credentials are separate."""

ENV_PREFIX: Final = "TAPE_"
"""Prefix of an environment override, for example ``TAPE_RECORDER__GROUP_SIZE``."""

ENV_SEPARATOR: Final = "__"
"""Separates the section and key names of an environment override."""

DEFAULT_SHOWCASE_SERIES: Final = ("KXBTC15M", "KXPAYROLLS", "KXHIGHNY", "KXFEDDECISION")
"""Series captured in full whatever their volume."""

_PRIVATE_KEY_FORBIDDEN_BITS: Final = stat.S_IRWXG | stat.S_IRWXO
"""A private key readable, writable, or executable by anyone but its owner is refused."""

_MAX_KEEPALIVE_S: Final = 60
"""Ceiling on the keepalive interval and pong timeout. A dead peer goes unnoticed for up to
their sum, so beyond a minute each a silently dead connection loses more than the
reconnect, capped at 30 seconds of backoff, that the long wait would avoid."""

_INTEGER: Final = re.compile(r"[+-]?[0-9]+")
_TRUE: Final = frozenset({"true", "1", "yes"})
_FALSE: Final = frozenset({"false", "0", "no"})
_HOME: Final = "~"

PositiveInt = Annotated[int, msgspec.Meta(gt=0)]
NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]
Word = Annotated[str, msgspec.Meta(pattern=r"^\S+$")]
KeepaliveSeconds = Annotated[int, msgspec.Meta(ge=1, le=_MAX_KEEPALIVE_S)]


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
        key_id: API key id shown when the key was created. Not a secret on its own.
        private_key_path: PEM file holding the private key; readable by its owner only.
        rest_timeout_s: Timeout for each REST request.
        ws_ping_interval_s: Seconds between keepalive pings on every WebSocket (ADR 0019).
        ws_ping_timeout_s: Seconds to wait for a keepalive pong before closing the socket.
        ws_silence_timeout_s: Seconds without an inbound frame after which the live-only
            ``ticker`` connection is declared dead. It always carries traffic, so silence
            there means the subscription stopped; no other connection has this timeout,
            because a quiet connection is healthy.
    """

    env: Env
    key_id: Word
    private_key_path: Path
    rest_timeout_s: PositiveInt = 10
    ws_ping_interval_s: KeepaliveSeconds = 10
    ws_ping_timeout_s: KeepaliveSeconds = 20
    ws_silence_timeout_s: PositiveInt = 60

    @property
    def endpoints(self) -> KalshiEndpoints:
        """The REST and WebSocket endpoints of :attr:`env`."""
        return ENDPOINTS[self.env]


class UniverseSettings(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``[recorder.universe]``: which markets earn full order-book capture.

    Attributes:
        min_volume_24h: 24-hour volume floor as a fixed-point count string, for example
            ``"1000.00"``. A string, never a TOML float, so the value is exact.
        max_l2_markets: Budget of order-book subscriptions; showcase markets may exceed it.
        showcase_series: Series captured whatever their volume.
        exclude_mve: Leave legs of multivariate event collections out.

    Raises:
        ValueError: If ``min_volume_24h`` is not an exact, non-negative fixed-point count.
    """

    min_volume_24h: str = "1000.00"
    max_l2_markets: NonNegativeInt = 2000
    showcase_series: tuple[Word, ...] = DEFAULT_SHOWCASE_SERIES
    exclude_mve: bool = True

    def __post_init__(self) -> None:
        self.policy()

    def policy(self) -> UniversePolicy:
        """Convert this section into the selector's policy; the one place the floor is parsed.

        Returns:
            The policy ``tape.recorder.universe.select`` applies.

        Raises:
            ValueError: If ``min_volume_24h`` is not an exact, non-negative count.
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
            showcase_series=frozenset(self.showcase_series),
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
        writer_queue_max: Records one segment sink holds before refusing more.
        universe_refresh_s: Seconds between market listings and replans.
        status_interval_s: Seconds between status log lines.
        universe: The ``[recorder.universe]`` section.

    Raises:
        ValueError: If the connection layout does not fit ``max_connections``, the book
            connections cannot carry ``universe.max_l2_markets``, or the keyframe interval
            does not tile an hour.
    """

    data_dir: Path
    max_connections: PositiveInt = 16
    book_connections: PositiveInt = 4
    group_size: Annotated[int, msgspec.Meta(ge=1, le=MAX_GROUP_SIZE)] = MAX_GROUP_SIZE
    keyframe_interval_s: PositiveInt = 300
    audit_interval_s: PositiveInt = 300
    audit_sample: PositiveInt = 200
    writer_queue_max: PositiveInt = DEFAULT_MAX_QUEUED_RECORDS
    universe_refresh_s: PositiveInt = 300
    status_interval_s: PositiveInt = 60
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


class Settings(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Every setting of a ``tape`` process (docs/INTERFACES.md 17).

    Attributes:
        kalshi: The ``[kalshi]`` section.
        recorder: The ``[recorder]`` section.
    """

    kalshi: KalshiSettings
    recorder: RecorderSettings


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

    Raises:
        ConfigError: If the file cannot be read or parsed, an override names no setting
            or does not parse, a value has the wrong type or range, or the private key
            or data directory fails its file-system check. The message names the setting.
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
    settings = Settings(
        kalshi=msgspec.structs.replace(
            kalshi,
            private_key_path=_resolve(
                kalshi.private_key_path, base, environ, "kalshi.private_key_path"
            ),
        ),
        recorder=msgspec.structs.replace(
            recorder, data_dir=_resolve(recorder.data_dir, base, environ, "recorder.data_dir")
        ),
    )
    _check_private_key(settings.kalshi.private_key_path)
    _check_data_dir(settings.recorder.data_dir)
    return settings


def redacted(settings: Settings) -> dict[str, Any]:  # Any: nested TOML-shaped builtins
    """Render the effective settings for display, with the endpoints ``env`` selects.

    Nothing is masked because nothing secret is a value: the private key appears only as
    the path to its file, and the key id alone cannot sign a request.

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
        ConfigError: If the keys name no setting, or name a whole section.
    """
    node = schema
    for depth, key in enumerate(keys):
        dotted = ".".join(keys[: depth + 1])
        if not isinstance(node, msgspec.inspect.StructType):
            raise ConfigError(f"{variable}: {'.'.join(keys[:depth])} is a setting, not a section")
        field = next((f for f in node.fields if f.encode_name == key), None)
        if field is None:
            raise ConfigError(f"{variable}: there is no setting {dotted}")
        node = field.type
    if isinstance(node, msgspec.inspect.StructType):
        raise ConfigError(f"{variable}: {'.'.join(keys)} is a section, not a setting")
    return node


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
