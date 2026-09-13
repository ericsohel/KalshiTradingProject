"""Settings: defaults, environment overrides, validation, derived endpoints, and display."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import Final

import msgspec
import pytest

from tape.config import (
    ENDPOINTS,
    BakeSettings,
    KalshiSettings,
    RecorderSettings,
    ServeSettings,
    Settings,
    SigningCredentials,
    UniverseGroupSettings,
    UniverseSettings,
    load_settings,
    redacted,
    signing_credentials,
)
from tape.errors import ConfigError
from tape.fixedpoint import CountE2
from tape.recorder.universe import UniverseGroup, UniversePolicy

ROOT: Final = Path(__file__).resolve().parents[1]
EXAMPLE: Final = ROOT / "config" / "tape.example.toml"
SERVER_EXAMPLE: Final = ROOT / "deploy" / "tape.server.example.toml"
DELETE: Final = object()
"""Marks a key to leave out of a rendered table."""

type TomlValue = str | int | bool | list[str]
type Tables = dict[str, dict[str, TomlValue]]
type GroupTable = dict[str, TomlValue]

VALID_GROUP: Final[GroupTable] = {
    "name": "g",
    "series": ["KXA"],
    "events": 1,
    "markets_per_event": 1,
}


def toml_value(value: TomlValue) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
    return json.dumps(value)  # a JSON string or integer is also a TOML one


def render(tables: Tables) -> str:
    return "\n".join(
        f"[{name}]\n" + "".join(f"{key} = {toml_value(value)}\n" for key, value in body.items())
        for name, body in tables.items()
    )


@pytest.fixture
def key(tmp_path: Path) -> Path:
    path = tmp_path / "keys" / "read.pem"
    path.parent.mkdir()
    # Settings validation checks only that the key file exists with owner-only
    # permissions; parsing the key is the signer's job, so a placeholder suffices.
    path.write_text("placeholder: configuration tests never parse this file\n")
    path.chmod(0o600)
    return path


@pytest.fixture
def tables(key: Path) -> Tables:
    return {
        "kalshi": {"env": "demo", "key_id": "key-1", "private_key_path": str(key)},
        "recorder": {"data_dir": "data"},
    }


def render_with_groups(tables: Tables, *groups: GroupTable) -> str:
    """Render tables, then one ``[[recorder.universe.groups]]`` table per group."""
    return render(tables) + "".join(
        "\n[[recorder.universe.groups]]\n"
        + "".join(f"{key} = {toml_value(value)}\n" for key, value in group.items())
        for group in groups
    )


def load_groups(tmp_path: Path, tables: Tables, *groups: GroupTable) -> Settings:
    path = tmp_path / "tape.toml"
    path.write_text(render_with_groups(tables, *groups))
    return load_settings(path, environ={})


def load(
    tmp_path: Path, tables: Tables, environ: Mapping[str, str] | None = None
) -> RecorderSettings:
    return load_all(tmp_path, tables, environ).recorder


def load_all(tmp_path: Path, tables: Tables, environ: Mapping[str, str] | None = None) -> Settings:
    path = tmp_path / "tape.toml"
    path.write_text(render(tables))
    return load_settings(path, environ={} if environ is None else environ)


@pytest.fixture
def home(tmp_path: Path) -> Path:
    home = tmp_path / "home"
    key = home / ".config" / "tape" / "keys" / "prod-read.pem"
    key.parent.mkdir(parents=True)
    key.write_text("placeholder: configuration tests never parse this file\n")
    key.chmod(0o600)
    return home


def test_only_env_and_the_data_dir_are_required_and_everything_else_has_a_default(
    tmp_path: Path, key: Path, tables: Tables
) -> None:
    path = tmp_path / "tape.toml"
    path.write_text(render(tables))
    settings = load_settings(path, environ={})

    assert settings.kalshi.env == "demo"
    assert settings.kalshi.key_id == "key-1"
    assert settings.kalshi.private_key_path == key
    kalshi = settings.kalshi
    assert kalshi.rest_timeout_s == 10
    assert (kalshi.ws_ping_interval_s, kalshi.ws_ping_timeout_s) == (10, 20)
    assert settings.recorder == RecorderSettings(data_dir=tmp_path / "data")
    recorder = settings.recorder
    assert (recorder.max_connections, recorder.book_connections, recorder.group_size) == (
        16,
        4,
        500,
    )
    assert (recorder.keyframe_interval_s, recorder.audit_interval_s) == (300, 300)
    assert (recorder.audit_sample, recorder.writer_queue_max) == (200, 200_000)
    assert (recorder.audit_lead_ms, recorder.audit_settle_ms, recorder.audit_tap_max_events) == (
        250,
        750,
        5000,
    )
    assert (recorder.universe_refresh_s, recorder.status_interval_s) == (300, 60)
    assert (recorder.bus_endpoint, recorder.bus_refresh_s, recorder.bus_send_hwm) == (
        None,
        10,
        10_000,
    )
    # Without groups nothing is recorded; every host names its own (ADR 0028).
    assert recorder.universe.policy() == UniversePolicy(
        min_volume_24h=CountE2(100_000), max_l2_markets=2000, groups=(), exclude_mve=True
    )
    serve = settings.serve
    assert serve == ServeSettings()
    assert (serve.listen_host, serve.listen_port, serve.allowed_origins) == (
        "127.0.0.1",
        8080,
        ("http://localhost:5173", "http://127.0.0.1:5173"),
    )
    assert (serve.max_clients, serve.max_tickers_per_client, serve.client_queue_max) == (
        200,
        10,
        5000,
    )
    assert (serve.bus_receive_hwm, serve.metadata_requests_per_s, serve.metadata_ttl_s) == (
        10_000,
        2,
        3600,
    )
    bake = settings.bake
    assert bake == BakeSettings()
    assert (bake.grace_s, bake.raw_retention_hours, bake.max_part_rows) == (600, 72, 500_000)
    # Validation reads the file system but never changes it.
    assert not (tmp_path / "data").exists()


def test_the_example_file_is_valid_and_spells_out_the_defaults(tmp_path: Path, home: Path) -> None:
    path = tmp_path / "tape.toml"
    shutil.copy(EXAMPLE, path)
    settings = load_settings(path, environ={"HOME": str(home)})

    assert settings.kalshi.env == "prod"
    assert settings.kalshi.private_key_path == home / ".config/tape/keys/prod-read.pem"
    assert settings.kalshi == KalshiSettings(
        env="prod", key_id=settings.kalshi.key_id, private_key_path=settings.kalshi.private_key_path
    )
    recorder = settings.recorder
    assert msgspec.structs.replace(recorder, universe=UniverseSettings()) == RecorderSettings(
        data_dir=tmp_path / "data"
    )
    assert settings.serve == ServeSettings()
    assert settings.bake == BakeSettings()
    # The groups are the one part of the example that is not a default.
    assert msgspec.structs.replace(recorder.universe, groups=()) == UniverseSettings()
    policy = recorder.universe.policy()
    assert any(group.series is not None for group in policy.groups)
    assert policy.categories == frozenset({"Politics", "Sports"})


def test_the_server_example_records_the_groups_of_adr_0028(tmp_path: Path, home: Path) -> None:
    path = tmp_path / "tape.toml"
    shutil.copy(SERVER_EXAMPLE, path)
    # The host's data directory does not exist here; everything else is read as written.
    environ = {"HOME": str(home), "TAPE_RECORDER__DATA_DIR": str(tmp_path / "data")}
    policy = load_settings(path, environ=environ).recorder.universe.policy()

    assert (policy.min_volume_24h, policy.max_l2_markets, policy.exclude_mve) == (
        CountE2(100_000),
        200,
        True,
    )
    assert policy.groups == (
        UniverseGroup(
            name="crypto 15-minute",
            series=("KXBTC15M", "KXETH15M"),
            events=1,
            markets_per_event=1,
        ),
        UniverseGroup(name="Bitcoin hourly", series=("KXBTCD",), events=1, markets_per_event=6),
        UniverseGroup(
            name="stock indexes hourly",
            series=("KXINXU", "KXNASDAQ100U"),
            events=1,
            markets_per_event=6,
        ),
        UniverseGroup(
            name="economy",
            series=("KXFEDDECISION", "KXCPIYOY", "KXPAYROLLS", "KXAAAGASW"),
            events=1,
            markets_per_event=6,
        ),
        UniverseGroup(
            name="weather",
            series=("KXHIGHNY", "KXHIGHLAX", "KXHIGHCHI", "KXHIGHMIA"),
            events=1,
            markets_per_event=6,
        ),
        UniverseGroup(name="politics", category="Politics", events=5, markets_per_event=1),
        UniverseGroup(
            name="sports",
            category="Sports",
            events=10,
            markets_per_event=1,
            max_hours_to_close=48,
        ),
    )


@pytest.mark.parametrize(
    ("env", "rest_url", "ws_url"),
    [
        (
            "prod",
            "https://external-api.kalshi.com/trade-api/v2",
            "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
        ),
        (
            "demo",
            "https://external-api.demo.kalshi.co/trade-api/v2",
            "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
        ),
    ],
)
def test_endpoints_follow_from_env_alone(
    tmp_path: Path, tables: Tables, env: str, rest_url: str, ws_url: str
) -> None:
    tables["kalshi"]["env"] = env
    path = tmp_path / "tape.toml"
    path.write_text(render(tables))
    endpoints = load_settings(path, environ={}).kalshi.endpoints
    assert (endpoints.rest_url, endpoints.ws_url) == (rest_url, ws_url)
    assert ENDPOINTS[env] == endpoints


def test_an_endpoint_cannot_be_written_in_the_file_or_the_environment(
    tmp_path: Path, tables: Tables
) -> None:
    tables["kalshi"]["ws_url"] = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    with pytest.raises(ConfigError, match="unknown field `ws_url`"):
        load_all(tmp_path, tables)
    del tables["kalshi"]["ws_url"]
    with pytest.raises(ConfigError, match=r"there is no setting kalshi\.rest_url"):
        load_all(tmp_path, tables, {"TAPE_KALSHI__REST_URL": "https://example.com"})


def test_environment_overrides_replace_and_add_values(
    tmp_path: Path, tables: Tables, key: Path
) -> None:
    del tables["kalshi"]["private_key_path"]
    path = tmp_path / "tape.toml"
    path.write_text(render(tables))
    settings = load_settings(
        path,
        environ={
            "TAPE_KALSHI__ENV": "prod",
            "TAPE_KALSHI__PRIVATE_KEY_PATH": str(key),
            "TAPE_KALSHI__WS_PING_INTERVAL_S": "5",
            "TAPE_KALSHI__WS_PING_TIMEOUT_S": "15",
            "TAPE_RECORDER__GROUP_SIZE": "200",
            "TAPE_RECORDER__BOOK_CONNECTIONS": "10",
            "TAPE_RECORDER__AUDIT_LEAD_MS": "5000",
            "TAPE_RECORDER__AUDIT_SETTLE_MS": "1",
            "TAPE_RECORDER__AUDIT_TAP_MAX_EVENTS": "50000",
            "TAPE_RECORDER__BUS_ENDPOINT": "ipc:///run/tape/bus.sock",
            "TAPE_RECORDER__BUS_REFRESH_S": "60",
            "TAPE_RECORDER__BUS_SEND_HWM": "1000",
            "TAPE_RECORDER__UNIVERSE__EXCLUDE_MVE": "false",
            "TAPE_RECORDER__UNIVERSE__MIN_VOLUME_24H": "5.00",
            "TAPE_TEST_ENV": "demo",  # no separator: not an override
            "HOME": "/nowhere",
        },
    )
    assert settings.kalshi.env == "prod"
    assert settings.kalshi.private_key_path == key
    assert (settings.kalshi.ws_ping_interval_s, settings.kalshi.ws_ping_timeout_s) == (5, 15)
    assert (settings.recorder.group_size, settings.recorder.book_connections) == (200, 10)
    recorder = settings.recorder
    assert (recorder.audit_lead_ms, recorder.audit_settle_ms, recorder.audit_tap_max_events) == (
        5000,
        1,
        50000,
    )
    assert (recorder.bus_endpoint, recorder.bus_refresh_s, recorder.bus_send_hwm) == (
        "ipc:///run/tape/bus.sock",
        60,
        1000,
    )
    assert settings.recorder.universe == UniverseSettings(min_volume_24h="5.00", exclude_mve=False)


@pytest.mark.parametrize(
    ("environ", "message"),
    [
        ({"TAPE_RECORDER__GROUP_SIZE": "lots"}, "TAPE_RECORDER__GROUP_SIZE: expected an integer"),
        ({"TAPE_RECORDER__UNIVERSE__EXCLUDE_MVE": "maybe"}, "expected true or false"),
        ({"TAPE_NOPE__X": "1"}, r"there is no setting nope$"),
        ({"TAPE_RECORDER__UNIVERSE": "x"}, r"recorder\.universe is a section, not a setting"),
        ({"TAPE_KALSHI__ENV__X": "1"}, r"kalshi\.env is a setting, not a section"),
        (
            {"TAPE_KALSHI__WS_SILENCE_TIMEOUT_S": "60"},
            r"TAPE_KALSHI__WS_SILENCE_TIMEOUT_S: there is no setting kalshi\.ws_silence_timeout_s$",
        ),
        (
            {"TAPE_KALSHI__WS_PING_TIMEOUT_S": "61"},
            r"<= 60 - at `\$\.kalshi\.ws_ping_timeout_s` \(environment overrides: "
            r"TAPE_KALSHI__WS_PING_TIMEOUT_S\)",
        ),
        (
            {"TAPE_RECORDER__AUDIT_TAP_MAX_EVENTS": "50001"},
            r"<= 50000 - at `\$\.recorder\.audit_tap_max_events` \(environment overrides: "
            r"TAPE_RECORDER__AUDIT_TAP_MAX_EVENTS\)",
        ),
        (
            {"TAPE_RECORDER__GROUP_SIZE": "501"},
            r"<= 500 - at `\$\.recorder\.group_size` \(environment overrides: "
            r"TAPE_RECORDER__GROUP_SIZE\)",
        ),
        (
            {"TAPE_RECORDER__UNIVERSE__SHOWCASE_SERIES": "KXA,KXB"},
            r"there is no setting recorder\.universe\.showcase_series$",
        ),
        (
            {"TAPE_RECORDER__UNIVERSE__GROUPS": "KXA"},
            r"^TAPE_RECORDER__UNIVERSE__GROUPS: recorder\.universe\.groups is a list of tables, "
            r"which only the configuration file can set$",
        ),
        (
            {"TAPE_RECORDER__UNIVERSE__GROUPS__EVENTS": "2"},
            r"^TAPE_RECORDER__UNIVERSE__GROUPS__EVENTS: recorder\.universe\.groups is a list of",
        ),
    ],
)
def test_a_bad_override_names_its_variable(
    tmp_path: Path, tables: Tables, environ: dict[str, str], message: str
) -> None:
    with pytest.raises(ConfigError, match=message):
        load_all(tmp_path, tables, environ)


def test_an_override_into_a_value_that_is_not_a_table_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "tape.toml"
    path.write_text("recorder = 5\n")
    with pytest.raises(ConfigError, match="recorder in the file is not a table"):
        load_settings(path, environ={"TAPE_RECORDER__GROUP_SIZE": "10"})


@pytest.mark.parametrize(
    ("location", "value", "message"),
    [
        (("kalshi", "env"), "staging", r"Invalid enum value 'staging' - at `\$\.kalshi\.env`"),
        (("kalshi", "key_id"), "", r"at `\$\.kalshi\.key_id`"),
        (("kalshi", "key_id"), "two words", r"at `\$\.kalshi\.key_id`"),
        (("kalshi", "private_key_path"), 5, r"Expected `str`, got `int` - at `\$\.kalshi\."),
        (("kalshi", "rest_timeout_s"), 0, r">= 1 - at `\$\.kalshi\.rest_timeout_s`"),
        (("kalshi", "ws_ping_interval_s"), 0, r">= 1 - at `\$\.kalshi\.ws_ping_interval_s`"),
        (("kalshi", "ws_ping_interval_s"), 61, r"<= 60 - at `\$\.kalshi\.ws_ping_interval_s`"),
        (("kalshi", "ws_ping_timeout_s"), 0, r">= 1 - at `\$\.kalshi\.ws_ping_timeout_s`"),
        (("kalshi", "ws_ping_timeout_s"), 61, r"<= 60 - at `\$\.kalshi\.ws_ping_timeout_s`"),
        # Removed by ADR 0027: an old file that still sets it is refused by name.
        (
            ("kalshi", "ws_silence_timeout_s"),
            60,
            r"unknown field `ws_silence_timeout_s` - at `\$\.kalshi`",
        ),
        (("recorder", "data_dir"), DELETE, "missing required field `data_dir`"),
        (("recorder", "max_connections"), 3, r"needs 6 connections.*max_connections = 3"),
        (("recorder", "book_connections"), 0, r"at `\$\.recorder\.book_connections`"),
        (("recorder", "group_size"), 0, r">= 1 - at `\$\.recorder\.group_size`"),
        (("recorder", "group_size"), 501, r"<= 500 - at `\$\.recorder\.group_size`"),
        (("recorder", "keyframe_interval_s"), 90, "whole number of minutes that divides an hour"),
        (("recorder", "keyframe_interval_s"), 420, "whole number of minutes that divides an hour"),
        (("recorder", "keyframe_interval_s"), 0, r"at `\$\.recorder\.keyframe_interval_s`"),
        (("recorder", "audit_interval_s"), 0, r"at `\$\.recorder\.audit_interval_s`"),
        (("recorder", "audit_sample"), 0, r"at `\$\.recorder\.audit_sample`"),
        (("recorder", "audit_lead_ms"), 0, r">= 1 - at `\$\.recorder\.audit_lead_ms`"),
        (("recorder", "audit_lead_ms"), 5001, r"<= 5000 - at `\$\.recorder\.audit_lead_ms`"),
        (("recorder", "audit_settle_ms"), 0, r">= 1 - at `\$\.recorder\.audit_settle_ms`"),
        (("recorder", "audit_settle_ms"), 5001, r"<= 5000 - at `\$\.recorder\.audit_settle_ms`"),
        (("recorder", "audit_tap_max_events"), 0, r">= 1 - at `\$\.recorder\.audit_tap_max"),
        (("recorder", "audit_tap_max_events"), 50001, r"<= 50000 - at `\$\.recorder\.audit_tap"),
        (("recorder", "writer_queue_max"), 0, r"at `\$\.recorder\.writer_queue_max`"),
        (("recorder", "universe_refresh_s"), 0, r"at `\$\.recorder\.universe_refresh_s`"),
        (("recorder", "status_interval_s"), 0, r"at `\$\.recorder\.status_interval_s`"),
        (("recorder", "bus_endpoint"), "ipc://bus.sock", r"ipc path must be absolute - at `\$\."),
        (("recorder", "bus_endpoint"), "udp://127.0.0.1:1", r"ipc:///absolute/path or tcp://"),
        (("recorder", "bus_endpoint"), 5, r"Expected `str \| null`, got `int` - at `\$\.recorder"),
        (("recorder", "bus_refresh_s"), 0, r">= 1 - at `\$\.recorder\.bus_refresh_s`"),
        (("recorder", "bus_refresh_s"), 61, r"<= 60 - at `\$\.recorder\.bus_refresh_s`"),
        (("recorder", "bus_send_hwm"), 999, r">= 1000 - at `\$\.recorder\.bus_send_hwm`"),
        (("recorder", "bus_send_hwm"), 100_001, r"<= 100000 - at `\$\.recorder\.bus_send_hwm`"),
        (("recorder", "surprise"), 1, "unknown field `surprise`"),
        (("serve", "listen_host"), "", r"at `\$\.serve\.listen_host`"),
        (("serve", "listen_port"), 0, r">= 1 - at `\$\.serve\.listen_port`"),
        (("serve", "listen_port"), 65_536, r"<= 65535 - at `\$\.serve\.listen_port`"),
        (("serve", "allowed_origins"), [], r"length >= 1 - at `\$\.serve\.allowed_origins`"),
        (("serve", "allowed_origins"), ["*"], r"at `\$\.serve\.allowed_origins\[0\]`"),
        (
            ("serve", "allowed_origins"),
            ["http://localhost:5173", "https://tape.example/"],
            r"at `\$\.serve\.allowed_origins\[1\]`",
        ),
        (("serve", "max_clients"), 0, r">= 1 - at `\$\.serve\.max_clients`"),
        (("serve", "max_clients"), 1001, r"<= 1000 - at `\$\.serve\.max_clients`"),
        (("serve", "max_tickers_per_client"), 0, r">= 1 - at `\$\.serve\.max_tickers"),
        (("serve", "max_tickers_per_client"), 51, r"<= 50 - at `\$\.serve\.max_tickers"),
        (("serve", "client_queue_max"), 99, r">= 100 - at `\$\.serve\.client_queue_max`"),
        (("serve", "client_queue_max"), 100_001, r"<= 100000 - at `\$\.serve\.client_queue"),
        (("serve", "bus_receive_hwm"), 999, r">= 1000 - at `\$\.serve\.bus_receive_hwm`"),
        (("serve", "bus_receive_hwm"), 100_001, r"<= 100000 - at `\$\.serve\.bus_receive"),
        (("serve", "metadata_requests_per_s"), 0, r">= 1 - at `\$\.serve\.metadata_requests"),
        (("serve", "metadata_requests_per_s"), 11, r"<= 10 - at `\$\.serve\.metadata_request"),
        (("serve", "metadata_ttl_s"), 59, r">= 60 - at `\$\.serve\.metadata_ttl_s`"),
        (("serve", "metadata_ttl_s"), 86_401, r"<= 86400 - at `\$\.serve\.metadata_ttl_s`"),
        (("serve", "bus_endpoint"), "ipc:///tmp/bus.sock", "unknown field `bus_endpoint`"),
        (("bake", "grace_s"), 59, r">= 60 - at `\$\.bake\.grace_s`"),
        (("bake", "grace_s"), 86_401, r"<= 86400 - at `\$\.bake\.grace_s`"),
        (("bake", "raw_retention_hours"), 23, r">= 24 - at `\$\.bake\.raw_retention_hours`"),
        (("bake", "raw_retention_hours"), 8_761, r"<= 8760 - at `\$\.bake\.raw_retention"),
        (("bake", "max_part_rows"), 9_999, r">= 10000 - at `\$\.bake\.max_part_rows`"),
        (("bake", "max_part_rows"), 50_000_001, r"<= 50000000 - at `\$\.bake\.max_part_rows`"),
        (("bake", "raw_retention"), 72, "unknown field `raw_retention`"),
        (
            ("recorder.universe", "min_volume_24h"),
            "1.005",
            r"fixed-point count .* at `\$\.recorder",
        ),
        (
            ("recorder.universe", "min_volume_24h"),
            "-1.00",
            r"fixed-point count .* at `\$\.recorder",
        ),
        (("recorder.universe", "min_volume_24h"), 1000, r"Expected `str`, got `int`"),
        (("recorder.universe", "max_l2_markets"), -1, r"at `\$\.recorder\.universe\.max_l2"),
        # Removed by ADR 0028: an old file that still sets it is refused by name.
        (
            ("recorder.universe", "showcase_series"),
            ["KXBTC15M"],
            r"unknown field `showcase_series` - at `\$\.recorder\.universe`",
        ),
        (("recorder.universe", "exclude_mve"), "yes", r"Expected `bool`, got `str`"),
    ],
)
def test_every_invalid_value_is_refused_with_its_location(
    tmp_path: Path, tables: Tables, location: tuple[str, str], value: object, message: str
) -> None:
    table, setting = location
    body = tables.setdefault(table, {})
    if value is DELETE:
        del body[setting]
    else:
        assert isinstance(value, str | int | bool | list)
        body[setting] = value
    with pytest.raises(ConfigError, match=message):
        load_all(tmp_path, tables)


def test_universe_groups_are_read_in_order_into_the_policy(tmp_path: Path, tables: Tables) -> None:
    tables["recorder.universe"] = {"max_l2_markets": 100}
    settings = load_groups(
        tmp_path,
        tables,
        {
            "name": "crypto 15-minute",
            "series": ["KXBTC15M", "KXETH15M"],
            "events": 1,
            "markets_per_event": 1,
        },
        {
            "name": "sports",
            "category": "Sports",
            "events": 10,
            "markets_per_event": 2,
            "max_markets": 15,
            "max_hours_to_close": 6,
        },
    )
    policy = settings.recorder.universe.policy()
    assert policy.groups == (
        UniverseGroup(
            name="crypto 15-minute",
            series=("KXBTC15M", "KXETH15M"),
            events=1,
            markets_per_event=1,
        ),
        UniverseGroup(
            name="sports",
            category="Sports",
            events=10,
            markets_per_event=2,
            max_markets=15,
            max_hours_to_close=6,
        ),
    )
    assert policy.categories == frozenset({"Sports"})
    assert redacted(settings)["recorder"]["universe"]["groups"][1] == {
        "name": "sports",
        "series": None,
        "category": "Sports",
        "events": 10,
        "markets_per_event": 2,
        "max_markets": 15,
        "max_hours_to_close": 6,
    }


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            {"category": "Sports"},
            r"universe group 'g' must set exactly one of series and category - at "
            r"`\$\.recorder\.universe\.groups\[1\]`",
        ),
        (
            {"series": DELETE},
            r"universe group 'g' must set exactly one of series and category - at "
            r"`\$\.recorder\.universe\.groups\[1\]`",
        ),
        ({"series": []}, r"length >= 1 - at `\$\.recorder\.universe\.groups\[1\]\.series`"),
        ({"series": ["KXA", "KXB", "KXA"]}, r"universe group 'g' lists KXA more than once"),
        ({"series": ["KX A"]}, r"at `\$\.recorder\.universe\.groups\[1\]\.series\[0\]`"),
        ({"series": DELETE, "category": ""}, r"at `\$\.recorder\.universe\.groups\[1\]\.category`"),
        ({"name": ""}, r"at `\$\.recorder\.universe\.groups\[1\]\.name`"),
        ({"name": " padded"}, r"at `\$\.recorder\.universe\.groups\[1\]\.name`"),
        ({"events": 0}, r">= 1 - at `\$\.recorder\.universe\.groups\[1\]\.events`"),
        (
            {"markets_per_event": DELETE},
            r"missing required field `markets_per_event` - at "
            r"`\$\.recorder\.universe\.groups\[1\]`",
        ),
        ({"max_markets": 0}, r">= 1 - at `\$\.recorder\.universe\.groups\[1\]\.max_markets`"),
        (
            {"max_hours_to_close": 0},
            r">= 1 - at `\$\.recorder\.universe\.groups\[1\]\.max_hours_to_close`",
        ),
        (
            {"showcase": True},
            r"unknown field `showcase` - at `\$\.recorder\.universe\.groups\[1\]`",
        ),
    ],
)
def test_every_invalid_group_is_refused_with_its_location(
    tmp_path: Path, tables: Tables, changes: dict[str, object], message: str
) -> None:
    broken = dict(VALID_GROUP)
    for key, value in changes.items():
        if value is DELETE:
            del broken[key]
        else:
            assert isinstance(value, str | int | bool | list)
            broken[key] = value
    with pytest.raises(ConfigError, match=message):
        load_groups(tmp_path, tables, VALID_GROUP | {"name": "first"}, broken)


def test_group_names_must_be_unique(tmp_path: Path, tables: Tables) -> None:
    with pytest.raises(
        ConfigError,
        match=r"universe group names must be unique; repeated: 'g' - at `\$\.recorder\.universe`",
    ):
        load_groups(tmp_path, tables, VALID_GROUP, VALID_GROUP | {"series": ["KXB"]})


def test_a_universe_the_book_connections_cannot_carry_is_refused_with_the_fix(
    tmp_path: Path, tables: Tables
) -> None:
    """Each book connection carries at most group_size markets in one subscription (ADR 0020)."""
    tables["recorder"] |= {"book_connections": 2, "group_size": 500}
    tables["recorder.universe"] = {"max_l2_markets": 1001}
    with pytest.raises(
        ConfigError,
        match=(
            r"max_l2_markets = 1001 needs 3 book connections at group_size = 500, but "
            r"book_connections = 2 carry only 1000 markets; set book_connections to at least 3 "
            r"\(and max_connections to at least 5\) or lower max_l2_markets to 1000"
        ),
    ):
        load_all(tmp_path, tables)
    tables["recorder.universe"] = {"max_l2_markets": 1000}
    assert load(tmp_path, tables).universe.max_l2_markets == 1000
    with pytest.raises(ConfigError, match="set book_connections to at least 5"):
        load_all(tmp_path, tables, {"TAPE_RECORDER__UNIVERSE__MAX_L2_MARKETS": "2001"})


def test_credentials_are_optional_until_a_command_signs(
    tmp_path: Path, tables: Tables, key: Path
) -> None:
    """tape serve holds no credentials (ADR 0023); only tape record, which signs, requires them."""
    del tables["kalshi"]["key_id"]
    del tables["kalshi"]["private_key_path"]
    settings = load_all(tmp_path, tables)
    assert (settings.kalshi.key_id, settings.kalshi.private_key_path) == (None, None)
    assert redacted(settings)["kalshi"]["private_key_path"] is None
    with pytest.raises(ConfigError, match=r"^kalshi\.key_id is not set; tape record signs every"):
        signing_credentials(settings)

    tables["kalshi"]["key_id"] = "key-1"
    with pytest.raises(ConfigError, match=r"^kalshi\.private_key_path is not set; tape record"):
        signing_credentials(load_all(tmp_path, tables))

    tables["kalshi"]["private_key_path"] = str(key.relative_to(tmp_path))
    assert signing_credentials(load_all(tmp_path, tables)) == SigningCredentials(
        key_id="key-1", private_key_path=key
    )


def test_the_private_key_must_be_a_file_only_its_owner_can_read(
    tmp_path: Path, tables: Tables, key: Path
) -> None:
    key.chmod(0o640)
    # Loading never opens the key, so a command that does not sign is unaffected.
    settings = load_all(tmp_path, tables)
    with pytest.raises(ConfigError, match=r"mode 0640, so other users can read .* chmod 600"):
        signing_credentials(settings)
    key.chmod(0o604)
    with pytest.raises(ConfigError, match="mode 0604"):
        signing_credentials(settings)
    tables["kalshi"]["private_key_path"] = str(tmp_path / "missing.pem")
    with pytest.raises(ConfigError, match=r"private_key_path: .*missing\.pem does not exist"):
        signing_credentials(load_all(tmp_path, tables))
    tables["kalshi"]["private_key_path"] = str(key.parent)
    with pytest.raises(ConfigError, match="is not a regular file"):
        signing_credentials(load_all(tmp_path, tables))


def test_the_data_dir_must_be_a_writable_directory(tmp_path: Path, tables: Tables) -> None:
    (tmp_path / "occupied").write_text("")
    tables["recorder"]["data_dir"] = "occupied/tape"
    with pytest.raises(ConfigError, match=r"data_dir: .*occupied exists and is not a directory"):
        load_all(tmp_path, tables)

    locked = tmp_path / "locked"
    locked.mkdir(mode=0o500)
    tables["recorder"]["data_dir"] = "locked/tape"
    try:
        with pytest.raises(ConfigError, match=r"data_dir: .*locked is not writable"):
            load_all(tmp_path, tables)
    finally:
        locked.chmod(0o700)

    tables["recorder"]["data_dir"] = "fresh/nested/tape"
    assert load(tmp_path, tables).data_dir == tmp_path / "fresh/nested/tape"


def test_paths_expand_home_from_the_injected_environment(
    tmp_path: Path, tables: Tables, home: Path
) -> None:
    tables["kalshi"]["private_key_path"] = "~/.config/tape/keys/prod-read.pem"
    tables["recorder"]["data_dir"] = "~"
    settings = load_all(tmp_path, tables, {"HOME": str(home)})
    assert settings.kalshi.private_key_path == home / ".config/tape/keys/prod-read.pem"
    assert settings.recorder.data_dir == home

    with pytest.raises(ConfigError, match="starts with ~ but HOME is not set"):
        load_all(tmp_path, tables, {})
    tables["kalshi"]["private_key_path"] = "~alice/key.pem"
    with pytest.raises(ConfigError, match="only ~ for the current user is supported"):
        load_all(tmp_path, tables, {"HOME": str(home)})


def test_an_unreadable_or_malformed_file_is_a_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"cannot read configuration .*absent\.toml"):
        load_settings(tmp_path / "absent.toml", environ={})
    broken = tmp_path / "broken.toml"
    broken.write_text("[kalshi\nenv = ")
    with pytest.raises(ConfigError, match=r"broken\.toml: invalid TOML"):
        load_settings(broken, environ={})
    empty = tmp_path / "empty.toml"
    empty.write_text("")
    with pytest.raises(ConfigError, match="missing required field `kalshi`"):
        load_settings(empty, environ={})


def test_redacted_shows_identity_paths_and_endpoints_as_json(
    tmp_path: Path, tables: Tables, key: Path
) -> None:
    path = tmp_path / "tape.toml"
    path.write_text(render(tables))
    shown = redacted(load_settings(path, environ={}))

    assert shown["kalshi"] == {
        "env": "demo",
        "key_id": "key-1",
        "private_key_path": str(key),
        "rest_timeout_s": 10,
        "ws_ping_interval_s": 10,
        "ws_ping_timeout_s": 20,
        "rest_url": "https://external-api.demo.kalshi.co/trade-api/v2",
        "ws_url": "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
    }
    assert shown["recorder"]["data_dir"] == str(tmp_path / "data")
    assert tuple(shown["recorder"]["universe"]["groups"]) == ()
    assert json.loads(msgspec.json.encode(shown))["kalshi"] == shown["kalshi"]


def test_settings_built_in_code_are_validated_too() -> None:
    with pytest.raises(ValueError, match="fixed-point count"):
        UniverseSettings(min_volume_24h="lots")
    with pytest.raises(ValueError, match="'g' must set exactly one of series and category"):
        UniverseGroupSettings(name="g", events=1, markets_per_event=1)
    sports = UniverseGroupSettings(name="g", category="Sports", events=1, markets_per_event=1)
    with pytest.raises(ValueError, match="names must be unique"):
        UniverseSettings(groups=(sports, sports))
    with pytest.raises(ValueError, match="needs 17 connections"):
        RecorderSettings(data_dir=Path("data"), book_connections=15)
    with pytest.raises(ValueError, match="whole number of minutes"):
        RecorderSettings(data_dir=Path("data"), keyframe_interval_s=45)
    with pytest.raises(ValueError, match="tcp://host:port"):
        RecorderSettings(data_dir=Path("data"), bus_endpoint="tcp://no-port")


def test_the_serve_section_takes_overrides_and_its_origins_are_comma_separated(
    tmp_path: Path, tables: Tables
) -> None:
    settings = load_all(
        tmp_path,
        tables,
        {
            "TAPE_SERVE__ALLOWED_ORIGINS": "https://tape.example, http://localhost:5173",
            "TAPE_SERVE__LISTEN_PORT": "9090",
        },
    )
    assert settings.serve.allowed_origins == ("https://tape.example", "http://localhost:5173")
    assert settings.serve.listen_port == 9090
