"""The command line: ``config check`` output and exit codes, logging, and recorder wiring."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from tape.cli import (
    AuditTask,
    JsonLogFormatter,
    TextLogFormatter,
    build_recorder,
    configure_logging,
    main,
    recorder_config,
)
from tape.config import ENDPOINTS, load_settings
from tape.errors import ConfigError
from tape.timeutil import FrozenClock


def write_settings(tmp_path: Path, *, group_size: int = 500, pem: bytes | None = None) -> Path:
    key = tmp_path / "read.pem"
    if pem is None:
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    key.write_bytes(pem)
    key.chmod(0o600)
    config = tmp_path / "tape.toml"
    config.write_text(
        f'[kalshi]\nenv = "demo"\nkey_id = "key-1"\nprivate_key_path = "{key}"\n\n'
        f'[recorder]\ndata_dir = "data"\ngroup_size = {group_size}\nbook_connections = 3\n'
    )
    return config


@pytest.fixture
def root_logger() -> Iterator[logging.Logger]:
    """Restore the root logger that ``configure_logging`` replaces."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield root
    for handler in list(root.handlers):
        root.removeHandler(handler)
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(level)


def test_config_check_prints_the_effective_settings_and_exits_0(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = write_settings(tmp_path)
    assert main(["config", "check", "--config", str(config)]) == 0
    captured = capsys.readouterr()
    shown = json.loads(captured.out)
    assert shown["kalshi"]["env"] == "demo"
    assert shown["kalshi"]["ws_url"] == ENDPOINTS["demo"].ws_url
    assert shown["kalshi"]["private_key_path"] == str(tmp_path / "read.pem")
    assert shown["recorder"]["data_dir"] == str(tmp_path / "data")
    assert captured.err == ""


def test_config_check_names_the_invalid_setting_and_exits_1(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = write_settings(tmp_path, group_size=501)
    assert main(["config", "check", "--config", str(config)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("tape: configuration error: ")
    assert "$.recorder.group_size" in captured.err


def test_config_check_reports_a_missing_file_and_exits_1(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["config", "check", "--config", str(tmp_path / "absent.toml")]) == 1
    assert "cannot read configuration" in capsys.readouterr().err


def test_record_refuses_an_invalid_configuration_before_connecting(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], root_logger: logging.Logger
) -> None:
    config = write_settings(tmp_path, group_size=0)
    assert main(["record", "--config", str(config), "--log-format", "json"]) == 1
    assert "$.recorder.group_size" in capsys.readouterr().err
    assert isinstance(root_logger.handlers[0].formatter, JsonLogFormatter)


@pytest.mark.parametrize("argv", [[], ["config"], ["record"], ["config", "check"]])
def test_an_incomplete_command_is_a_usage_error(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        main(argv)
    assert raised.value.code == 2


def test_configure_logging_installs_one_handler_in_the_chosen_format(
    root_logger: logging.Logger,
) -> None:
    configure_logging(log_format="text", level="WARNING")
    (handler,) = root_logger.handlers
    assert isinstance(handler.formatter, TextLogFormatter)
    assert root_logger.level == logging.WARNING
    with pytest.raises(ValueError, match="log_format"):
        configure_logging(log_format="yaml", level="INFO")


def make_record(**extra: object) -> logging.LogRecord:
    try:
        raise ValueError("boom")
    except ValueError:
        exc_info = sys.exc_info()
    return logging.getLogger("tape.test").makeRecord(
        "tape.test", logging.WARNING, __file__, 1, "gap on %s", ("sid 3",), exc_info, extra=extra
    )


def test_json_log_lines_carry_structured_fields_and_the_traceback() -> None:
    line = JsonLogFormatter().format(make_record(conn_id=2, odd=object()))
    entry = json.loads(line)
    assert (entry["level"], entry["logger"], entry["message"]) == (
        "WARNING",
        "tape.test",
        "gap on sid 3",
    )
    assert entry["conn_id"] == 2
    assert entry["odd"].startswith("<object object")
    assert entry["ts"].endswith("+00:00")
    assert "ValueError: boom" in entry["exception"]
    assert "\n" not in line


def test_text_log_lines_append_structured_fields() -> None:
    text = TextLogFormatter().format(make_record(conn_id=2))
    first_line, *traceback = text.splitlines()
    assert first_line.endswith("WARNING tape.test: gap on sid 3 conn_id=2")
    assert traceback[-1] == "ValueError: boom"


async def test_build_recorder_wires_the_environment_and_the_connection_layout(
    tmp_path: Path,
) -> None:
    settings = load_settings(write_settings(tmp_path), environ={})
    async with httpx.AsyncClient() as http:
        recorder = build_recorder(settings, http=http, clock=FrozenClock(), host="box")
        assert recorder.config == recorder_config(settings, host="box")
        assert recorder.config.ws_url == ENDPOINTS["demo"].ws_url
        assert recorder.config.universe == settings.recorder.universe.policy()
        supervisors = recorder.supervisors
        assert sorted(supervisors) == [0, 1, 2, 3, 4]
        assert (supervisors[0].config.persist, supervisors[0].config.firehose_channels) == (
            False,
            ("ticker",),
        )
        assert (supervisors[1].config.persist, supervisors[1].config.firehose_channels) == (
            True,
            ("market_lifecycle_v2",),
        )
        for conn_id in (2, 3, 4):
            config = supervisors[conn_id].config
            assert config.book_channels == ("orderbook_delta", "trade")
            assert config.use_yes_price
        # The auditor is wired in, on the configured interval.
        (task,) = recorder.periodic_tasks
        assert isinstance(task, AuditTask)
        assert task.interval_s == settings.recorder.audit_interval_s
        await recorder.stop()


async def test_build_recorder_refuses_a_key_that_is_not_rsa(tmp_path: Path) -> None:
    settings = load_settings(write_settings(tmp_path, pem=b"not a key"), environ={})
    async with httpx.AsyncClient() as http:
        with pytest.raises(ConfigError, match="not a usable PEM private key"):
            build_recorder(settings, http=http, clock=FrozenClock(), host="box")
