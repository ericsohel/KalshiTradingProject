"""Request signing: correct RSA-PSS signature, headers, and configuration errors."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa

from tape.client.auth import HEADER_KEY, HEADER_SIGNATURE, HEADER_TIMESTAMP, RsaPssSigner, Signer
from tape.errors import ConfigError


@pytest.fixture(scope="module")
def rsa_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def key_path(tmp_path: Path, rsa_key: rsa.RSAPrivateKey) -> Path:
    path = tmp_path / "key.pem"
    path.write_bytes(
        rsa_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return path


def test_signature_verifies_against_the_public_key(
    key_path: Path, rsa_key: rsa.RSAPrivateKey
) -> None:
    signer = RsaPssSigner("kid-1", key_path)
    signature = signer.sign(1_700_000_000_000, "GET", "/trade-api/v2/portfolio/balance")
    rsa_key.public_key().verify(
        base64.b64decode(signature),
        b"1700000000000GET/trade-api/v2/portfolio/balance",
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256.digest_size),
        hashes.SHA256(),
    )


def test_signature_is_salted_so_two_signatures_differ(key_path: Path) -> None:
    signer = RsaPssSigner("kid-1", key_path)
    first = signer.sign(1, "GET", "/trade-api/v2/markets")
    second = signer.sign(1, "GET", "/trade-api/v2/markets")
    assert first != second  # PSS is randomized; both must still verify


def test_headers_carry_key_timestamp_and_signature(key_path: Path) -> None:
    signer = RsaPssSigner("kid-1", key_path)
    headers = signer.headers("POST", "/trade-api/v2/portfolio/events/orders", now_ms=42)
    assert headers[HEADER_KEY] == "kid-1"
    assert headers[HEADER_TIMESTAMP] == "42"
    assert base64.b64decode(headers[HEADER_SIGNATURE])
    assert set(headers) == {HEADER_KEY, HEADER_TIMESTAMP, HEADER_SIGNATURE}


def test_websocket_handshake_path_is_signable(key_path: Path, rsa_key: rsa.RSAPrivateKey) -> None:
    signer = RsaPssSigner("kid-1", key_path)
    signature = signer.sign(7, "GET", "/trade-api/ws/v2")
    rsa_key.public_key().verify(
        base64.b64decode(signature),
        b"7GET/trade-api/ws/v2",
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256.digest_size),
        hashes.SHA256(),
    )


def test_relative_path_is_rejected(key_path: Path) -> None:
    signer = RsaPssSigner("kid-1", key_path)
    with pytest.raises(ValueError, match="must start with"):
        signer.sign(1, "GET", "trade-api/v2/markets")


def test_query_string_is_the_callers_responsibility(key_path: Path) -> None:
    # The signer signs exactly what it is given; stripping the query string happens in
    # the REST client, so a path with one still signs (and would be rejected by Kalshi).
    signer = RsaPssSigner("kid-1", key_path)
    assert signer.sign(1, "GET", "/trade-api/v2/markets?limit=1")


def test_missing_file_is_a_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read"):
        RsaPssSigner("kid-1", tmp_path / "absent.pem")


def test_malformed_pem_is_a_config_error(tmp_path: Path) -> None:
    path = tmp_path / "bad.pem"
    path.write_bytes(b"not a pem")
    with pytest.raises(ConfigError, match="usable PEM"):
        RsaPssSigner("kid-1", path)


def test_non_rsa_key_is_a_config_error(tmp_path: Path) -> None:
    path = tmp_path / "ed25519.pem"
    path.write_bytes(
        ed25519.Ed25519PrivateKey.generate().private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    with pytest.raises(ConfigError, match="not an RSA"):
        RsaPssSigner("kid-1", path)


def test_empty_key_id_is_a_config_error(key_path: Path) -> None:
    with pytest.raises(ConfigError, match="key_id"):
        RsaPssSigner("", key_path)


def test_repr_redacts_the_key(key_path: Path) -> None:
    text = repr(RsaPssSigner("kid-1", key_path))
    assert "redacted" in text
    assert "BEGIN" not in text


def test_signer_satisfies_the_protocol(key_path: Path) -> None:
    signer: Signer = RsaPssSigner("kid-1", key_path)
    assert isinstance(signer, Signer)
    assert signer.key_id == "kid-1"
