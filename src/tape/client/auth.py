"""Kalshi request signing.

Every authenticated request carries three headers. The signature is RSA-PSS over
``timestamp + METHOD + path`` where ``path`` starts at ``/trade-api/v2`` and excludes the
query string (docs/DATA_FORMATS.md 2.1). The same scheme signs the WebSocket upgrade,
with method ``GET`` and path ``/trade-api/ws/v2``.

The private key is loaded once from a file and never exposed, logged, or serialized.
"""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Protocol, runtime_checkable

from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from tape.errors import ConfigError

__all__ = ["HEADER_KEY", "HEADER_SIGNATURE", "HEADER_TIMESTAMP", "RsaPssSigner", "Signer"]

HEADER_KEY = "KALSHI-ACCESS-KEY"
HEADER_TIMESTAMP = "KALSHI-ACCESS-TIMESTAMP"
HEADER_SIGNATURE = "KALSHI-ACCESS-SIGNATURE"


@runtime_checkable
class Signer(Protocol):
    """Produces the authentication headers for a request."""

    @property
    def key_id(self) -> str:
        """Kalshi API key id sent in ``KALSHI-ACCESS-KEY``."""
        ...

    def sign(self, timestamp_ms: int, method: str, path: str) -> str:
        """Return the base64 signature over ``timestamp_ms + method + path``."""
        ...

    def headers(self, method: str, path: str, *, now_ms: int) -> dict[str, str]:
        """Return the three authentication headers for one request."""
        ...


class RsaPssSigner:
    """Signs with an RSA private key loaded from a PEM file.

    Args:
        key_id: The API key id shown when the key was created.
        private_key_path: Path to the PEM file Kalshi provided. It is read once, at
            construction, and the key material is never stored anywhere else.
        password: Passphrase, if the PEM is encrypted.

    Raises:
        ConfigError: If the file is missing, unreadable, not a private key, or not RSA.
    """

    __slots__ = ("_key", "_key_id")

    def __init__(
        self, key_id: str, private_key_path: Path, *, password: bytes | None = None
    ) -> None:
        if not key_id:
            raise ConfigError("key_id must not be empty")
        try:
            pem = private_key_path.read_bytes()
        except OSError as exc:
            raise ConfigError(f"cannot read private key at {private_key_path}: {exc}") from exc
        try:
            key = serialization.load_pem_private_key(pem, password=password)
        except (ValueError, TypeError, UnsupportedAlgorithm) as exc:
            raise ConfigError(f"{private_key_path} is not a usable PEM private key") from exc
        if not isinstance(key, rsa.RSAPrivateKey):
            raise ConfigError(f"{private_key_path} is not an RSA private key")
        self._key = key
        self._key_id = key_id

    @property
    def key_id(self) -> str:
        """Kalshi API key id sent in ``KALSHI-ACCESS-KEY``."""
        return self._key_id

    def sign(self, timestamp_ms: int, method: str, path: str) -> str:
        """Sign ``timestamp_ms + method + path`` with RSA-PSS and return base64.

        Args:
            timestamp_ms: Current time in milliseconds; must match the header exactly.
            method: Upper-case HTTP method, for example ``"GET"``.
            path: Path from the root, without host or query string, for example
                ``"/trade-api/v2/portfolio/balance"``.

        Raises:
            ValueError: If ``path`` does not start with ``/``, which would silently
                produce a signature the exchange rejects.
        """
        if not path.startswith("/"):
            raise ValueError(f"path must start with '/': {path!r}")
        message = f"{timestamp_ms}{method}{path}".encode()
        signature = self._key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256.digest_size),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("ascii")

    def headers(self, method: str, path: str, *, now_ms: int) -> dict[str, str]:
        """Return ``KALSHI-ACCESS-KEY``, ``-TIMESTAMP``, and ``-SIGNATURE`` for one request."""
        return {
            HEADER_KEY: self._key_id,
            HEADER_TIMESTAMP: str(now_ms),
            HEADER_SIGNATURE: self.sign(now_ms, method, path),
        }

    def __repr__(self) -> str:
        """Redacted representation; the key material is never rendered."""
        return f"RsaPssSigner(key_id={self._key_id!r}, key=<redacted>)"
