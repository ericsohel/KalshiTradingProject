"""ZeroMQ PUB/SUB adapters for the bus ports (ADR 0008).

Responsibility: carry bus messages between processes on one host. :class:`ZmqPublisher` binds a
PUB socket and sends without blocking; :class:`ZmqSubscriber` connects an asyncio SUB socket,
filters by topic prefix, and yields ``(topic, payload)`` pairs; :func:`check_endpoint` refuses
an endpoint before anything binds it.

Loss. A PUB socket never blocks and never refuses a send. When one subscriber's queue is full
(the publisher's ``send_hwm``, the kernel buffer, and that subscriber's ``receive_hwm``),
ZeroMQ drops that subscriber's copy alone and tells no one. Other subscribers are unaffected,
which is the isolation ADR 0008 chose; the slow subscriber sees its loss as a ``bus_seq`` gap
(ADR 0022). A publisher's ``dropped`` therefore counts only sends the socket refused outright.

Binding. libzmq unlinks whatever file is at an ``ipc://`` path before binding it, so the
publisher looks first: a socket that refuses connections is left over from a process that died
and is removed; a socket that accepts one belongs to a live publisher, and anything that is not
a socket belongs to someone else, so both are refused and left alone.

Invariants: publishing never blocks and never raises; closing is idempotent and discards
unsent messages; a publisher removes nothing but a stale socket; no ZeroMQ exception escapes
either class except as :class:`tape.errors.BusError`.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import socket
import stat
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Final

import msgspec
import zmq
import zmq.asyncio

from tape.bus.ports import PublisherStats
from tape.errors import BusError

__all__ = [
    "DEFAULT_SEND_HWM",
    "IPC_SCHEME",
    "TCP_SCHEME",
    "SubscriberStats",
    "ZmqPublisher",
    "ZmqSubscriber",
    "check_endpoint",
]

IPC_SCHEME: Final = "ipc://"
TCP_SCHEME: Final = "tcp://"

DEFAULT_SEND_HWM: Final = 10_000
"""Messages a publisher queues for one subscriber by default before dropping its copies."""

_TCP_ENDPOINT: Final = re.compile(r"tcp://\S+:(?:[0-9]+|\*)")
_PROBE_TIMEOUT_S: Final = 1.0
"""How long probing an ipc socket for a live publisher may take; a local accept is immediate."""

_MESSAGE_FRAMES: Final = 2
"""A bus message is a topic frame and a payload frame."""


def check_endpoint(endpoint: str) -> None:
    """Check that an endpoint names an absolute ipc path this platform can bind, or a tcp address.

    Args:
        endpoint: For example ``ipc:///run/tape/bus.sock`` or ``tcp://127.0.0.1:5555``.

    Raises:
        ValueError: If the scheme is neither ``ipc://`` nor ``tcp://``, a tcp address has no
            port, an ipc path is relative, or it is longer than ZeroMQ allows here (103 bytes
            on macOS, 107 on Linux).
    """
    if endpoint.startswith(IPC_SCHEME):
        path = endpoint.removeprefix(IPC_SCHEME)
        if not path.startswith("/"):
            raise ValueError(f"bus endpoint {endpoint!r}: an ipc path must be absolute")
        size = len(os.fsencode(path))
        if size > zmq.IPC_PATH_MAX_LEN:
            raise ValueError(
                f"bus endpoint {endpoint!r}: the ipc path is {size} bytes, but this platform "
                f"allows at most {zmq.IPC_PATH_MAX_LEN}"
            )
        return
    if _TCP_ENDPOINT.fullmatch(endpoint):
        return
    raise ValueError(
        f"bus endpoint must be ipc:///absolute/path or tcp://host:port, got {endpoint!r}"
    )


def _check_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")


class SubscriberStats(msgspec.Struct, frozen=True, kw_only=True):
    """Counters of one subscriber since it was opened.

    Attributes:
        received: Messages yielded.
        malformed: Messages skipped because they did not have exactly two frames.
    """

    received: int
    malformed: int


class ZmqPublisher:
    """A ZeroMQ PUB socket bound to one endpoint; a :class:`tape.bus.ports.Publisher`.

    Not thread-safe: a ZeroMQ socket belongs to one thread. See the module docstring for how
    loss is counted and how an ipc path is claimed.

    Args:
        endpoint: Where to bind, checked by :func:`check_endpoint`.
        send_hwm: Most messages queued for one subscriber before its further copies are dropped.
        context: Context to open the socket in; by default the publisher opens its own and
            terminates it on :meth:`close`.
        logger: Destination for logs; defaults to this module's logger.

    Raises:
        ValueError: If the endpoint is malformed or ``send_hwm`` is not positive.
        BusError: If the ipc path holds something other than a stale socket, a live publisher
            holds it, or the bind fails.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        send_hwm: int,
        context: zmq.Context[zmq.Socket[bytes]] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        check_endpoint(endpoint)
        _check_positive("send_hwm", send_hwm)
        if endpoint.startswith(IPC_SCHEME):
            _remove_stale_socket(Path(endpoint.removeprefix(IPC_SCHEME)))
        self._endpoint = endpoint
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._owns_context = context is None
        self._context: zmq.Context[zmq.Socket[bytes]] = (
            zmq.Context() if context is None else context
        )
        self._socket = self._context.socket(zmq.PUB)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.setsockopt(zmq.SNDHWM, send_hwm)
        self._closed = False
        self._sent = 0
        self._dropped = 0
        self._errors = 0
        try:
            self._socket.bind(endpoint)
        except zmq.ZMQError as exc:
            self.close()
            raise BusError(f"cannot bind the bus to {endpoint}: {exc}") from exc

    @property
    def endpoint(self) -> str:
        """The endpoint the socket is bound to."""
        return self._endpoint

    @property
    def stats(self) -> PublisherStats:
        """Counters since the socket was bound; see :class:`tape.bus.ports.PublisherStats`."""
        return PublisherStats(sent=self._sent, dropped=self._dropped, errors=self._errors)

    def publish(self, topic: bytes, payload: bytes) -> None:
        """Send one two-frame message without blocking, or count why it was not sent.

        Args:
            topic: The topic frame.
            payload: The payload frame.
        """
        if self._closed:
            self._errors += 1
            return
        try:
            self._socket.send_multipart((topic, payload), flags=zmq.NOBLOCK)
        except zmq.Again:
            self._dropped += 1
        except zmq.ZMQError as exc:
            self._errors += 1
            if self._errors == 1:
                self._log.error(
                    "bus send failed; later failures are only counted",
                    extra={"endpoint": self._endpoint, "error": repr(exc)},
                )
        else:
            self._sent += 1

    def close(self) -> None:
        """Close the socket at once, discarding unsent messages, and the context if owned.

        Idempotent. Later sends are counted as errors.
        """
        if self._closed:
            return
        self._closed = True
        self._socket.close(linger=0)
        if self._owns_context:
            self._context.term()


class ZmqSubscriber:
    """An asyncio ZeroMQ SUB socket connected to one endpoint; a :class:`tape.bus.ports.Subscriber`.

    Connecting succeeds before the publisher exists: ZeroMQ connects when it appears and again
    after it restarts. Subscribe to at least one prefix, ``b""`` for everything, before
    anything arrives. Used on one event loop.

    Args:
        endpoint: The publisher's endpoint, checked by :func:`check_endpoint`.
        receive_hwm: Most messages queued here before ZeroMQ drops this subscriber's copies.
        context: Asyncio context to open the socket in; by default the subscriber opens its own
            and terminates it on :meth:`close`.

    Raises:
        ValueError: If the endpoint is malformed or ``receive_hwm`` is not positive.
        BusError: If the socket cannot connect.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        receive_hwm: int,
        context: zmq.asyncio.Context | None = None,
    ) -> None:
        check_endpoint(endpoint)
        _check_positive("receive_hwm", receive_hwm)
        self._endpoint = endpoint
        self._owns_context = context is None
        self._context = zmq.asyncio.Context() if context is None else context
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.setsockopt(zmq.RCVHWM, receive_hwm)
        self._closed = False
        self._received = 0
        self._malformed = 0
        try:
            self._socket.connect(endpoint)
        except zmq.ZMQError as exc:
            self.close()
            raise BusError(f"cannot connect to the bus at {endpoint}: {exc}") from exc

    @property
    def stats(self) -> SubscriberStats:
        """Counters since the socket was opened; see :class:`SubscriberStats`."""
        return SubscriberStats(received=self._received, malformed=self._malformed)

    def subscribe(self, topic_prefix: bytes) -> None:
        """Receive every message whose topic starts with ``topic_prefix``.

        Args:
            topic_prefix: For example ``b"md.KXA-1"``; ``b""`` subscribes to everything, which a
                consumer following ``bus_seq`` needs (see :mod:`tape.bus.envelope`).

        Raises:
            BusError: If the subscriber is closed.
        """
        try:
            self._socket.setsockopt(zmq.SUBSCRIBE, topic_prefix)
        except zmq.ZMQError as exc:
            raise BusError(f"cannot subscribe to {topic_prefix!r}: {exc}") from exc

    async def messages(self) -> AsyncIterator[tuple[bytes, bytes]]:
        """Yield ``(topic, payload)`` in arrival order until :meth:`close`.

        A message without exactly two frames is skipped and counted; publishers built on this
        module never send one.

        Raises:
            BusError: If receiving fails while the subscriber is open.
        """
        while True:
            try:
                frames = await self._socket.recv_multipart()
            except asyncio.CancelledError:
                # close() cancels a receive in flight; that ends the stream, anything else is a
                # cancellation of the caller and must propagate.
                if self._closed:
                    return
                raise
            except zmq.ZMQError as exc:
                if self._closed:
                    return
                raise BusError(f"cannot receive from the bus at {self._endpoint}: {exc}") from exc
            if len(frames) != _MESSAGE_FRAMES:
                self._malformed += 1
                continue
            self._received += 1
            yield frames[0], frames[1]

    def close(self) -> None:
        """Close the socket, ending :meth:`messages`, and the context if owned. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._socket.close(linger=0)
        if self._owns_context:
            self._context.term()


def _remove_stale_socket(path: Path) -> None:
    """Clear an ipc path for binding, removing only a socket no process listens on.

    Raises:
        BusError: If the path holds something other than a socket, a live process listens on
            it, or it cannot be inspected or removed.
    """
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    except OSError as exc:
        raise BusError(f"cannot inspect bus socket path {path}: {exc.strerror}") from exc
    if not stat.S_ISSOCK(mode):
        raise BusError(f"bus socket path {path} exists and is not a socket; not replacing it")
    if _accepts_connections(path):
        raise BusError(f"bus socket path {path} is in use by a running publisher")
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        raise BusError(f"cannot remove stale bus socket {path}: {exc.strerror}") from exc


def _accepts_connections(path: Path) -> bool:
    """Whether a process is listening on a Unix socket.

    Raises:
        BusError: If connecting fails for a reason that does not settle the question.
    """
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        probe.settimeout(_PROBE_TIMEOUT_S)
        try:
            probe.connect(str(path))
        except (ConnectionRefusedError, FileNotFoundError):
            return False
        except OSError as exc:
            raise BusError(f"cannot tell whether bus socket {path} is in use: {exc}") from exc
    return True
