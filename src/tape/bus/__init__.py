"""The event bus between the recorder and its live consumers (ADR 0008, ADR 0022).

``ports`` defines what a publisher and a subscriber promise; ``envelope`` numbers, routes, and
encodes each event; ``livebooks`` is the consumer rule that rebuilds books from a lossy stream
and periodic refresh images; ``sockets`` carries the bytes over ZeroMQ PUB/SUB. The bus is a
leaf adapter: it imports core modules and never the recorder or the exchange client
(docs/ARCHITECTURE.md 6).
"""

from tape.bus.envelope import (
    CONTROL_PREFIX,
    FIRST_BUS_SEQ,
    GAP_TOPIC,
    LIFECYCLE_TOPIC,
    MARKET_DATA_PREFIX,
    BusEnvelope,
    SequencedPublisher,
    decode_bus_envelope,
    encode_bus_envelope,
    topic_for,
)
from tape.bus.livebooks import (
    BOOK_FRESH,
    BOOK_STALE,
    BOOK_UNKNOWN,
    RESET_EPOCH,
    RESET_GAP,
    RESET_START,
    BookStatus,
    LiveBooks,
    LiveBooksStats,
    Observation,
    ResetReason,
    StatusChange,
)
from tape.bus.ports import Publisher, PublisherStats, Subscriber
from tape.bus.sockets import (
    DEFAULT_SEND_HWM,
    IPC_SCHEME,
    TCP_SCHEME,
    SubscriberStats,
    ZmqPublisher,
    ZmqSubscriber,
    check_endpoint,
)

__all__ = [
    "BOOK_FRESH",
    "BOOK_STALE",
    "BOOK_UNKNOWN",
    "CONTROL_PREFIX",
    "DEFAULT_SEND_HWM",
    "FIRST_BUS_SEQ",
    "GAP_TOPIC",
    "IPC_SCHEME",
    "LIFECYCLE_TOPIC",
    "MARKET_DATA_PREFIX",
    "RESET_EPOCH",
    "RESET_GAP",
    "RESET_START",
    "TCP_SCHEME",
    "BookStatus",
    "BusEnvelope",
    "LiveBooks",
    "LiveBooksStats",
    "Observation",
    "Publisher",
    "PublisherStats",
    "ResetReason",
    "SequencedPublisher",
    "StatusChange",
    "Subscriber",
    "SubscriberStats",
    "ZmqPublisher",
    "ZmqSubscriber",
    "check_endpoint",
    "decode_bus_envelope",
    "encode_bus_envelope",
    "topic_for",
]
