"""Adapters that talk to Kalshi: request signing, rate limiting, REST, and WebSocket.

This package is the only place the exchange is contacted. It performs no decoding
beyond what the transport requires and holds no domain logic (ADR 0003, ADR 0004).
"""

from tape.client.auth import RsaPssSigner, Signer
from tape.client.ratelimit import (
    Bucket,
    BucketLimits,
    BucketRateLimiter,
    NullRateLimiter,
    RateLimiter,
    TokenBucket,
)
from tape.client.ws import (
    Command,
    ListSubscriptionsCommand,
    RawFrame,
    SubscribeCommand,
    UnsubscribeCommand,
    UpdateSubscriptionCommand,
    WsSession,
    encode_command,
)

__all__ = [
    "Bucket",
    "BucketLimits",
    "BucketRateLimiter",
    "Command",
    "ListSubscriptionsCommand",
    "NullRateLimiter",
    "RateLimiter",
    "RawFrame",
    "RsaPssSigner",
    "Signer",
    "SubscribeCommand",
    "TokenBucket",
    "UnsubscribeCommand",
    "UpdateSubscriptionCommand",
    "WsSession",
    "encode_command",
]
