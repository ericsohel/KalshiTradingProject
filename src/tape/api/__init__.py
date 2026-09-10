"""The live API that ``tape serve`` runs (ADR 0023, docs/FRONTEND.md 4).

``contract`` defines every REST body and WebSocket message; ``directory`` builds the REST views
from the recorder's catalog, status reports, and ticker updates; ``metadata`` resolves titles,
categories, and price grids from Kalshi's public endpoints; ``session`` serves one WebSocket;
``hub`` follows the bus and fans messages out to sessions; ``app`` puts it all on Starlette
routes. Only the composition root imports this package, and it never imports the recorder,
whose state reaches it only over the bus (docs/ARCHITECTURE.md 6).
"""

from tape.api.app import API_PREFIX, create_app
from tape.api.directory import UNRESOLVED, MarketDirectory, MarketMetadata, book_state
from tape.api.hub import EVERY_TOPIC, LiveHub, ServeConfig
from tape.api.metadata import MetadataResolver, ResolverStats, metadata_limits
from tape.api.session import ClientSession, LiveSocket, SessionFeed

__all__ = [
    "API_PREFIX",
    "EVERY_TOPIC",
    "UNRESOLVED",
    "ClientSession",
    "LiveHub",
    "LiveSocket",
    "MarketDirectory",
    "MarketMetadata",
    "MetadataResolver",
    "ResolverStats",
    "ServeConfig",
    "SessionFeed",
    "book_state",
    "create_app",
    "metadata_limits",
]
