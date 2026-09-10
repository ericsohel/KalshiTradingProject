# 0023. The live API: Starlette on msgspec, recorder-published catalog, lazy public metadata

Status: accepted. Date: 2026-09-10. Amends ADR 0009 (API framework).

## Context

`tape serve` is the only process the public reaches. Its first milestone is the live
path: a market list, one market's current book, recorder health, and a WebSocket feed
of books, trades, and ticker updates. Three questions had to be settled first.

- **Framework.** ADR 0009 named FastAPI so that front-end types could be generated from
  its OpenAPI document. Every struct in this project is msgspec; FastAPI's schemas and
  validation come from pydantic, which the engineering standards keep off hot paths, and
  OpenAPI does not describe WebSocket messages, which are most of the contract.
- **What the recorder must tell the API.** Only the recorder knows which markets are
  recorded, which are showcase markets, and how healthy capture is. Its status is
  currently only logged.
- **Titles and price grids.** A viewer needs "Highest temperature in NYC today?" and
  "84° to 85°", not a ticker, and the heatmap needs each market's tick grid. Kalshi's
  market listing carries neither an event title nor a series category; they live on
  events and series, which Kalshi serves without authentication.

## Alternatives considered

1. **FastAPI.** The most familiar choice. Rejected: a second, pydantic model layer
   mirroring the msgspec structs, and a generated contract that still omits the
   WebSocket messages.
2. **Litestar.** Native msgspec support and OpenAPI generation. Rejected, narrowly: a
   larger framework than six routes need, and its OpenAPI output would still cover only
   the REST half, leaving two sources of front-end types.
3. **The recorder fetches event and series metadata** and publishes titles with the
   catalog. One place talks to Kalshi. Rejected: presentation metadata for 2,000 markets
   is REST traffic and failure surface in the process the project treats as sacred, for
   data only the viewer uses.
4. **The API reads recorder state from files** (a universe or status JSON the recorder
   rewrites). Rejected for the reason ADR 0008 rejected tailing segments: consumers
   coupled to storage layout, with a second recovery story beside the bus's.

## Decision

- **Framework.** `tape.api` is a Starlette application served by uvicorn, bound to
  localhost. msgspec structs are the only model layer: request parsing, response
  encoding, and WebSocket messages all use them. One script emits a JSON Schema
  document for every API type, REST and WebSocket alike, from those structs, and the
  front end's TypeScript types are generated from that document; CI fails when the
  committed schema drifts from the code.
- **Catalog and status on the bus.** The recorder publishes `ctl.catalog`, the markets
  it records with the fields it already holds (series, event, 24-hour volume, close
  time, showcase flag), once per bus refresh cycle, and `ctl.status`, its status, every
  status interval. Both follow ADR 0022's rule: a consumer that just started has them
  within one interval.
- **Lazy public metadata.** The API resolves titles, categories, and price grids from
  Kalshi's public event and series endpoints, without credentials, only for markets a
  response or a subscription actually needs, deduplicated per event, cached with a TTL,
  and paced by its own token bucket. Until a market resolves, responses carry nulls and
  the viewer shows the ticker. A metadata failure never affects live data.

## Consequences

The API holds no Kalshi credentials and makes a handful of public requests per hour in
steady state. Front-end types have a single source that covers the WebSocket feed. The
recorder gains two small bus topics and needs one more restart to publish them. The
framework provides less than FastAPI or Litestar (no automatic docs page, no built-in
rate limiting), so CORS, origin checks, and connection limits are explicit code with
tests. Historical routes (`/book?at=`, `/tape`, `/status/days`) wait for M4.

## What would reverse it

A route count large enough that hand-written request handling becomes a burden (move to
Litestar, keeping the msgspec structs), or Kalshi requiring authentication for event
metadata (move resolution into a small credentialed sidecar, still outside the recorder).
