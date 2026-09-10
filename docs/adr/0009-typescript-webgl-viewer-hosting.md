# 0009. TypeScript + WebGL2 viewer on Cloudflare Pages; API behind Caddy

Status: accepted; framework amended by ADR 0023 (Starlette on msgspec, not FastAPI). Date: 2026-09-09.

## Context

The owner chose the most impressive viewer: a GPU-rendered liquidity heatmap that
scrubs like a video and streams live to the public. Hosting must fit a $20 budget and
must not endanger the recorder host.

## Alternatives considered

1. **Python Panel/Bokeh/datashader.** One language, server-rendered. Rejected: each
   viewer costs server CPU on the recorder host; interactivity is far from 60 fps;
   scrubbing re-renders on the server.
2. **Grafana.** Excellent for the status metrics. Rejected as the main viewer: no
   depth heatmap with scrubbing and trade overlays; kept as an optional ops dashboard.
3. **Static site served by the API host.** One origin, simplest CORS. Rejected: couples
   public page load to the recorder host; a traffic spike would compete with capture.
4. **Cloudflare Tunnel** for ingress. Clean, no open port. Rejected: named tunnels need
   a Cloudflare-managed domain, which costs money the budget does not have; Caddy with
   automatic TLS on a free dynamic-DNS hostname achieves the same with one open port.

## Decision

The viewer is a Vite + TypeScript (strict) application with React for chrome and a
framework-free WebGL2 renderer, deployed to Cloudflare Pages on every push to `main`.
The API is a FastAPI service bound to localhost on the recorder host, exposed only
through Caddy. Response types are generated from the API's OpenAPI document.

## Consequences

A second language with its own lint, type, and test gates. The recorder host exposes
one port. The rendering budget (60 fps, 20,000 bubbles) is a tested requirement, not
an aspiration.

## What would reverse it

If the audience were only the owner, Python plotting would do. If the budget grew, a
Cloudflare-managed domain and Tunnel would replace Caddy and the open port.
