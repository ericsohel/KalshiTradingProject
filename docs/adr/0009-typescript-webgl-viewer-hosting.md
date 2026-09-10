# 0009. TypeScript + WebGL2 viewer on Cloudflare Pages; API behind Caddy

Status: accepted. Date: 2026-09-09.

## Context

The owner chose the most impressive viewer option: a GPU-rendered liquidity heatmap
that scrubs like a video and streams live. Python plotting stacks cannot deliver that
interactivity in a browser. Hosting must stay within a $20 budget and must not
endanger the recorder host.

## Decision

The viewer is a Vite + TypeScript (strict) application with React for chrome and a
framework-free WebGL2 renderer, deployed to Cloudflare Pages (free) on every push to
`main`. The API is a FastAPI service on the recorder host, bound to localhost, exposed
only through Caddy with automatic TLS on a free dynamic-DNS hostname (or a purchased
domain). Types for API responses are generated from the API's OpenAPI document.

## Consequences

A second language in the repository, with its own lint, type, and test gates. The
recorder host exposes one port. The heatmap's performance budget (60 fps, 20,000
bubbles) becomes a tested requirement.
