---
name: freeinference-1-concurrent-proxy
status: confirmed
date: 2026-09-08
---

# Spec: publishable FreeInference 1-concurrent proxy

## Why this exists

FreeInference's free tier allows only 1-2 concurrent requests per account
(verified 2026-09-01: live HTTP 429 "Too many concurrent requests (limit: 1)"
from upstream). Hermes, the agent this user runs, fires requests in parallel:
a main turn plus auxiliary vision and compression sub-calls plus delegation
children. That parallel burst trips FreeInference's limit, and requests fail
with 429 instead of getting an answer.

The fix is a local serializing reverse proxy: it forces a global in-flight
cap of exactly 1 upstream request at a time and queues the rest. Callers never
see the upstream concurrency error because the second request simply waits
instead of failing. It sits only in front of the freeinference-provider
upstream; every other provider connects directly and is untouched.

Because a one-at-a-time queue hides queued work, the proxy also records every
request to SQLite and ships a self-contained real-time dashboard (SSE push) so
you can see what is waiting and for how long.

## Scope for the publishable repo

Core deliverable: `FreeInference 1-concurrent request proxy` (user-named).

1. `serial_proxy.py` — the reverse proxy with the global 1-in-flight gate,
   SQLite request log, and read-only local dashboard + JSON + SSE endpoints.
2. `dashboard.html` — self-contained dark-theme dashboard served at
   `/__dashboard`.
3. `tailscale_bridge.py` — optional stdlib reverse proxy to expose only the
   dashboard (+ its API fetch) on a Tailscale IP:port, keeping the raw proxy
   API localhost-only.

## Publishability requirements

- Remove all hardcoded `/home/hermes/.hermes/...` paths. Data dir (log, DB,
  dashboard) configurable via `--data-dir` / env, defaulting to
  `~/.local/share/freeinference-1-concurrent-proxy/`. Dashboard ships as a
  package resource read via `importlib.resources` (stdlib).
- Ports and upstream remain CLI/env-overridable; defaults unchanged
  (127.0.0.1:8788 → freeinference.org, gate limit 1).
- Add `requirements.txt` (`requests`), a `pyproject.toml` entry point, LICENSE,
  and README written through the unslop pass.
- Ship systemd unit examples (user units) under `deploy/`.
- No secrets or user-specific values in the repo (/home/hermes, Tailscale IP,
  tokens). They become documented defaults / env.

## Non-goals

- No TLS termination, auth, or multi-account support. The concurrency limit is
  per account and this single proxy is for one account.
- No queue length cap beyond the existing acquire-timeout 429. Existing 300s
  wait-before-429 behavior stays.
- Keep the runtime behavior byte-for-byte compatible with the live proxy
  except for the configurable paths.