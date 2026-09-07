---
name: freeinference-1cc-proxy-tests
status: confirmed
date: 2026-09-08
---

# Spec: unit tests for the freeinference 1-concurrent proxy

## Goal

Add a proper test suite that locks down the proxy's behavior so publishes and
future changes don't silently break the concurrency guarantee.

## What the tests must prove

- Local read-only endpoints (`/__dashboard`, `/__api/requests`, favicon) serve
  locally and never reach the upstream.
- The `/__api/events` SSR endpoint streams (handshake flushed immediately).
- Requests forward to the upstream verbatim: method, path, JSON body, and
  quietly-dropped hop-by-hop headers, with the response relayed back.
- Streaming (text/event-stream) responses relay chunked to the client.
- The serialization gate: N parallel clients never let more than 1 request be
  upstream at once (the core guarantee).
- Queue wait is recorded (a request behind another gets `waited_s > 0`).
- A caller that waits past `ACQUIRE_TIMEOUT` gets a local `429` with
  `Retry-After: 5` and never reaches upstream.
- An unreachable upstream yields a local `502 local_proxy_upstream_error`.
- Every proxied request is written to SQLite history with status/user-agent.

## Approach

- Run a fake upstream in-process (`ThreadingHTTPServer`) that can hold
  requests open, record what it receives, track peak in-flight concurrency,
  and fail on demand. Point the real proxy at it via module-global override.
- Each test builds a fresh proxy on an ephemeral port with an isolated `tmp_path`
  data dir, via pytest fixtures (`monkeypatch` + `tmp_path`).
- Runner: `uv run pytest` (allowlisted in omh config). No network, no
  FreeInference account needed.
- Add `[dependency-groups] dev = ["pytest"]` and a `[tool.pytest.ini_options]`
  with `pythonpath = ["src"]`; commit `uv.lock` for reproducibility.

## Non-goals

- No mocking of the proxy's HTTP layer: tests exercise the real server and
  real `requests` client against a real in-process upstream, so the
  serialization behaviour is actually exercised.
- No dependency on the live FreeInference service.