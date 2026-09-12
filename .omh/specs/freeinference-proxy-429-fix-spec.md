# Fix freeinference proxy 429 cascade

status: confirmed
date: 2026-09-12
instance: fix-freeinference-proxy-429

## Problem

Users of the freeinference 1-concurrent serializing proxy keep getting HTTP 429.

The proxy serializes all requests through a single global gate (`GATE_LIMIT = 1`)
to respect freeinference.org's 1-2 concurrent-request limit. When the single slot
is held by a long request and several clients queue up behind it, upstream
(freeinference.org) can momentarily hit its own rate/concurrency limit just as the
slot frees. The proxy then **forwards that upstream 429 to every queued client
unconditionally** — no retry, no backoff — producing a cascade of errors.

Evidence (proxy.log, 2026-09-12T15:21):
- One `/v1/models` request held the slot for `waited_s: 236.5`.
- Queued chat calls then got 429 with `input_tokens: 0, output_tokens: 0`
  and `waited_s` of 216 / 22 / 0 / 0 / 0 — these are **upstream 429s forwarded
  after queueing**, NOT the local gate timeout (local timeout fires only at
  `ACQUIRE_TIMEOUT=300` and logs `waited_s: 300`).
- A burst of 4+ 429s landed within ~3 seconds — the classic cascade.

## Root cause

`serial_proxy.py` acquires the gate, calls upstream, and on `upstream.status_code == 429`
simply forwards the error and returns (drops the gate). No retry-with-backoff, so a
transient upstream rate-limit aborts every waiting client instead of draining the queue.

## Fix

Add a bounded retry-with-backoff for **upstream 429s only**, **while holding the gate**:

1. When the upstream response is `429`, do NOT immediately return. Instead, retry
   the same request up to `UPSTREAM_429_RETRIES` (default 2) times.
2. Between retries, sleep a short backoff: honor upstream's `Retry-After` header if
   present and sane (<= a cap, default 5s), otherwise a small fixed/jittered delay
   (default 1.0s + jitter).
3. While retrying, keep the gate held so no other request collides with the upstream
   slot during the backoff window.
4. If all retries still return 429, forward the 429 to the client (unchanged behavior
   as last resort).
5. Non-429 upstream statuses are forwarded unchanged (no behavior change).

Constants (module-level, next to the existing `GATE_LIMIT` / `ACQUIRE_TIMEOUT`):
- `UPSTREAM_429_RETRIES = 2`
- `RETRY_AFTER_CAP_S = 5`
- `RETRY_BASE_DELAY_S = 1.0`
- add a small random jitter (0–0.5s) to avoid thundering-herd re-sync.

## Acceptance criteria

- [ ] A queued-then-429 request is retried instead of immediately failing when the
      upstream 429 is transient.
- [ ] After all retries are exhausted (persistent upstream 429), the client still
      receives a 429 (deduped/no worse).
- [ ] Non-429 upstream responses (200, 401, 502, 4xx/5xx) are forwarded **unchanged** —
      zero behavior change outside the 429 path.
- [ ] The gate is held across the retry backoff (no concurrent upstream collision).
- [ ] Existing test suite (`tests/test_proxy.py`, 37 tests) still passes (GREEN).
- [ ] New unit test(s) added for the 429-retry path (assert retried-then-success and
      retried-then-fail).

## Out of scope

- Raising `GATE_LIMIT` (upstream hard limit is 1-2; keep 1).
- Changing the local queue-timeout semantics.
- Touching the dashboard or ngrok/tailscale bridges.

## Deploy

Repo: `/home/hermes/work/freeinference-1-concurrent-proxy` (git, origin = GitHub mobin-zaman).
Deployed copy: `/home/hermes/.hermes/scripts/freeinference-serial-proxy.py` (must stay
byte-identical to `src/freeinference_proxy/serial_proxy.py`).
Runtime: systemd user unit `freeinference-proxy.service`, env from
`/home/hermes/.hermes/freeinference-proxy.env`.