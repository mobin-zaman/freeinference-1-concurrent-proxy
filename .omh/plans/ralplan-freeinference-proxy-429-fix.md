# ralph plan — freeinference proxy 429 retry fix

plan: ralph
instance: fix-freeinference-proxy-429
goal: Add bounded upstream-429 retry+backoff to the serial proxy so transient upstream
rate-limits drain the queue instead of cascading 429s to every waiting client.

## Repo / sandbox

- Repo: `/home/hermes/work/freeinference-1-concurrent-proxy` (git, branch: TBD at execute),
  origin GitHub `mobin-zaman/freeinference-1-concurrent-proxy`.
- Source: `src/freeinference_proxy/serial_proxy.py`.
- Tests: `tests/test_proxy.py` (37 tests). Run with `uv run pytest -q`.
- Deployed copy must stay byte-identical:
  `cp src/freeinference_proxy/serial_proxy.py /home/hermes/.hermes/scripts/freeinference-serial-proxy.py`.
- Restart: `systemctl --user restart freeinference-proxy.service` (env in
  `/home/hermes/.hermes/freeinference-proxy.env`).

## Tasks

### Task 1 — implement upstream-429 retry with backoff (file scope: serial_proxy.py + tests)
TDD requested: write failing test first, then implement.

1. Add module constants next to GATE_LIMIT/ACQUIRE_TIMEOUT:
   - `UPSTREAM_429_RETRIES = 2`
   - `RETRY_AFTER_CAP_S = 5`
   - `RETRY_BASE_DELAY_S = 1.0`
   - small jitter 0–0.5s.
2. In the upstream call path (after `upstream = requests.request(...)`), if
   `upstream.status_code == 429`: loop up to `UPSTREAM_429_RETRIES` more attempts.
   - Between attempts: parse `Retry-After` (clamp to `RETRY_AFTER_CAP_S`), else
     `RETRY_BASE_DELAY_S + random jitter`; sleep **while holding the gate**.
   - A non-429 on a retry attempt propagates normally (breaks the loop, handled by
     the existing stream/content forward path).
   - Exhausted retries → forward the 429 (current behavior, unchanged).
3. Only the 429 retry loop changes. Non-429 statuses: zero behavior change.
4. Tests (TDD): assert (a) upstream 429 then success on retry → client gets 200;
   (b) upstream persistent 429 → client gets 429 after all retries; (c) a non-429
   (e.g. a 4xx) is forwarded without retry.

### Task 2 — verify full suite + deploy
- `uv run pytest -q` — all 37 existing + new tests GREEN.
- `cp src/freeinference_proxy/serial_proxy.py /home/hermes/.hermes/scripts/freeinference-serial-proxy.py`
- Confirm byte-identical (`diff`).
- Restart `freeinference-proxy.service`; verify PID change + `ss -ltn | grep 8788` listening.

## Acceptance (from spec)

- Transient upstream 429 retried; persistent 429 still surfaces to client.
- Non-429 responses unchanged.
- Gate held across backoff.
- Full suite green. Deployed copy identical + service restarted live.