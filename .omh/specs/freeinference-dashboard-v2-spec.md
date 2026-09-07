---
name: freeinference-dashboard-v2
status: confirmed
date: 2026-09-08
---

# Spec: token counts, time filters, theme toggle, and animation pass for the proxy dashboard

## Why

The proxy already records one row per request to SQLite and serves a real-time
dashboard. Two gaps worth closing: it does not capture token usage (the whole
point of an LLM proxy is knowing how much you used), and the dashboard can't
segment traffic by time or switch to a light theme. While touching the UI,
polish the feel per the emil design-engineering playbook.

## Scope

### Backend (serial_proxy.py)

1. Schema: add `input_tokens` and `output_tokens` INTEGER columns to the
   `requests` table, via a guarded `ALTER TABLE ... ADD COLUMN` so existing
   databases (already populated) migrate in place. Default 0.
2. Token capture, from OpenAI-compatible responses:
   - Non-streaming: parse the buffered JSON body for `usage.prompt_tokens`
     and `usage.completion_tokens`.
   - Streaming (SSE): scan the relayed bytes (bounded tail buffer) for a
     `"usage": {...}` object in the final chunk before `[DONE]`, and record it.
   - Reuse the existing single insert; tokens pass through `record_request`.
3. API `/__api/requests` gains a `range` query param (`today` | `7d` | `all`,
   default `all`) that filters rows by `at` (epoch seconds) cutoff and returns
   the filtered rows plus aggregate `input_tokens` / `output_tokens` sums.
   Totals/errors/avg are computed over the filtered window, not the whole table.

### Frontend (dashboard.html)

1. Token visibility: two stat cards (Input tokens, Output tokens) and two
   columns (or a compact combined readout) in both the desktop table and the
   mobile card layout.
2. Time filter: segmented control with Today / 7 days / All time. Selecting a
   range re-fetches and re-aggregates stats and the table.
3. Theme: light/dark toggle in the header. Persist choice in localStorage;
   default follows `prefers-color-scheme`. Both palettes defined as CSS
   variable sets on `:root` / `.light`.
4. Animation pass (emil playbook):
   - Segmented filter switch with a sliding active-pill (clip-path or transform).
   - Theme toggle morph (icon) with a 200ms ease-out transform.
   - Token stat cards flash on change (reuse existing flash pattern).
   - Keep start-from-scale(≥0.9), ease-out, <300ms, and gate everything behind
     `prefers-reduced-motion`. Do not animate the live dot (100+ views/day).

### Tests

- Extend `tests/test_proxy.py`: token capture (non-streaming `usage` recorded;
  streaming `usage` recorded) and `range` filtering (today/7d/all counts).
- Keep existing assertions green (existing rows default tokens to 0).

## Non-goals

- No tokenization of inbound messages; rely on the authoritative `usage`
  object returned by the model.
- No chart/graph library added; keep it a single self-contained HTML file with
  no build step.
- No multi-account or billing integration.