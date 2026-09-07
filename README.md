# FreeInference 1-concurrent request proxy

A local reverse proxy that caps FreeInference at one in-flight upstream request at a time and queues the rest, so parallel clients stop hitting the provider's per-account concurrency limit.

> **Not affiliated.** This is an independent, unofficial tool. It is not made by, endorsed by, or connected to FreeInference in any way. It is a small concurrency-gate utility I wrote so I can use FreeInference from Hermes without tripping the provider's concurrent-request limit. It only limits how many requests reach FreeInference at once; it does not change, wrap, or replace the FreeInference service itself. Use it at your own risk and respect FreeInference's terms of service.

> **Warning: account ban risk.** FreeInference's terms can terminate your account for circumventing or gaming its request limits. Two things in particular can get you banned:
>
> 1. **Changing the User-Agent.** Do not spoof or rewrite the User-Agent header your client sends to FreeInference (for example to hide the calling library). Send your real User-Agent. This tool forwards the header untouched for exactly that reason.
> 2. **Concurrency evasion.** Any tool whose purpose is to get more concurrent requests than FreeInference's free tier allows is a terms-of-service risk. This proxy exists so concurrent callers queue and stay inside the per-account limit; it does not raise that limit, and it must not be used to push past it.

> This project is provided as-is for legitimate, single-account use. You are responsible for how you use it and for complying with FreeInference's terms. If asked, it is not worth losing your account over.

## Features

- **Serializes requests.** A global semaphore holds upstream concurrency at exactly 1. Any request that arrives while one is in flight queues instead of failing.
- **OpenAI-compatible endpoint.** Point any OpenAI SDK client at `http://127.0.0.1:8788`; the proxy forwards to `freeinference.org` untouched.
- **Streaming passthrough.** Chat completions are relayed token-by-token with chunked encoding, so a long generation streams back live.
- **Real-time dashboard.** A single self-contained HTML page shows request activity over Server-Sent Events (no polling, no client build step), plus a JSON history API.
- **Local only by default.** Binds `127.0.0.1`; no auth or TLS because nothing is exposed to the network. An optional bridge exposes just the dashboard on another interface.
- **Honest queues.** Every request is written to SQLite with its queue-wait and duration, so you can see what waited and why.

## Why you'd use it

FreeInference's free tier allows 1 to 2 concurrent requests per account. An agent like Hermes fires a main request plus vision, compression, and delegation sub-requests in a burst, which trips that limit and returns `429`. Pointing the agent at this proxy instead of FreeInference directly collapses the burst into a serial queue: one request at a time upstream, the rest wait, and nothing fails with a concurrency error.

It sits only in front of the FreeInference provider. Every other provider connects directly and is untouched.

## Screenshot

![FreeInference proxy dashboard](docs/dashboard.png)

## Install

Requires Python 3.9+ and `requests`.

```bash
pip install -r requirements.txt
# or, from the source tree
pip install .
```

## Quick start

```bash
freeinference-serial-proxy
```

Defaults:

- listens on `127.0.0.1:8788`
- forwards to `https://freeinference.org`
- writes data to `~/.local/share/freeinference-1-concurrent-proxy/`

Now point an OpenAI-compatible client at `http://127.0.0.1:8788`. Set the API key the way you normally would for FreeInference; the proxy forwards it verbatim.

Open the dashboard in a browser:

```
http://127.0.0.1:8788/__dashboard
```

## How it works

The proxy is a `ThreadingHTTPServer`; each request runs in its own thread. The interesting part is the handoff between the caller's thread and the upstream's.

1. **Strip hop-by-hop headers.** `Connection`, `Transfer-Encoding`, `Content-Encoding`, and similar are removed; the rest are forwarded.
2. **Acquire the gate.** A `threading.BoundedSemaphore(GATE_LIMIT)` with `GATE_LIMIT = 1` blocks the thread. If it waits longer than `ACQUIRE_TIMEOUT` (300s), the caller gets a local `429` with `Retry-After: 5` and nothing goes upstream.
3. **Forward.** The request is sent to `freeinference.org` with a streaming body. SSE responses (`text/event-stream`) are relayed chunk by chunk with `Transfer-Encoding: chunked`; other bodies are buffered and re-sent with `Content-Length`.
4. **Release the gate.**

Each completed request is written to SQLite (method, path, status, queue wait, duration, user agent) and one JSON line to the log. A nonzero `waited_s` means it queued behind another request.

Three read-only endpoints bypass the gate and never count against the concurrency limit: the dashboard HTML, the recent-requests JSON, and the SSE stream the dashboard subscribes to.

## Options

```
--data-dir DIR    where the SQLite history and request log live
                  (default ~/.local/share/freeinference-1-concurrent-proxy)
--listen-host H   interface to bind (default 127.0.0.1)
--port P          listen port (default 8788)
--upstream URL    upstream base (default https://freeinference.org)
```

## Run as a service

`deploy/freeinference-serial-proxy.service` is a systemd user unit. Copy it to `~/.config/systemd/user/`, then:

```bash
systemctl --user daemon-reload
systemctl --user enable --now freeinference-serial-proxy
```

## Dashboard on another interface (optional)

The proxy binds `127.0.0.1` only, so it cannot listen on a network interface directly. If you want the dashboard reachable elsewhere, say a Tailscale IP, without exposing the raw proxy API there, run the included bridge:

```bash
freeinference-dashboard-bridge --listen-host <your.ip> --port 8789 \
  --target-host 127.0.0.1 --target-port 8788
```

This forwards only `/__dashboard` and its `/__api/requests` fetch. The raw proxy stays localhost-only. It is pure standard library and is not required for the core proxy.

## Limitations

No TLS and no authentication: by default it binds `127.0.0.1`, so it is only reachable on the machine it runs on and nothing is exposed to the network. If you expose it beyond localhost via the bridge, add your own auth or firewall rules.

This tool only throttles concurrency. It does not add models, routes, billing, or anything else on top of FreeInference; you still need a working FreeInference account and API key.

This project is not affiliated with or endorsed by FreeInference. See the notice at the top.

## Testing

```bash
uv run pytest tests/ -q
```

The suite runs the proxy in-process against a fake upstream on ephemeral ports with an isolated data dir. It covers the serialization gate (5 parallel requests never exceed 1 upstream), queue wait recording, gate-timeout 429s, upstream 502s, streaming passthrough, the local dashboard/API endpoints, and SQLite history. No network access and no FreeInference account needed.

## License

MIT.