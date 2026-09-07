# FreeInference 1-concurrent request proxy

A local reverse proxy that caps FreeInference at one in-flight upstream request at a time and queues the rest.

Auto-generated text is boring. Here is the short version of why this exists.

## Why I built it

FreeInference's free tier lets you run 1 to 2 concurrent requests per account. I hit a hard wall on this the hard way: live `429 Too many concurrent requests (limit: 1)` from the upstream, and a community catalog saying the same thing ("2 Max Concurrent Requests").

The problem is that an AI agent like Hermes does not make one request at a time. A single turn fires a main call plus auxiliary vision and compression calls plus delegation children. All of those hit FreeInference in one burst. The provider answers with `429` and the requests just fail. You do not get a model answer; you get an error.

I could have throttled inside the agent, but that meant touching every place that sends a request. The cleaner fix is one choke point in front of the provider. This proxy is that choke point: it allows exactly one request upstream at any moment and puts every other caller in a queue. The second request waits instead of failing. The agent never sees the concurrency error because the concurrency never happens upstream.

The proxy sits only in front of the FreeInference provider entry. Every other provider connects directly and is untouched.

## How it works

- Global gate of `1` in-flight request, implemented as a semaphore. Every request after the first waits.
- Callers that wait longer than `300s` get a local `429` with `Retry-After: 5`. Upstream requests can run up to `900s`, so a long generation will not die to a premature connect timeout.
- Authorization passes through verbatim from the client. No secrets live in this repo.
- Each request lands in a SQLite history file with method, path, status, queue wait, duration, and user agent.

## The dashboard

Because a one-at-a-time queue hides queued work, the proxy logs every request and serves a real-time dashboard on the same port. It is a single self-contained HTML file, dark theme, no build step, no dependencies, and it updates live over Server-Sent Events (no page refresh, no polling).

Read-only endpoints, all served locally and never counted against the upstream gate:

- `/__dashboard` the dashboard
- `/__api/requests?limit=N` the recent-requests JSON
- `/__api/events` the SSE stream

![FreeInference proxy dashboard](docs/dashboard.png)

## Install

Python 3.9+ and `requests`.

```bash
pip install -r requirements.txt
# or from source
pip install .
```

Then run:

```bash
freeinference-serial-proxy
```

Defaults: listens on `127.0.0.1:8788`, forwards to `https://freeinference.org`, data written to `~/.local/share/freeinference-1-concurrent-proxy/`.

Point an OpenAI-compatible client at `http://127.0.0.1:8788`. Set the API key as you normally would for FreeInference; the proxy forwards it untouched.

Point your browser at `http://127.0.0.1:8788/__dashboard`.

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

The proxy binds `127.0.0.1` only, so it cannot listen on a network interface directly. If you want the dashboard reachable somewhere else (say on a Tailscale IP) without exposing the raw proxy API there, run the included bridge:

```bash
freeinference-dashboard-bridge --listen-host <your.ip> --port 8789 \
  --target-host 127.0.0.1 --target-port 8788
```

This forwards only `/__dashboard` and its `/__api/requests` fetch. The raw proxy stays localhost-only. It is pure standard library and is not required for the core proxy to work.

## What this is not

No TLS, no auth, no multi-account support. FreeInference caps concurrency per account, and this proxy assumes one account. If you need to spread load across accounts, this is the wrong tool. If you need to gate a single account locally, this is the whole job.

## License

MIT.