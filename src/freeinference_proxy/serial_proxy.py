#!/usr/bin/env python3
"""Local serializing reverse-proxy for FreeInference (freeinference.org/v1).

FreeInference's free tier allows only 1-2 concurrent requests per account
(verified 2026-09-01: live HTTP 429 "Too many concurrent requests (limit: 1)";
community catalog reports "2 Max Concurrent Requests", verified 2026-08-30).
Hermes fires parallel requests (main turn + auxiliary vision/compression +
delegation children), which trips the limit.

This proxy enforces a GLOBAL in-flight cap of exactly 1 request at a time,
in a queue. The second caller waits; it does not fail. It sits ONLY in front
of the freeinference custom_providers entry; all other providers connect
directly and are untouched.

Authorization passes through verbatim from the client (the OpenAI SDK injects
the API key from the environment); no secrets live here.

Each request is appended to a SQLite history, and a self-contained real-time
dashboard (SSE push) is served read-only on the same port, bypassing the gate.
Logs one JSON line per request too; "waited_s" > 0 proves the request was
queued behind another (serialization working as intended).
"""

import argparse
import json
import os
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path

import requests

UPSTREAM = "https://freeinference.org"
LISTEN_HOST = "127.0.0.1"   # localhost-only, always
LISTEN_PORT = 8788
GATE_LIMIT = 1              # hard global serialization: 1 in-flight request
ACQUIRE_TIMEOUT = 300       # max seconds to wait in queue before 429ing
READ_TIMEOUT = (30, 900)    # (connect, read) — long generations need patience
DATA_DIR_DEFAULT = "~/.local/share/freeinference-1-concurrent-proxy"

# Hop-by-hop headers must not be forwarded in either direction.
_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-encoding",
}

_gate = threading.BoundedSemaphore(GATE_LIMIT)
_log_lock = threading.Lock()
_db_lock = threading.Lock()

DATA_DIR = ""   # set in main()
LOG_PATH = ""
DB_PATH = ""


def _load_dashboard_html() -> bytes:
    """Read the served dashboard from the installed package (stdlib)."""
    try:
        return (
            resources.files("freeinference_proxy").joinpath("dashboard.html")
            .read_bytes()
        )
    except (FileNotFoundError, ModuleNotFoundError):
        # Fall back to the source-tree layout (dashboard.html beside this file).
        return (Path(__file__).parent / "dashboard.html").read_bytes()


_DASHBOARD = _load_dashboard_html()


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {msg}"
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with _log_lock:
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")


def _db() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE IF NOT EXISTS requests ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " at REAL NOT NULL,"
        " ts TEXT NOT NULL,"
        " method TEXT NOT NULL,"
        " path TEXT NOT NULL,"
        " status INTEGER NOT NULL,"
        " waited_s REAL NOT NULL,"
        " dur_s REAL NOT NULL,"
        " user_agent TEXT NOT NULL,"
        " input_tokens INTEGER NOT NULL DEFAULT 0,"
        " output_tokens INTEGER NOT NULL DEFAULT 0)"
    )
    # In-place migration for DBs created before token columns existed.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(requests)")}
    if "input_tokens" not in cols:
        conn.execute("ALTER TABLE requests ADD COLUMN input_tokens INTEGER NOT NULL DEFAULT 0")
    if "output_tokens" not in cols:
        conn.execute("ALTER TABLE requests ADD COLUMN output_tokens INTEGER NOT NULL DEFAULT 0")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_at ON requests(at DESC)")
    return conn


def record_request(method, path, status, waited_s, dur_s, ua,
                   input_tokens=0, output_tokens=0) -> None:
    with _db_lock:
        conn = _db()
        try:
            conn.execute(
                "INSERT INTO requests (at, ts, method, path, status, waited_s, dur_s,"
                " user_agent, input_tokens, output_tokens)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (time.time(), time.strftime("%Y-%m-%d %H:%M:%S"),
                 method, path, status, waited_s, dur_s, ua,
                 int(input_tokens or 0), int(output_tokens or 0)),
            )
            conn.commit()
        finally:
            conn.close()


def parse_usage_tokens(body: bytes) -> tuple[int, int]:
    """Extract input/output tokens from an OpenAI-compatible response body.

    Reads `usage.prompt_tokens` / `usage.completion_tokens`. Tolerates the SSE
    framing used by streaming chat completions, where a final `data:` line
    carries the usage object, and returns (0, 0) on any parse failure."""
    if not body:
        return 0, 0
    try:
        text = body.decode("utf-8", "replace")
    except Exception:
        return 0, 0
    # Non-streaming JSON, or an SSE line carrying the usage payload.
    candidates = []
    if text.lstrip().startswith("{"):
        candidates.append(json.loads(text))
    else:
        for line in text.splitlines():
            if line.startswith("data:"):
                payload = line[5:].strip()
                if payload and payload not in ("[DONE]",):
                    try:
                        candidates.append(json.loads(payload))
                    except (json.JSONDecodeError, ValueError):
                        continue  # non-JSON SSE lines (event names, plain text)
    for obj in candidates:
        usage = obj.get("usage") if isinstance(obj, dict) else None
        if isinstance(usage, dict):
            return (int(usage.get("prompt_tokens") or 0),
                    int(usage.get("completion_tokens") or 0))
    return 0, 0


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # silence stderr; we log structured JSON
        pass

    def _reply_json(self, status: int, payload: dict, retry_after: int = 5) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if status == 429:
            self.send_header("Retry-After", str(retry_after))
        self.end_headers()
        self.wfile.write(body)

    def _stream_events(self) -> None:
        """Server-Sent Events: pushes new request rows to the dashboard as they
        land in SQLite. Sub-second realtime without client polling. Never
        touches the upstream gate. Optional ?after=<id> starts the stream from
        that id (skips history replay on reconnect)."""
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
        except (BrokenPipeError, ConnectionResetError):
            return

        last_id = 0
        try:
            last_id = int(self.path.split("after=")[-1].split("&")[0])
        except (ValueError, IndexError):
            last_id = 0
        idle = 0
        self.wfile.write(b"retry: 2000\n\n")
        self.wfile.flush()  # push handshake immediately so proxies/browsers connect now
        try:
            while True:
                with _db_lock:
                    conn = _db()
                    try:
                        rows = conn.execute(
                            "SELECT id, ts, method, path, status, waited_s, dur_s, user_agent,"
                            " input_tokens, output_tokens"
                            " FROM requests WHERE id > ? ORDER BY id ASC", (last_id,)).fetchall()
                    finally:
                        conn.close()
                if rows:
                    for r in rows:
                        last_id = max(last_id, r["id"])
                        payload = {
                            "kind": "request", "id": r["id"], "ts": r["ts"],
                            "method": r["method"], "path": r["path"],
                            "status": r["status"], "waited_s": r["waited_s"],
                            "dur_s": r["dur_s"], "user_agent": r["user_agent"],
                            "input_tokens": r["input_tokens"], "output_tokens": r["output_tokens"],
                        }
                        self.wfile.write(("data: %s\n\n" % json.dumps(payload)).encode())
                    self.wfile.flush()
                    idle = 0
                else:
                    idle += 1
                    if idle >= 30:  # keepalive comment ~every 30s
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                        idle = 0
                time.sleep(1)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            return  # client closed the stream

    def _serve_local(self) -> bool:
        """Serve dashboard + stats API locally. Local reads, never counted
        against the upstream serialization gate. Returns True if handled."""
        if self.command != "GET":
            return False
        if self.path in ("/favicon.ico", "/robots.txt", "/favicon.png"):
            # Browser noise (tab favicon fetch). Answer locally, never touch
            # the upstream gate or freeinference's WAF.
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return True
        if self.path.startswith("/__api/events"):
            self._stream_events()
            return True
        if self.path.startswith("/__api/requests"):
            try:
                limit = int(self.path.split("limit=")[-1].split("&")[0] or "50")
            except ValueError:
                limit = 50
            limit = max(1, min(limit, 500))
            # Time window: today / 7d / all (default all).
            rng = "all"
            if "range=" in self.path:
                cand = self.path.split("range=")[-1].split("&")[0].strip().lower()
                if cand in ("today", "7d", "all"):
                    rng = cand
            now = time.time()
            if rng == "today":
                start = time.mktime(time.localtime(now)[:3] + (0, 0, 0, -1, -1, -1))
                where, params = "WHERE at >= ?", (start,)
            elif rng == "7d":
                where, params = "WHERE at >= ?", (now - 7 * 86400,)
            else:
                where, params = "", ()
            with _db_lock:
                conn = _db()
                rows = conn.execute(
                    f"SELECT id, ts, method, path, status, waited_s, dur_s, user_agent,"
                    f" input_tokens, output_tokens FROM requests {where}"
                    f" ORDER BY id DESC LIMIT ?", params + (limit,)).fetchall()
                total, errs = conn.execute(
                    "SELECT COUNT(*), SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END)"
                    f" FROM requests {where}", params).fetchone()
                tin, tout = conn.execute(
                    "SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0)"
                    f" FROM requests {where}", params).fetchone()
                avgq = conn.execute(
                    "SELECT COALESCE(AVG(waited_s),0) FROM requests"
                    f" {where}", params).fetchone()[0]
                conn.close()
            self._reply_json(200, {
                "requests": [dict(r) for r in rows],
                "total": total, "errors": errs or 0,
                "range": rng,
                "input_tokens": int(tin or 0), "output_tokens": int(tout or 0),
                "avg_queue_s": round(float(avgq or 0), 3),
            })
            return True
        if self.path.startswith("/__dashboard"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(_DASHBOARD)))
            self.end_headers()
            self.wfile.write(_DASHBOARD)
            return True
        return False

    def _proxy(self) -> None:
        t0 = time.monotonic()
        if self._serve_local():
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        body = self.rfile.read(length) if length else None

        fwd_headers = {}
        for key, value in self.headers.items():
            if key.lower() in _HOP_BY_HOP or key.lower() == "host":
                continue
            fwd_headers[key] = value
        fwd_headers["Host"] = "freeinference.org"
        # Preserve the client's User-Agent exactly. No spoofing or fallback.

        acquired = _gate.acquire(timeout=ACQUIRE_TIMEOUT)
        if not acquired:
            log(json.dumps({
                "event": "gate_timeout", "method": self.command,
                "path": self.path, "waited_s": ACQUIRE_TIMEOUT,
            }))
            record_request(self.command, self.path, 429,
                           ACQUIRE_TIMEOUT, 0, fwd_headers.get("User-Agent", ""))
            self._reply_json(429, {
                "error": {
                    "message": "local freeinference serialization queue timed out",
                    "type": "local_proxy_timeout",
                    "code": 429,
                }
            })
            return

        try:
            waited_s = round(time.monotonic() - t0, 3)
            try:
                upstream = requests.request(
                    self.command,
                    UPSTREAM + self.path,
                    headers=fwd_headers,
                    data=body,
                    stream=True,
                    timeout=READ_TIMEOUT,
                )
            except requests.RequestException as exc:
                log(json.dumps({
                    "event": "upstream_error", "method": self.command,
                    "path": self.path, "error": str(exc)[:300],
                }))
                record_request(self.command, self.path, 502,
                               waited_s, round(time.monotonic() - t0, 3),
                               fwd_headers.get("User-Agent", ""))
                self._reply_json(502, {
                    "error": {
                        "message": f"upstream connection failed: {exc}"[:300],
                        "type": "local_proxy_upstream_error",
                        "code": 502,
                    }
                })
                return

            content_type = upstream.headers.get("Content-Type", "")
            is_stream = "text/event-stream" in content_type

            self.send_response(upstream.status_code)
            for key, value in upstream.headers.items():
                if key.lower() in _HOP_BY_HOP or key.lower() == "content-length":
                    continue
                self.send_header(key, value)

            if is_stream:
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                tail = bytearray()  # bounded tail for token capture
                try:
                    for chunk in upstream.iter_content(chunk_size=1024):
                        if not chunk:
                            continue
                        self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                        tail.extend(chunk)
                        if len(tail) > 65536:  # ponytail: keep last 64KB for usage tail
                            del tail[: len(tail) - 65536]
                except (BrokenPipeError, ConnectionResetError):
                    log(json.dumps({"event": "client_disconnected", "path": self.path}))
                    return  # finally still releases the gate
                self.wfile.write(b"0\r\n\r\n")  # terminate the chunked stream
                in_tok, out_tok = parse_usage_tokens(bytes(tail))
            else:
                data = upstream.content  # buffer non-streaming bodies (models, errors)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                try:
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    log(json.dumps({"event": "client_disconnected", "path": self.path}))
                    return
                in_tok, out_tok = parse_usage_tokens(data)

            ua = fwd_headers.get("User-Agent", "")
            log(json.dumps({
                "event": "request", "method": self.command, "path": self.path,
                "status": upstream.status_code, "waited_s": waited_s,
                "dur_s": round(time.monotonic() - t0, 3),
                "user_agent": ua, "input_tokens": in_tok, "output_tokens": out_tok,
            }))
            record_request(self.command, self.path, upstream.status_code,
                           waited_s, round(time.monotonic() - t0, 3), ua,
                           in_tok, out_tok)
        finally:
            _gate.release()

    do_GET = _proxy
    do_POST = _proxy
    do_DELETE = _proxy
    do_PUT = _proxy
    do_PATCH = _proxy
    do_OPTIONS = _proxy


def main() -> None:
    global DATA_DIR, LOG_PATH, DB_PATH
    global UPSTREAM, LISTEN_HOST, LISTEN_PORT
    parser = argparse.ArgumentParser(
        description="Local 1-concurrent serializing reverse-proxy for FreeInference.")
    parser.add_argument("--data-dir", default=DATA_DIR_DEFAULT,
                        help="dir for sqlite history + request log "
                             "(default ~/.local/share/freeinference-1-concurrent-proxy)")
    parser.add_argument("--listen-host", default=LISTEN_HOST, help="default 127.0.0.1")
    parser.add_argument("--port", type=int, default=LISTEN_PORT, help="default 8788")
    parser.add_argument("--upstream", default=UPSTREAM, help="default https://freeinference.org")
    args = parser.parse_args()

    LISTEN_HOST = args.listen_host
    LISTEN_PORT = args.port
    UPSTREAM = args.upstream
    DATA_DIR = os.path.expanduser(args.data_dir)
    os.makedirs(DATA_DIR, exist_ok=True)
    DB_PATH = os.path.join(DATA_DIR, "requests.db")
    LOG_PATH = os.path.join(DATA_DIR, "proxy.log")

    log(json.dumps({"event": "start", "listen": f"{LISTEN_HOST}:{LISTEN_PORT}",
                    "gate_limit": GATE_LIMIT, "upstream": UPSTREAM,
                    "data_dir": DATA_DIR}))
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()