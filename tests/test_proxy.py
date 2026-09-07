"""Tests for the FreeInference serializing proxy.

The proxy forwards to a real upstream; to test it in isolation we run a tiny
fake upstream in-process and point the proxy at it. The fake upstream can
hold requests open (to exercise the serialization gate) and record what it
receives and how many requests were in flight at once.

Each test builds a fresh proxy with an isolated data dir (tmp_path) and an
ephemeral port, so nothing is shared or touches the real FreeInference.
"""
import json
import sqlite3
import threading
import time

import pytest
import requests
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import freeinference_proxy.serial_proxy as proxy


# ---------------------------------------------------------------------------
# Fake upstream
# ---------------------------------------------------------------------------
class UpstreamState:
    def __init__(self):
        self.lock = threading.Lock()
        self.received = []        # dicts: method, path, headers, body
        self.hold_s = 0.0         # seconds each proxied request is held
        self.hold_event = threading.Event()  # set: start blocking requests
        self._active = 0          # current in-flight count
        self.max_active = 0       # peak in-flight count (the serialization check)
        self.fail = False         # if True, respond 500 (upstream error path)

    def track_active(self):
        with self.lock:
            self._active += 1
            self.max_active = max(self.max_active, self._active)

    def track_done(self):
        with self.lock:
            self._active -= 1


class UpstreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _handle(self):
        state = self.server.state
        state.track_active()
        try:
            state.hold_event.wait(5)          # allow tests to open the floodgates
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode() if length else ""
            with state.lock:
                state.received.append({
                    "method": self.command,
                    "path": self.path,
                    "headers": dict(self.headers),
                    "body": body,
                })
            if state.hold_s:
                time.sleep(state.hold_s)
            if state.fail:
                payload = b'{"error":"boom"}'
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            if self.headers.get("X-Want-SSE"):
                data = b"data: first\n\ndata: second\n\n"
                if self.headers.get("X-Want-Usage"):
                    data += b'data: {"usage":{"prompt_tokens":3,"completion_tokens":9}}\n\ndata: [DONE]\n\n'
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("X-Upstream-Saw", str(self.headers.get("X-Marker", "")))
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            payload = json.dumps({"ok": True, "path": self.path}).encode()
            if self.headers.get("X-Want-Usage"):
                payload = json.dumps({
                    "ok": True, "path": self.path,
                    "usage": {"prompt_tokens": 11, "completion_tokens": 22},
                }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Upstream-Saw", str(self.headers.get("X-Marker", "")))
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        finally:
            state.track_done()

    do_GET = _handle
    do_POST = _handle
    do_PUT = _handle
    do_DELETE = _handle
    do_PATCH = _handle


class UpstreamServer:
    """In-process upstream server. start()/stop() manage the thread."""

    def __init__(self):
        self.state = UpstreamState()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), UpstreamHandler)
        self.httpd.state = self.state
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def port(self):
        return self.httpd.server_address[1]

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture()
def upstream():
    srv = UpstreamServer().start()
    yield srv
    srv.stop()


@pytest.fixture()
def proxy_server(monkeypatch, tmp_path, upstream):
    """A real proxy pointed at the fake upstream, with isolated data dir."""
    monkeypatch.setattr(proxy, "UPSTREAM", f"http://127.0.0.1:{upstream.port}")
    monkeypatch.setattr(proxy, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(proxy, "DB_PATH", str(tmp_path / "requests.db"))
    monkeypatch.setattr(proxy, "LOG_PATH", str(tmp_path / "proxy.log"))
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), proxy.Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    yield {"httpd": httpd, "base": base, "db": str(tmp_path / "requests.db"), "data_dir": str(tmp_path)}
    httpd.shutdown()
    httpd.server_close()
    thread.join(timeout=2)


def db_rows(path, limit=100):
    conn = sqlite3.connect(path)
    try:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(
            "SELECT method, path, status, waited_s, dur_s, user_agent,"
            " input_tokens, output_tokens"
            " FROM requests ORDER BY id LIMIT ?", (limit,)).fetchall()]
    finally:
        conn.close()


def wait_for_rows(path, n, timeout=3.0):
    """Poll until the proxy's DB has n rows. The proxy commits the history row
    just after it finishes sending the response body, so the client can observe
    the response before the row is on disk; this closes that race."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            rows = db_rows(path)
        except sqlite3.OperationalError:
            rows = []
        if len(rows) >= n:
            return rows
        time.sleep(0.05)
    raise AssertionError(f"expected >= {n} rows in {path}, got {len(rows)}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
def test_local_endpoints_never_reach_upstream(proxy_server, upstream):
    base = proxy_server["base"]
    r = requests.get(f"{base}/__dashboard")
    assert r.status_code == 200
    assert r.headers["Content-Type"].startswith("text/html")

    r = requests.get(f"{base}/__api/requests?limit=5")
    assert r.status_code == 200
    body = r.json()
    assert body["requests"] == []
    assert body["total"] == 0
    assert body["errors"] == 0
    assert body["input_tokens"] == 0
    assert body["output_tokens"] == 0

    r = requests.get(f"{base}/favicon.ico")
    assert r.status_code == 204

    # none of the above count against the gate or hit the upstream
    assert upstream.state.received == []


def test_events_endpoint_is_streaming(proxy_server):
    """The /__api/events endpoint opens an SSE stream; the handshake line is
    flushed immediately. We read it with a raw socket because requests/urllib3
    blocks on a blocking socket for a full buffer, which masks the flush."""
    import socket as _socket
    base = proxy_server["base"]
    host = base.split("//")[1].rsplit(":", 1)[0]
    port = int(base.rsplit(":", 1)[1])
    s = _socket.create_connection((host, port), timeout=5)
    try:
        s.sendall(b"GET /__api/events HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
        buf = b""
        deadline = time.monotonic() + 3
        while b"retry:" not in buf and time.monotonic() < deadline:
            s.settimeout(0.5)
            try:
                chunk = s.recv(4096)
            except _socket.timeout:
                continue
            if not chunk:
                break
            buf += chunk
        head, _, _ = buf.partition(b"\r\n\r\n")
        assert b"200 OK" in head
        assert b"text/event-stream" in head
        assert b"retry: 2000" in buf
    finally:
        s.close()


def test_forwards_request_verbatim(proxy_server, upstream):
    base = proxy_server["base"]
    marker = "custom-ua"
    r = requests.get(f"{base}/v1/models", headers={"X-Marker": marker, "User-Agent": marker}, timeout=30)
    assert r.status_code == 200
    assert r.json() == {"ok": True, "path": "/v1/models"}
    assert r.headers["X-Upstream-Saw"] == marker

    upstream.state.hold_event.wait(2)  # first request already recorded
    recv = upstream.state.received[0]
    assert recv["method"] == "GET"
    assert recv["path"] == "/v1/models"
    assert recv["headers"]["X-Marker"] == marker


def test_post_body_and_json_forwarded(proxy_server, upstream):
    base = proxy_server["base"]
    body = {"model": "deepseek-v4-flash", "messages": [{"role": "user", "content": "hi"}]}
    r = requests.post(f"{base}/v1/chat/completions", json=body, timeout=30)
    assert r.status_code == 200
    recv = upstream.state.received[0]
    assert recv["method"] == "POST"
    assert recv["body"] == json.dumps(body)


def test_streaming_response_forwarded(proxy_server, upstream):
    base = proxy_server["base"]
    r = requests.get(f"{base}/stream", headers={"X-Want-SSE": "1"}, timeout=30)
    assert r.status_code == 200
    assert "text/event-stream" in r.headers["Content-Type"]
    assert email_body(r) == b"data: first\n\ndata: second\n\n"


def email_body(response):
    chunks = []
    for chunk in response.iter_content():
        chunks.append(chunk)
    return b"".join(chunks)


def test_serialization_gate_caps_concurrency_at_one(proxy_server, upstream):
    """"The core guarantee: N parallel clients still only ever run 1 upstream request at a time."""
    base = proxy_server["base"]
    upstream.state.hold_s = 0.2
    upstream.state.hold_event.set()  # let all requests through to the gate

    # fire 5 parallel requests
    with requests.get(f"{base}/a", timeout=30), requests.get(f"{base}/b", timeout=30), \
         requests.get(f"{base}/c", timeout=30), requests.get(f"{base}/d", timeout=30), \
         requests.get(f"{base}/e", timeout=30):
        pass

    assert len(upstream.state.received) == 5
    assert upstream.state.max_active == 1  # never more than one at a time


def test_queue_wait_is_recorded(proxy_server, upstream):
    """A request that queues behind another gets a nonzero waited_s."""
    base = proxy_server["base"]
    upstream.state.hold_s = 0.3
    upstream.state.hold_event.set()

    results = []
    def fire(url):
        results.append(requests.get(f"{base}{url}", timeout=30).status_code)
    threads = [threading.Thread(target=fire, args=(u,)) for u in ("/a", "/b", "/c")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results == [200, 200, 200]
    rows = wait_for_rows(proxy_server["db"], 3)
    assert any(r["waited_s"] > 0 for r in rows)  # at least one queued
    assert all(r["status"] == 200 for r in rows)


def test_gate_timeout_returns_429(monkeypatch, proxy_server, upstream):
    """A caller that waits past ACQUIRE_TIMEOUT gets a local 429, nothing upstream."""
    monkeypatch.setattr(proxy, "ACQUIRE_TIMEOUT", 1)  # give up after 1s
    base = proxy_server["base"]
    upstream.state.hold_s = 3.0   # each upstream request holds the gate for 3s
    upstream.state.hold_event.set()

    # First request grabs the gate for ~3s; second times out after ACQUIRE_TIMEOUT (1s).
    first = threading.Thread(target=lambda: requests.get(f"{base}/first", timeout=30))
    first.start()
    time.sleep(0.3)  # let the first request acquire the gate
    r2 = requests.get(f"{base}/second", timeout=15)
    first.join()

    assert r2.status_code == 429
    assert "local_proxy_timeout" in r2.text
    assert r2.headers.get("Retry-After") == "5"
    # the timed-out caller never reached upstream
    assert len(upstream.state.received) == 1


def test_upstream_failure_returns_502(monkeypatch, proxy_server, upstream):
    """When the upstream is unreachable, the proxy returns a local 502."""
    base = proxy_server["base"]
    # Point the proxy at a dead port; connection refused raises requests.RequestException.
    monkeypatch.setattr(proxy, "UPSTREAM", "http://127.0.0.1:1")
    upstream.state.hold_event.set()
    r = requests.get(f"{base}/boom", timeout=30)
    assert r.status_code == 502
    assert "local_proxy_upstream_error" in r.text
    # the failure is recorded in history, not sent upstream (nothing to send to)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["status"] == 502


def test_history_row_recorded(proxy_server, upstream):
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/models", headers={"User-Agent": "test-suite/1.0"}, timeout=30)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["method"] == "GET"
    assert rows[0]["path"] == "/v1/models"
    assert rows[0]["status"] == 200
    assert rows[0]["user_agent"] == "test-suite/1.0"
    assert rows[0]["waited_s"] == 0


def test_tokens_recorded_from_buffered_response(proxy_server, upstream):
    """Non-streaming chat completions: usage.prompt_tokens/completion_tokens are stored."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/chat/completions", headers={"X-Want-Usage": "1"}, timeout=30)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["input_tokens"] == 11
    assert rows[0]["output_tokens"] == 22


def test_tokens_recorded_from_streaming_response(proxy_server, upstream):
    """Streaming responses: the final SSE chunk carries usage; it is captured."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/chat/completions",
                 headers={"X-Want-SSE": "1", "X-Want-Usage": "1"}, timeout=30)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["input_tokens"] == 3
    assert rows[0]["output_tokens"] == 9


def test_no_usage_means_zero_tokens(proxy_server, upstream):
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/models", timeout=30)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["input_tokens"] == 0
    assert rows[0]["output_tokens"] == 0


def test_api_range_filter(proxy_server, upstream):
    """range=today / range=7d / range=all filter rows and aggregate totals."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    # three requests, all happening "now"
    for _ in range(3):
        requests.get(f"{base}/v1/models", headers={"X-Want-Usage": "1"}, timeout=30)
    wait_for_rows(proxy_server["db"], 3)  # all rows committed before querying the API

    r_today = requests.get(f"{base}/__api/requests?range=today&limit=50", timeout=5).json()
    r_all = requests.get(f"{base}/__api/requests?range=all&limit=50", timeout=5).json()
    r_7d = requests.get(f"{base}/__api/requests?range=7d&limit=50", timeout=5).json()

    # all three in the window (fresh DB, all at now)
    assert r_today["total"] == 3
    assert r_7d["total"] == 3
    assert r_all["total"] == 3
    # token aggregates sum the three 11/22 rows
    assert r_all["input_tokens"] == 33
    assert r_all["output_tokens"] == 66
    assert r_all["range"] == "all"
    assert r_all["requests"][0]["input_tokens"] == 11
    # avg queue time over the window is a number (>= 0)
    assert isinstance(r_all["avg_queue_s"], (int, float))
    assert r_all["avg_queue_s"] >= 0


def test_api_range_invalid_defaults_to_all(proxy_server, upstream):
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/models", headers={"X-Want-Usage": "1"}, timeout=30)
    wait_for_rows(proxy_server["db"], 1)
    r = requests.get(f"{base}/__api/requests?range=bogus", timeout=5).json()
    assert r["total"] == 1
    assert r["input_tokens"] == 11