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
# Test auth keys
# ---------------------------------------------------------------------------
TEST_MOBIN_KEY = "test-mobin-key-0123456789"          # 32+ chars
TEST_NIRJHOR_KEY = "test-nirjhor-key-9876543210"
TEST_WRONG_KEY = "test-wrong-key-0000"
TEST_ADMIN_KEY = "test-admin-key-000000000000"


def auth(name=TEST_MOBIN_KEY):
    return {"Authorization": f"Bearer {name}"}


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
        self.burst_429s = 0       # remaining 429 responses to emit before succeeding

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
            if state.burst_429s > 0:
                with state.lock:
                    state.burst_429s -= 1
                payload = b'{"error":"Too many concurrent requests (limit: 1)"}'
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", getattr(state, "retry_after", "0"))
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
    """A real proxy pointed at the fake upstream, with isolated data dir
    and a deterministic auth key set injected via env."""
    monkeypatch.setattr(proxy, "UPSTREAM", f"http://127.0.0.1:{upstream.port}")
    monkeypatch.setattr(proxy, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(proxy, "DB_PATH", str(tmp_path / "requests.db"))
    monkeypatch.setattr(proxy, "LOG_PATH", str(tmp_path / "proxy.log"))
    monkeypatch.setattr(proxy, "_UPSTREAM_KEY", "local-hermes-upstream-key")
    monkeypatch.setattr(proxy, "_ENV_ADMIN_KEYS", (TEST_ADMIN_KEY,))
    # Seed the DB with the test LLM keys (matching how main() seeds env keys)
    # so _refresh_key_cache() restores them after any create/enable/disable.
    with proxy._db_lock:
        conn = proxy._db()
        try:
            for raw, name in ((TEST_MOBIN_KEY, "mobin"), (TEST_NIRJHOR_KEY, "nirjhor")):
                try:
                    conn.execute(
                        "INSERT INTO api_keys (name, key_hash, role, enabled, created_at)"
                        " VALUES (?,?,'llm',1,?)",
                        (name, proxy._hash_key(raw), time.time()))
                except sqlite3.IntegrityError:
                    pass
            conn.commit()
        finally:
            conn.close()
    proxy._refresh_key_cache()
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
            "SELECT method, path, status, waited_s, dur_s, user_agent, key_name,"
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
    r = requests.get(f"{base}/__dashboard", headers=auth(TEST_ADMIN_KEY))
    assert r.status_code == 200
    assert r.headers["Content-Type"].startswith("text/html")

    r = requests.get(f"{base}/__api/requests?limit=5", headers=auth(TEST_ADMIN_KEY))
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
        s.sendall(
            f"GET /__api/events HTTP/1.1\r\nHost: test\r\n"
            f"Authorization: Bearer {TEST_ADMIN_KEY}\r\n"
            f"Connection: close\r\n\r\n".encode()
        )
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
    r = requests.get(f"{base}/v1/models", headers=auth() | {"X-Marker": marker, "User-Agent": marker}, timeout=30)
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
    r = requests.post(f"{base}/v1/chat/completions", json=body, headers=auth(), timeout=30)
    assert r.status_code == 200
    recv = upstream.state.received[0]
    assert recv["method"] == "POST"
    assert recv["body"] == json.dumps(body)


def test_streaming_response_forwarded(proxy_server, upstream):
    base = proxy_server["base"]
    r = requests.get(f"{base}/stream", headers=auth() | {"X-Want-SSE": "1"}, timeout=30)
    assert r.status_code == 200
    assert "text/event-stream" in r.headers["Content-Type"]
    assert email_body(r) == b"data: first\n\ndata: second\n\n"


def email_body(response):
    chunks = []
    for chunk in response.iter_content():
        chunks.append(chunk)
    return b"".join(chunks)


def test_serialization_gate_caps_concurrency_at_one(proxy_server, upstream):
    """The core guarantee: N parallel clients still only ever run 1 upstream request at a time —
    a HARD invariant under contention, not best-effort."""
    base = proxy_server["base"]
    upstream.state.hold_s = 0.03
    upstream.state.hold_event.set()  # let all requests through to the gate

    # fire 20 parallel requests down overlapping sockets to maximise race pressure
    urls = [f"{base}/req-{i}" for i in range(20)]
    threads = [threading.Thread(target=lambda u=u: requests.get(u, headers=auth(), timeout=30))
               for u in urls]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert len(upstream.state.received) == 20
    assert upstream.state.max_active == 1  # never more than one at a time, ever


def test_queue_wait_is_recorded(proxy_server, upstream):
    """A request that queues behind another gets a nonzero waited_s."""
    base = proxy_server["base"]
    upstream.state.hold_s = 0.3
    upstream.state.hold_event.set()

    results = []
    def fire(url):
        results.append(requests.get(f"{base}{url}", headers=auth(), timeout=30).status_code)
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
    first = threading.Thread(target=lambda: requests.get(f"{base}/first", headers=auth(), timeout=30))
    first.start()
    time.sleep(0.3)  # let the first request acquire the gate
    r2 = requests.get(f"{base}/second", headers=auth(), timeout=15)
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
    r = requests.get(f"{base}/boom", headers=auth(), timeout=30)
    assert r.status_code == 502
    assert "local_proxy_upstream_error" in r.text
    # the failure is recorded in history, not sent upstream (nothing to send to)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["status"] == 502


def test_history_row_recorded(proxy_server, upstream):
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/models", headers=auth() | {"User-Agent": "test-suite/1.0"}, timeout=30)
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
    requests.get(f"{base}/v1/chat/completions", headers=auth() | {"X-Want-Usage": "1"}, timeout=30)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["input_tokens"] == 11
    assert rows[0]["output_tokens"] == 22


def test_tokens_recorded_from_streaming_response(proxy_server, upstream):
    """Streaming responses: the final SSE chunk carries usage; it is captured."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/chat/completions",
                 headers=auth() | {"X-Want-SSE": "1", "X-Want-Usage": "1"}, timeout=30)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["input_tokens"] == 3
    assert rows[0]["output_tokens"] == 9


def test_no_usage_means_zero_tokens(proxy_server, upstream):
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/models", headers=auth(), timeout=30)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["input_tokens"] == 0
    assert rows[0]["output_tokens"] == 0


def test_api_range_filter(proxy_server, upstream):
    """range=today / range=7d / range=all filter rows and aggregate totals."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    # three requests, all happening "now"
    for _ in range(3):
        requests.get(f"{base}/v1/models", headers=auth() | {"X-Want-Usage": "1"}, timeout=30)
    wait_for_rows(proxy_server["db"], 3)  # all rows committed before querying the API

    r_today = requests.get(f"{base}/__api/requests?range=today&limit=50", headers=auth(TEST_ADMIN_KEY), timeout=5).json()
    r_all = requests.get(f"{base}/__api/requests?range=all&limit=50", headers=auth(TEST_ADMIN_KEY), timeout=5).json()
    r_7d = requests.get(f"{base}/__api/requests?range=7d&limit=50", headers=auth(TEST_ADMIN_KEY), timeout=5).json()

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
    requests.get(f"{base}/v1/models", headers=auth() | {"X-Want-Usage": "1"}, timeout=30)
    wait_for_rows(proxy_server["db"], 1)
    r = requests.get(f"{base}/__api/requests?range=bogus", headers=auth(TEST_ADMIN_KEY), timeout=5).json()
    assert r["total"] == 1
    assert r["input_tokens"] == 11


# ---------------------------------------------------------------------------
# API-key authentication
# ---------------------------------------------------------------------------
def test_proxy_requires_api_key(proxy_server, upstream):
    """A proxied request with no Authorization header is rejected 401."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    r = requests.get(f"{base}/v1/models", timeout=10)
    assert r.status_code == 401
    assert "error" in r.json()
    # nothing reached upstream
    assert upstream.state.received == []


def test_proxy_rejects_wrong_api_key(proxy_server, upstream):
    """A proxied request with an invalid key is rejected 401."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    r = requests.get(f"{base}/v1/models", headers=auth(TEST_WRONG_KEY), timeout=10)
    assert r.status_code == 401
    assert upstream.state.received == []


def test_proxy_rejects_bare_key_in_query(proxy_server, upstream):
    """The key must come in a header, not the URL query string (no leakage into logs)."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    r = requests.get(f"{base}/v1/models", params={"api_key": TEST_MOBIN_KEY}, timeout=10)
    assert r.status_code == 401
    assert upstream.state.received == []


@pytest.mark.parametrize("key", [TEST_MOBIN_KEY, TEST_NIRJHOR_KEY])
def test_proxy_accepts_valid_keys(proxy_server, upstream, key):
    """Both role keys authenticate a proxied LLM request."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    r = requests.get(f"{base}/v1/models", headers=auth(key), timeout=10)
    assert r.status_code == 200
    assert r.json() == {"ok": True, "path": "/v1/models"}


def test_proxy_accepts_x_api_key_header(proxy_server, upstream):
    """`X-Api-Key` header works as an alternative to Bearer Authorization."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    r = requests.get(f"{base}/v1/models", headers={"X-Api-Key": TEST_MOBIN_KEY}, timeout=10)
    assert r.status_code == 200


def test_proxy_accepts_local_hermes_upstream_key(proxy_server, upstream):
    """The key Hermes already injects (FREEINFERENCE_API_KEY) keeps the
    existing provider working with zero config change."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    r = requests.get(f"{base}/v1/models", headers=auth("local-hermes-upstream-key"), timeout=10)
    assert r.status_code == 200


def test_proxy_normalizes_bearer_case(proxy_server, upstream):
    """`bearer` (lowercase scheme) must authenticate too — clients differ."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    r = requests.get(
        f"{base}/v1/models",
        headers={"Authorization": f"bearer {TEST_MOBIN_KEY}"}, timeout=10)
    assert r.status_code == 200


def test_proxy_does_not_forward_local_api_key_upstream(proxy_server, upstream):
    """The client's proxy key must never leak upstream; the proxy instead
    injects the REAL upstream credential (FIF_UPSTREAM_KEY)."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/models", headers=auth(TEST_MOBIN_KEY), timeout=10)
    recv = upstream.state.received[0]
    authz = recv["headers"].get("Authorization", "")
    # the upstream saw the injected upstream credential, never the client's key
    assert authz == "Bearer local-hermes-upstream-key"
    assert TEST_MOBIN_KEY not in authz


def test_admin_endpoints_require_admin_key(proxy_server, upstream):
    """Dashboard and stats API reject a valid LLM key; they need the admin key."""
    base = proxy_server["base"]
    for path in ("/__dashboard", "/__api/requests?limit=5", "/__api/events"):
        r = requests.get(f"{base}{path}", headers=auth(TEST_MOBIN_KEY), timeout=10)
        assert r.status_code == 401, f"{path} should reject a non-admin key"
    # the admin key works (dashboard + requests already asserted elsewhere)
    r = requests.get(f"{base}/__dashboard", headers=auth(TEST_ADMIN_KEY), timeout=10)
    assert r.status_code == 200


def test_favicon_noise_stays_public(proxy_server, upstream):
    """favicon.ico is served without auth (browser tab noise), never hits upstream."""
    base = proxy_server["base"]
    r = requests.get(f"{base}/favicon.ico", timeout=10)
    assert r.status_code == 204
    assert upstream.state.received == []


def test_user_agent_is_never_spoofed(proxy_server, upstream):
    """The client's REAL User-Agent always reaches upstream verbatim."""
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    real_ua = "MyClient/1.2.3 (real-ua)"
    requests.get(f"{base}/v1/models", headers=auth() | {"User-Agent": real_ua}, timeout=10)
    recv = upstream.state.received[0]
    assert recv["headers"]["User-Agent"] == real_ua


# ---------------------------------------------------------------------------
# API-key management (create / list / enable / disable) — admin-only
# ---------------------------------------------------------------------------
def _admin(base):
    return {"Authorization": f"Bearer {TEST_ADMIN_KEY}"}


def test_create_key_returns_plaintext_once(proxy_server, upstream):
    """POST /__api/keys returns the full key once; the stored record never contains it."""
    base = proxy_server["base"]
    r = requests.post(f"{base}/__api/keys", json={"name": "alice"}, headers=_admin(base), timeout=10)
    assert r.status_code == 201
    body = r.json()
    assert body["name"] == "alice"
    new_key = body["key"]
    assert len(new_key) >= 24
    # the new key actually works
    upstream.state.hold_event.set()
    rr = requests.get(f"{base}/v1/models", headers=auth(new_key), timeout=10)
    assert rr.status_code == 200
    # the DB record does not contain the key (only its hash)
    rows = db_rows(proxy_server["db"])
    assert all(new_key not in str(r) for r in rows)


def test_key_list_shows_masked_keys(proxy_server, upstream):
    """GET /__api/keys returns name + masked key, never plaintext."""
    base = proxy_server["base"]
    requests.post(f"{base}/__api/keys", json={"name": "bob"}, headers=_admin(base), timeout=10)
    r = requests.get(f"{base}/__api/keys", headers=_admin(base), timeout=10)
    assert r.status_code == 200
    keys = {k["name"]: k for k in r.json()["keys"]}
    assert "bob" in keys
    # masked: last 4 only, prefixed (never the raw key)
    mask = keys["bob"]["masked"]
    assert mask != "" and mask.endswith(keys["bob"]["key_last4"])
    assert "..." in mask or mask.startswith("sk-")


def test_create_key_requires_admin(proxy_server, upstream):
    """A non-admin key cannot create keys."""
    base = proxy_server["base"]
    r = requests.post(f"{base}/__api/keys", json={"name": "mallory"}, headers=auth(), timeout=10)
    assert r.status_code == 401


def test_create_duplicate_name_rejected(proxy_server, upstream):
    base = proxy_server["base"]
    h = _admin(base)
    r1 = requests.post(f"{base}/__api/keys", json={"name": "carol"}, headers=h, timeout=10)
    assert r1.status_code == 201
    r2 = requests.post(f"{base}/__api/keys", json={"name": "carol"}, headers=h, timeout=10)
    assert r2.status_code == 409  # duplicate name


def test_disable_key_rejects_requests(proxy_server, upstream):
    """Disabling a key makes it stop authenticating immediately, without a restart."""
    base = proxy_server["base"]
    created = requests.post(f"{base}/__api/keys", json={"name": "dave"}, headers=_admin(base), timeout=10).json()
    kid = created["id"]; key = created["key"]

    upstream.state.hold_event.set()
    assert requests.get(f"{base}/v1/models", headers=auth(key), timeout=10).status_code == 200

    # disable it
    dr = requests.patch(f"{base}/__api/keys/{kid}", json={"enabled": False}, headers=_admin(base), timeout=10)
    assert dr.status_code == 200
    # now the key is rejected (without restart)
    rr = requests.get(f"{base}/v1/models", headers=auth(key), timeout=10)
    assert rr.status_code == 401


def test_enable_key_restores_access(proxy_server, upstream):
    base = proxy_server["base"]
    created = requests.post(f"{base}/__api/keys", json={"name": "erin"}, headers=_admin(base), timeout=10).json()
    kid = created["id"]; key = created["key"]
    requests.patch(f"{base}/__api/keys/{kid}", json={"enabled": False}, headers=_admin(base), timeout=10)

    upstream.state.hold_event.set()
    assert requests.get(f"{base}/v1/models", headers=auth(key), timeout=10).status_code == 401
    # re-enable
    requests.patch(f"{base}/__api/keys/{kid}", json={"enabled": True}, headers=_admin(base), timeout=10)
    assert requests.get(f"{base}/v1/models", headers=auth(key), timeout=10).status_code == 200


def test_disable_admin_key_admin_surface_still_works(proxy_server, upstream):
    """A disabled managed key cannot read the dashboard; the env admin key still can."""
    base = proxy_server["base"]
    created = requests.post(f"{base}/__api/keys", json={"name": "superadmin", "role": "admin"},
                            headers=_admin(base), timeout=10).json()
    kid = created["id"]; key = created["key"]
    # the new admin key can hit the dashboard
    assert requests.get(f"{base}/__dashboard", headers=auth(key), timeout=10).status_code == 200
    # disable it -> no longer admin
    requests.patch(f"{base}/__api/keys/{kid}", json={"enabled": False}, headers=_admin(base), timeout=10)
    assert requests.get(f"{base}/__dashboard", headers=auth(key), timeout=10).status_code == 401
    # the env (implicit) admin key still works
    assert requests.get(f"{base}/__dashboard", headers=_admin(base), timeout=10).status_code == 200


def test_delete_key_removes_auth(proxy_server, upstream):
    """DELETE /__api/keys/<id> permanently removes a key: it stops authenticating and
    disappears from the list, without a restart. Deleting an unknown id 404s."""
    base = proxy_server["base"]
    created = requests.post(f"{base}/__api/keys", json={"name": "gina"}, headers=_admin(base), timeout=10).json()
    kid = created["id"]; key = created["key"]

    upstream.state.hold_event.set()
    assert requests.get(f"{base}/v1/models", headers=auth(key), timeout=10).status_code == 200

    # delete it
    dr = requests.delete(f"{base}/__api/keys/{kid}", headers=_admin(base), timeout=10)
    assert dr.status_code == 200
    assert dr.json()["deleted"] is True

    # now the key is rejected immediately (cache refreshed, no restart)
    assert requests.get(f"{base}/v1/models", headers=auth(key), timeout=10).status_code == 401

    # and it's gone from the list
    names = {k["name"] for k in requests.get(f"{base}/__api/keys", headers=_admin(base), timeout=10).json()["keys"]}
    assert "gina" not in names

    # deleting the same id again 404s
    assert requests.delete(f"{base}/__api/keys/{kid}", headers=_admin(base), timeout=10).status_code == 404


def test_delete_key_requires_admin(proxy_server, upstream):
    """A non-admin key cannot delete keys."""
    base = proxy_server["base"]
    created = requests.post(f"{base}/__api/keys", json={"name": "heidi"}, headers=_admin(base), timeout=10).json()
    assert requests.delete(f"{base}/__api/keys/{created['id']}", headers=auth(), timeout=10).status_code == 401
    # still present (not deleted)
    names = {k["name"] for k in requests.get(f"{base}/__api/keys", headers=_admin(base), timeout=10).json()["keys"]}
    assert "heidi" in names


# ---------------------------------------------------------------------------
# Per-key daily input-token limit
# ---------------------------------------------------------------------------
def test_set_and_clear_daily_input_limit(proxy_server, upstream):
    """PATCH daily_input_limit sets the cap; GET reflects it; null clears (unlimited)."""
    base = proxy_server["base"]
    created = requests.post(f"{base}/__api/keys", json={"name": "jared"}, headers=_admin(base), timeout=10).json()
    kid = created["id"]

    r = requests.patch(f"{base}/__api/keys/{kid}", json={"daily_input_limit": 150_000_000}, headers=_admin(base), timeout=10)
    assert r.status_code == 200
    assert r.json()["daily_input_limit"] == 150_000_000

    # reflected in the list
    keys = {k["name"]: k for k in requests.get(f"{base}/__api/keys", headers=_admin(base), timeout=10).json()["keys"]}
    assert keys["jared"]["daily_input_limit"] == 150_000_000

    # clear back to unlimited
    r = requests.patch(f"{base}/__api/keys/{kid}", json={"daily_input_limit": None}, headers=_admin(base), timeout=10)
    assert r.status_code == 200
    assert r.json()["daily_input_limit"] is None


def test_daily_input_limit_rejects_at_cap(proxy_server, upstream):
    """A key at its daily input cap is rejected 429 with type 'daily_input_limit'."""
    base = proxy_server["base"]
    created = requests.post(f"{base}/__api/keys", json={"name": "kate"}, headers=_admin(base), timeout=10).json()
    kid = created["id"]; key = created["key"]

    # cap at exactly one usage request's input tokens (11 each via X-Want-Usage).
    requests.patch(f"{base}/__api/keys/{kid}", json={"daily_input_limit": 11}, headers=_admin(base), timeout=10)

    upstream.state.hold_event.set()
    upstream.state.received.clear()
    # first request uses 11 input tokens -> reaches the cap
    assert requests.get(f"{base}/v1/models", headers=auth(key) | {"X-Want-Usage": "1"}, timeout=30).status_code == 200
    assert len(upstream.state.received) == 1

    # second request is now at/over the cap -> rejected locally, nothing upstream
    r2 = requests.get(f"{base}/v1/models", headers=auth(key) | {"X-Want-Usage": "1"}, timeout=10)
    assert r2.status_code == 429
    assert r2.json()["error"]["type"] == "daily_input_limit"
    assert len(upstream.state.received) == 1  # nothing extra reached upstream


def test_daily_input_limit_zero_blocks_immediately(proxy_server, upstream):
    """A cap of 0 means no input tokens allowed at all -> first request is rejected."""
    base = proxy_server["base"]
    created = requests.post(f"{base}/__api/keys", json={"name": "leo"}, headers=_admin(base), timeout=10).json()
    kid = created["id"]; key = created["key"]
    requests.patch(f"{base}/__api/keys/{kid}", json={"daily_input_limit": 0}, headers=_admin(base), timeout=10)

    upstream.state.hold_event.set()
    upstream.state.received.clear()
    r = requests.get(f"{base}/v1/models", headers=auth(key), timeout=10)
    assert r.status_code == 429
    assert r.json()["error"]["type"] == "daily_input_limit"
    assert upstream.state.received == []  # nothing reached upstream


def test_daily_input_limit_rejects_bad_values(proxy_server, upstream):
    """PATCH rejects negative or non-int caps."""
    base = proxy_server["base"]
    created = requests.post(f"{base}/__api/keys", json={"name": "mia"}, headers=_admin(base), timeout=10).json()
    kid = created["id"]
    assert requests.patch(f"{base}/__api/keys/{kid}", json={"daily_input_limit": -1}, headers=_admin(base), timeout=10).status_code == 400
    assert requests.patch(f"{base}/__api/keys/{kid}", json={"daily_input_limit": "lots"}, headers=_admin(base), timeout=10).status_code == 400
    assert requests.patch(f"{base}/__api/keys/{kid}", json={"daily_input_limit": True}, headers=_admin(base), timeout=10).status_code == 400


# ---------------------------------------------------------------------------
# Key attribution — each request records WHICH key was used
# ---------------------------------------------------------------------------
def test_request_records_key_name(proxy_server, upstream):
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/models", headers=auth(TEST_MOBIN_KEY), timeout=10)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["key_name"] == "mobin"


def test_request_records_created_key_name(proxy_server, upstream):
    base = proxy_server["base"]
    created = requests.post(f"{base}/__api/keys", json={"name": "frank"}, headers=_admin(base), timeout=10).json()
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/models", headers=auth(created["key"]), timeout=10)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["key_name"] == "frank"


def test_request_records_hermes_key_name(proxy_server, upstream):
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/models", headers=auth("local-hermes-upstream-key"), timeout=10)
    rows = wait_for_rows(proxy_server["db"], 1)
    assert rows[0]["key_name"] == "hermes"


def test_api_includes_key_name(proxy_server, upstream):
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    requests.get(f"{base}/v1/models", headers=auth(TEST_NIRJHOR_KEY), timeout=10)
    wait_for_rows(proxy_server["db"], 1)
    body = requests.get(f"{base}/__api/requests?limit=5", headers=_admin(base), timeout=10).json()
    assert body["requests"][0]["key_name"] == "nirjhor"


# ---------------------------------------------------------------------------
# Upstream 429 -> bounded retry with backoff (transient rate-limit drain)
# ---------------------------------------------------------------------------
def _fast_retries(monkeypatch):
    """Zero out the retry backoff so tests don't sleep."""
    monkeypatch.setattr(proxy, "RETRY_BASE_DELAY_S", 0.0)
    monkeypatch.setattr(proxy, "RETRY_AFTER_CAP_S", 0.0)


def test_transient_upstream_429_retries_then_succeeds(proxy_server, upstream, monkeypatch):
    """A single transient upstream 429 must not fail the client: retry succeeds."""
    _fast_retries(monkeypatch)
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    upstream.state.burst_429s = 1          # first attempt 429, second attempt OK
    r = requests.post(f"{base}/v1/chat/completions", headers=auth(),
                      json={"model": "deepseek-v4-flash", "messages": [{}, {}]}, timeout=20)
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is True
    # the upstream saw both the 429 attempt and the retried success
    assert sum(1 for x in upstream.state.received
               if x["path"] == "/v1/chat/completions") >= 2


def test_transient_upstream_429_respects_retry_after_and_caps_backoff(proxy_server, upstream, monkeypatch):
    """Upstream Retry-After is honored but capped by RETRY_AFTER_CAP_S."""
    _fast_retries(monkeypatch)
    base = proxy_server["base"]
    # a huge Retry-After must be clamped down to RETRY_AFTER_CAP_S (0 here) so we
    # don't stall the queue for the cap ceiling on a transient blip.
    upstream.state.hold_event.set()
    upstream.state.retry_after = "3600"
    monkeypatch.setattr(proxy, "RETRY_AFTER_CAP_S", 0.0)
    upstream.state.burst_429s = 1
    r = requests.post(f"{base}/v1/chat/completions", headers=auth(),
                      json={"model": "deepseek-v4-flash", "messages": [{}, {}]}, timeout=20)
    assert r.status_code == 200, r.text


def test_persistent_upstream_429_is_forwarded_after_exhausting_retries(proxy_server, upstream, monkeypatch):
    """If upstream keeps 429ing, the client still gets a 429 (deduped, no worse)."""
    _fast_retries(monkeypatch)
    base = proxy_server["base"]
    upstream.state.hold_event.set()
    upstream.state.burst_429s = 10         # exhaust all retries too
    r = requests.post(f"{base}/v1/chat/completions", headers=auth(),
                      json={"model": "deepseek-v4-flash", "messages": [{}, {}]}, timeout=20)
    assert r.status_code == 429
    # the upstream was attempted 1 + UPSTREAM_429_RETRIES times (all 429)
    attempts = sum(1 for x in upstream.state.received if x["path"] == "/v1/chat/completions")
    assert attempts == 1 + proxy.UPSTREAM_429_RETRIES