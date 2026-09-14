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
import hmac
import json
import os
import random
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
UPSTREAM_429_RETRIES = 2    # extra attempts after an upstream 429 (transient rate limit)
RETRY_AFTER_CAP_S = 5       # cap on honored Retry-After seconds
RETRY_BASE_DELAY_S = 1.0    # fallback delay between upstream-429 retries
DATA_DIR_DEFAULT = "~/.local/share/freeinference-1-concurrent-proxy"

# Hop-by-hop headers must not be forwarded in either direction.
_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-encoding",
}

_gate = threading.BoundedSemaphore(GATE_LIMIT)


def _parse_retry_after(value: str):
    """Parse a Retry-After header into seconds (float) or None if absent/invalid."""
    if not value:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None  # HTTP-date form unsupported; fall back to the fixed delay

# Authorization enforced since the proxy can be exposed beyond localhost.
# secrets live in the environment, never in source:
#   FIF_AUTH_KEY_MOBIN          -> the "mobin" role key (sent as Bearer)
#   FIF_AUTH_KEY_NIRJHOR        -> the "nirjhor" role key
#   FIF_AUTH_ADMIN_KEY          -> admin key for /__dashboard and /__api/*
#   FREEINFERENCE_API_KEY       -> also accepted so the current Hermes provider
#                                  (which already injects this Bearer key) keeps
#                                  working with zero config change.
_KEYS: "dict[str, str]" = {}       # valid LLM secret -> key name (raw keys, enabled only)
_ADMIN_KEYS: "dict[str, str]" = {}  # valid admin secret -> key name
_KEY_LIMITS: "dict[str, int]" = {}  # key name -> daily input-token cap (0/absent = unlimited)
_UPSTREAM_KEY: str = ""            # real freeinference credential, injected upstream
_ENV_ADMIN_KEYS: "tuple[str, ...]" = ()  # master admin keys from env, never in DB

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
        " key_name TEXT NOT NULL DEFAULT '',"
        " input_tokens INTEGER NOT NULL DEFAULT 0,"
        " output_tokens INTEGER NOT NULL DEFAULT 0)"
    )
    # In-place migration for DBs created before token columns existed.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(requests)")}
    if "input_tokens" not in cols:
        conn.execute("ALTER TABLE requests ADD COLUMN input_tokens INTEGER NOT NULL DEFAULT 0")
    if "output_tokens" not in cols:
        conn.execute("ALTER TABLE requests ADD COLUMN output_tokens INTEGER NOT NULL DEFAULT 0")
    if "key_name" not in cols:
        conn.execute("ALTER TABLE requests ADD COLUMN key_name TEXT NOT NULL DEFAULT ''")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS api_keys ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " name TEXT NOT NULL UNIQUE,"
        " key_hash TEXT NOT NULL UNIQUE,"
        " role TEXT NOT NULL DEFAULT 'llm',"
        " enabled INTEGER NOT NULL DEFAULT 1,"
        " created_at REAL NOT NULL,"
        " last_used_at REAL)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_at ON requests(at DESC)")
    # Per-key daily input-token cap (NULL / absent = unlimited). Configurable via
    # the dashboard; enforced in the proxy request path before hitting upstream.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(api_keys)")}
    if "daily_input_limit" not in cols:
        conn.execute("ALTER TABLE api_keys ADD COLUMN daily_input_limit INTEGER")
    return conn


def _hash_key(key: str) -> str:
    """SHA-256 of a key. Plaintext keys are never stored at rest."""
    import hashlib
    return hashlib.sha256(key.encode()).hexdigest()


def _refresh_key_cache() -> None:
    """Rebuild the in-memory auth caches from the DB (enabled keys) plus the
    implicit keys that are never stored in the DB (the env admin key and the
    upstream/Hermes key). Called at startup and after every mutation so an
    enable/disable/create takes effect without a restart.

    Every entry in the caches is the SHA-256 of a key (never a raw key).
    The hot path hashes the presented key and compares hashes in constant time."""
    global _KEYS, _ADMIN_KEYS, _KEY_LIMITS
    keys, admins, limits = {}, {}, {}
    # Implicit LLM key: the upstream/FREEINFERENCE credential so the current
    # Hermes provider keeps working; can never be disabled from the dashboard.
    if _UPSTREAM_KEY:
        keys[_hash_key(_UPSTREAM_KEY)] = "hermes"
    # Implicit admin key from the env — the master, always accepted.
    for a in _ENV_ADMIN_KEYS:
        admins[_hash_key(a)] = "admin"
    with _db_lock:
        conn = _db()
        try:
            for r in conn.execute(
                    "SELECT name, key_hash, role, enabled, daily_input_limit"
                    " FROM api_keys WHERE enabled=1"):
                h = r["key_hash"]  # stored as a SHA-256 already
                if r["role"] == "admin":
                    admins.setdefault(h, r["name"])
                keys.setdefault(h, r["name"])
                if r["daily_input_limit"] is not None:
                    limits[r["name"]] = int(r["daily_input_limit"])
        finally:
            conn.close()
    _KEYS = keys
    _ADMIN_KEYS = admins
    _KEY_LIMITS = limits
    # Every mutation that changes a cap rebuilds the cache; enforcement reads
    # only the in-memory dict, never the DB, on the hot path.


def _daily_midnight_epoch() -> float:
    """Epoch seconds of the start of the current quota day.

    freeinference.org resets its daily quota at 06:00 GMT+6 (== 00:00 UTC), so
    the quota day boundary is UTC midnight, not local-server midnight. Using
    UTC keeps 'today' usage and the input-token cap aligned with the provider's
    own reset regardless of this host's timezone."""
    return int(time.time() // 86400 * 86400)


def _daily_input_used(key_name: str) -> int:
    """Input tokens logged today for this key (post-execution totals from the
    request log). Used by the per-key daily input-token limiter."""
    start = _daily_midnight_epoch()
    with _db_lock:
        conn = _db()
        try:
            row = conn.execute(
                "SELECT COALESCE(SUM(input_tokens),0) FROM requests"
                " WHERE key_name=? AND at>=?", (key_name, start)).fetchone()
            return int(row[0])
        finally:
            conn.close()


def record_request(method, path, status, waited_s, dur_s, ua, key_name="",
                   input_tokens=0, output_tokens=0) -> None:
    with _db_lock:
        conn = _db()
        try:
            conn.execute(
                "INSERT INTO requests (at, ts, method, path, status, waited_s, dur_s,"
                " user_agent, key_name, input_tokens, output_tokens)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (time.time(), time.strftime("%Y-%m-%d %H:%M:%S"),
                 method, path, status, waited_s, dur_s, ua, key_name or "",
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
        # Whole-body JSON (non-streaming). Guard it: a body cut mid-object by
        # an upstream interruption would otherwise crash the whole thread.
        try:
            candidates.append(json.loads(text))
        except (json.JSONDecodeError, ValueError):
            return 0, 0
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

    def handle(self):
        # Clients (proxies, aborted browser fetches) routinely close the
        # keep-alive connection mid-read, which makes BaseHTTPRequestHandler's
        # built-in handle_one_request raise ConnectionResetError. That is a
        # normal HTTP/1.1 lifecycle event, not a fault: catch it here so the
        # request thread exits cleanly instead of dumping a traceback and
        # churning socket threads. TimeoutError (slow/abandoned client) is
        # likewise benign.
        try:
            super().handle()
        except (ConnectionResetError, BrokenPipeError, TimeoutError, ValueError):
            self.close_connection = True

    def _reply_json(self, status: int, payload: dict, retry_after: int = 5) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if status == 429:
            self.send_header("Retry-After", str(retry_after))
        self.end_headers()
        self.wfile.write(body)

    def _reject_auth(self) -> None:
        """Respond 401 without ever revealing WHICH key is valid or expected."""
        self.send_response(401)
        body = json.dumps({"error": {
            "message": "missing or invalid API key",
            "type": "local_proxy_unauthorized",
            "code": 401,
        }}).encode()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("WWW-Authenticate", 'Bearer realm="freeinference-proxy"')
        self.end_headers()
        self.wfile.write(body)

    def _client_key(self) -> str:
        """Extract the presented API key from header, lower-cased-scheme-aware.
        Returns '' if absent. A key in the URL query string is ignored on
        purpose: it'd leak into access logs and proxy history."""
        authz = self.headers.get("Authorization", "")
        if authz.lower().startswith("bearer "):
            return authz[7:].strip()
        x = self.headers.get("X-Api-Key", "")
        if x:
            return x.strip()
        return ""

    def _key_name(self) -> str:
        """Return the name of the authenticated LLM key, or '' if invalid."""
        key = self._client_key()
        if not key:
            return ""
        presented = _hash_key(key)
        # constant-time over the whole set so timing doesn't leak membership.
        for stored, name in _KEYS.items():
            if hmac.compare_digest(presented, stored):
                return name
        return ""

    def _authenticated(self) -> bool:
        return bool(self._key_name())

    def _admin_name(self) -> str:
        key = self._client_key()
        if not key:
            return ""
        presented = _hash_key(key)
        for stored, name in _ADMIN_KEYS.items():
            if hmac.compare_digest(presented, stored):
                return name
        return ""

    def _is_admin(self) -> bool:
        return bool(self._admin_name())

    # --- API-key management (admin-only, never counted against the gate) ----
    def _handle_keys_admin(self) -> bool:
        """Serve the /__api/keys manage surface. Returns True if handled."""
        if not self.path.startswith("/__api/keys"):
            return False
        if not self._is_admin():
            self._reject_auth()
            return True
        base = self.path
        # PATCH /__api/keys/<id>  {enabled?: bool, daily_input_limit?: int|null}
        if self.command == "PATCH":
            seg = base.removesuffix("/").split("/")
            if len(seg) == 4:
                try:
                    kid = int(seg[3])
                except ValueError:
                    self._reply_json(400, {"error": {"message": "bad key id", "code": 400}})
                    return True
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}") if length else {}
                sets, params = [], []
                if "enabled" in body:
                    enable = body["enabled"]
                    if not isinstance(enable, bool):
                        self._reply_json(400, {"error": {"message": "enabled must be bool", "code": 400}})
                        return True
                    sets.append("enabled=?")
                    params.append(1 if enable else 0)
                if "daily_input_limit" in body:
                    lim = body["daily_input_limit"]
                    if lim is not None and (not isinstance(lim, int) or isinstance(lim, bool) or lim < 0):
                        self._reply_json(400, {"error": {"message": "daily_input_limit must be a non-negative int or null", "code": 400}})
                        return True
                    sets.append("daily_input_limit=?")
                    params.append(lim)
                if not sets:
                    self._reply_json(400, {"error": {"message": "nothing to update (send enabled and/or daily_input_limit)", "code": 400}})
                    return True
                params.append(kid)
                with _db_lock:
                    conn = _db()
                    try:
                        cur = conn.execute(
                            "UPDATE api_keys SET " + ", ".join(sets) +
                            " WHERE id=? RETURNING name, role, enabled, daily_input_limit",
                            params)
                        row = cur.fetchone()
                        conn.commit()
                    finally:
                        conn.close()
                if row is None:
                    self._reply_json(404, {"error": {"message": "no such key", "code": 404}})
                    return True
                _refresh_key_cache()
                self._reply_json(200, {"id": kid, "name": row["name"], "role": row["role"],
                                       "enabled": bool(row["enabled"]),
                                       "daily_input_limit": row["daily_input_limit"]})
                return True
        # DELETE /__api/keys/<id>  -> 204 (removes the key from the DB + auth cache)
        if self.command == "DELETE":
            seg = base.removesuffix("/").split("/")
            if len(seg) == 4:
                try:
                    kid = int(seg[3])
                except ValueError:
                    self._reply_json(400, {"error": {"message": "bad key id", "code": 400}})
                    return True
                with _db_lock:
                    conn = _db()
                    try:
                        cur = conn.execute("DELETE FROM api_keys WHERE id=? RETURNING name, role",
                                           (kid,))
                        row = cur.fetchone()
                        conn.commit()
                    finally:
                        conn.close()
                if row is None:
                    self._reply_json(404, {"error": {"message": "no such key", "code": 404}})
                    return True
                _refresh_key_cache()
                self._reply_json(200, {"id": kid, "name": row["name"], "role": row["role"],
                                       "deleted": True})
                return True
        # POST /__api/keys  {name, role?} -> 201 {id,name,role,key}
        if self.command == "POST":
            length = int(self.headers.get("Content-Length") or 0)
            try:
                body = json.loads(self.rfile.read(length) or b"{}") if length else {}
            except (json.JSONDecodeError, ValueError):
                self._reply_json(400, {"error": {"message": "invalid JSON", "code": 400}})
                return True
            name = (body.get("name") or "").strip()
            role = (body.get("role") or "llm").strip().lower()
            if not name:
                self._reply_json(400, {"error": {"message": "name required", "code": 400}})
                return True
            if role not in ("llm", "admin"):
                self._reply_json(400, {"error": {"message": "role must be 'llm' or 'admin'", "code": 400}})
                return True
            import secrets
            raw = "sk-" + secrets.token_urlsafe(32)
            t = time.time()
            with _db_lock:
                conn = _db()
                try:
                    try:
                        cur = conn.execute(
                            "INSERT INTO api_keys (name, key_hash, role, enabled, created_at)"
                            " VALUES (?,?,?,1,?) RETURNING id", (name, _hash_key(raw), role, t))
                        kid = cur.fetchone()["id"]
                        conn.commit()
                    except sqlite3.IntegrityError:
                        self._reply_json(409, {"error": {"message": "name already exists", "code": 409}})
                        return True
                finally:
                    conn.close()
            _refresh_key_cache()
            self._reply_json(201, {"id": kid, "name": name, "role": role, "key": raw})
            return True
        # GET /__api/keys -> list (masked)
        if self.command == "GET":
            rows = []
            # Time window: today / 7d / 30d / all (default all) — mirrors the
            # /__api/requests handler so per-key token totals follow the filter.
            rng = "all"
            if "range=" in self.path:
                cand = self.path.split("range=")[-1].split("&")[0].strip().lower()
                if cand in ("today", "7d", "30d", "all"):
                    rng = cand
            now = time.time()
            if rng == "today":
                start = _daily_midnight_epoch()
                rngclause, rngpar = " AND at >= ?", [start]
            elif rng == "7d":
                rngclause, rngpar = " AND at >= ?", [now - 7 * 86400]
            elif rng == "30d":
                rngclause, rngpar = " AND at >= ?", [now - 30 * 86400]
            else:
                rngclause, rngpar = "", []
            with _db_lock:
                conn = _db()
                try:
                    # Per-key token totals from the request log within the window.
                    # Keyed by key_name so implicit keys (e.g. 'hermes') are
                    # included even though they're not in the api_keys table.
                    tok = {}
                    for r in conn.execute(
                            "SELECT key_name, COALESCE(SUM(input_tokens),0) AS it,"
                            " COALESCE(SUM(output_tokens),0) AS ot"
                            " FROM requests WHERE key_name != ''" + rngclause +
                            " GROUP BY key_name", rngpar):
                        tok[r["key_name"]] = (r["it"], r["ot"])
                    for r in conn.execute(
                            "SELECT id, name, role, enabled, created_at, last_used_at, key_hash,"
                            " daily_input_limit FROM api_keys ORDER BY id"):
                        h = r["key_hash"]
                        it, ot = tok.get(r["name"], (0, 0))
                        rows.append({
                            "id": r["id"], "name": r["name"], "role": r["role"],
                            "enabled": bool(r["enabled"]), "created_at": r["created_at"],
                            "last_used_at": r["last_used_at"],
                            "key_last4": h[-4:],
                            "masked": "sk-…" + h[-4:],
                            "input_tokens": int(it), "output_tokens": int(ot),
                            "daily_input_limit": r["daily_input_limit"],
                        })
                finally:
                    conn.close()
            self._reply_json(200, {"keys": rows})
            return True
        # Unsupported method on /__api/keys
        self._reply_json(405, {"error": {"message": "method not allowed", "code": 405}})
        return True

    def _is_public_noise(self) -> bool:
        return self.command == "GET" and self.path in (
            "/favicon.ico", "/robots.txt", "/favicon.png")

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
                try:
                    with _db_lock:
                        conn = _db()
                        try:
                            rows = conn.execute(
                                "SELECT id, at, ts, method, path, status, waited_s, dur_s, user_agent,"
                                " key_name, input_tokens, output_tokens"
                                " FROM requests WHERE id > ? ORDER BY id ASC", (last_id,)).fetchall()
                        finally:
                            conn.close()
                except (sqlite3.Error, OSError):
                    return  # DB unreadable/removed: end the stream, don't spin a thread or spam stderr
                if rows:
                    for r in rows:
                        last_id = max(last_id, r["id"])
                        payload = {
                            "kind": "request", "id": r["id"], "ts": r["ts"],
                            "method": r["method"], "path": r["path"],
                            "status": r["status"], "waited_s": r["waited_s"],
                            "dur_s": r["dur_s"], "user_agent": r["user_agent"],
                            "key_name": r["key_name"],
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
            # Time window: today / 7d / 30d / all (default all).
            rng = "all"
            if "range=" in self.path:
                cand = self.path.split("range=")[-1].split("&")[0].strip().lower()
                if cand in ("today", "7d", "30d", "all"):
                    rng = cand
            now = time.time()
            if rng == "today":
                start = _daily_midnight_epoch()
                clauses, params = ["at >= ?"], [start]
            elif rng == "7d":
                clauses, params = ["at >= ?"], [now - 7 * 86400]
            elif rng == "30d":
                clauses, params = ["at >= ?"], [now - 30 * 86400]
            else:
                clauses, params = [], []
            # App-key/user filter: &key=<name> (or 'all'/'') shows only that key.
            keyf = ""
            if "key=" in self.path:
                cand = self.path.split("key=")[-1].split("&")[0].strip()
                if cand and cand != "all":
                    keyf = cand
            if keyf:
                clauses.append("key_name = ?")
                params.append(keyf)
            where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
            with _db_lock:
                conn = _db()
                rows = conn.execute(
                    f"SELECT id, ts, method, path, status, waited_s, dur_s, user_agent,"
                    f" key_name, input_tokens, output_tokens FROM requests {where}"
                    f" ORDER BY id DESC LIMIT ?", params + [limit]).fetchall()
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
        # --- Authorization gate (proxy may be exposed beyond localhost) ----
        if self._is_public_noise():          # favicon/robots: harmless browser noise
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        is_admin_path = (
            self.path.startswith("/__api/keys")
            or (self.command == "GET"
                and (self.path.startswith("/__dashboard")
                     or self.path.startswith("/__api/")))
        )
        if is_admin_path:
            if not self._is_admin():
                self._reject_auth()
                return
        else:
            if not self._authenticated():
                self._reject_auth()
                return
        if self._handle_keys_admin():
            return
        if self._serve_local():
            return
        key_name = self._key_name()
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        body = self.rfile.read(length) if length else None

        fwd_headers = {}
        for key, value in self.headers.items():
            if key.lower() in _HOP_BY_HOP or key.lower() == "host":
                continue
            if key.lower() == "authorization":
                continue  # never forward the client's proxy key; inject upstream auth
            fwd_headers[key] = value
        fwd_headers["Host"] = "freeinference.org"
        # The proxy owns the upstream credential: it authenticates the client
        # with a proxy-scoped key and injects the REAL upstream key here, so
        # nobody with a mobin/nirjhor key ever sees or spoofs the upstream secret.
        fwd_headers["Authorization"] = f"Bearer {_UPSTREAM_KEY}" if _UPSTREAM_KEY \
            else "Bearer placeholder"
        # Preserve the client's User-Agent exactly. No spoofing or fallback.

        acquired = _gate.acquire(timeout=ACQUIRE_TIMEOUT)
        if not acquired:
            log(json.dumps({
                "event": "gate_timeout", "method": self.command,
                "path": self.path, "waited_s": ACQUIRE_TIMEOUT,
            }))
            record_request(self.command, self.path, 429,
                           ACQUIRE_TIMEOUT, 0, fwd_headers.get("User-Agent", ""),
                           key_name)
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
            # Per-key daily input-token cap: enforced inside the gate critical
            # section so concurrent requests are serialized against a stable
            # "today so far" total (the gate is 1, so no two can admit at once).
            limit = _KEY_LIMITS.get(key_name)
            if limit is not None and _daily_input_used(key_name) >= limit:
                log(json.dumps({
                    "event": "daily_input_limit_hit", "key_name": key_name,
                    "limit": limit, "path": self.path,
                }))
                record_request(self.command, self.path, 429,
                               waited_s, round(time.monotonic() - t0, 3),
                               fwd_headers.get("User-Agent", ""), key_name)
                self._reply_json(429, {
                    "error": {
                        "message": f"daily input-token limit of {limit} reached "
                                   f"for key '{key_name}'",
                        "type": "daily_input_limit", "code": 429,
                    }
                })
                return
            try:
                attempts = 1 + UPSTREAM_429_RETRIES  # first try + bounded retries
                for attempt in range(attempts):
                    upstream = requests.request(
                        self.command,
                        UPSTREAM + self.path,
                        headers=fwd_headers,
                        data=body,
                        stream=True,
                        timeout=READ_TIMEOUT,
                    )
                    if upstream.status_code != 429 or attempt == attempts - 1:
                        break
                    # Transient upstream rate-limit: back off (gate stays held) and
                    # retry so a single blip never 429-cascades every queued client.
                    retry_after = _parse_retry_after(upstream.headers.get("Retry-After", ""))
                    delay = (min(retry_after, RETRY_AFTER_CAP_S)
                             if retry_after is not None
                             else max(RETRY_BASE_DELAY_S + random.random() * 0.5,
                                      RETRY_BASE_DELAY_S))
                    log(json.dumps({
                        "event": "upstream_429_retry", "method": self.command,
                        "path": self.path, "attempt": attempt + 1,
                        "retry_after": retry_after, "delay_s": round(delay, 3),
                    }))
                    time.sleep(max(delay, 0.0))
            except requests.RequestException as exc:
                log(json.dumps({
                    "event": "upstream_error", "method": self.command,
                    "path": self.path, "error": str(exc)[:300],
                }))
                record_request(self.command, self.path, 502,
                               waited_s, round(time.monotonic() - t0, 3),
                               fwd_headers.get("User-Agent", ""), key_name)
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
                "user_agent": ua, "key_name": key_name,
                "input_tokens": in_tok, "output_tokens": out_tok,
            }))
            record_request(self.command, self.path, upstream.status_code,
                           waited_s, round(time.monotonic() - t0, 3), ua,
                           key_name, in_tok, out_tok)
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
    global _KEYS, _ADMIN_KEYS, _UPSTREAM_KEY, _ENV_ADMIN_KEYS

    # --- Credentials from the environment (never in source / args). ---------
    # A proxy key set is required once the proxy is reachable off-loopback.
    mobin = os.environ.get("FIF_AUTH_KEY_MOBIN", "").strip()
    nirjhor = os.environ.get("FIF_AUTH_KEY_NIRJHOR", "").strip()
    admin = os.environ.get("FIF_AUTH_ADMIN_KEY", "").strip()
    upstream_key = os.environ.get("FIF_UPSTREAM_KEY", "").strip() \
        or os.environ.get("FREEINFERENCE_API_KEY", "").strip()
    if admin:
        _ENV_ADMIN_KEYS = (admin,)
    _UPSTREAM_KEY = upstream_key

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

    # Seed the env-defined LLM keys into the DB (idempotent) so they show up in
    # the dashboard as managed keys. They are still usable regardless.
    seed = {}
    if mobin:
        seed[mobin] = "mobin"
    if nirjhor:
        seed[nirjhor] = "nirjhor"
    with _db_lock:
        conn = _db()
        try:
            for raw, name in seed.items():
                try:
                    conn.execute(
                        "INSERT INTO api_keys (name, key_hash, role, enabled, created_at)"
                        " VALUES (?,?,'llm',1,?)",
                        (name, _hash_key(raw), time.time()))
                except sqlite3.IntegrityError:
                    continue  # already seeded
            conn.commit()
        finally:
            conn.close()
    _refresh_key_cache()

    log(json.dumps({"event": "start", "listen": f"{LISTEN_HOST}:{LISTEN_PORT}",
                    "gate_limit": GATE_LIMIT, "upstream": UPSTREAM,
                    "data_dir": DATA_DIR, "keys": sorted(set(_KEYS.values()))}))
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()