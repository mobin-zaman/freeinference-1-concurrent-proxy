"""Reverse proxy for the ngrok public surface.

Listens on 127.0.0.1:8791 (ngrok static domain -> this port) and forwards ONLY
the OpenAI-compatible proxy API (/v1/*) to the localhost freeinference proxy at
127.0.0.1:8788, passing the caller's Authorization header straight through to
upstream — the caller must hold a valid API key; no credential is embedded in
the public surface. Everything else (/__dashboard, /__api, /favicon.ico)
returns 404 so no internal dashboard/API ever leaks onto the public static
domain. Uses requests to stream SSE responses (iter_content), same pattern as
the 8788 proxy — the previous stdlib raw-socket relay (resp.fp.raw._sock)
bypassed urllib's internal read buffer and stalled/truncated mid-stream.
"""
import http.server
import socketserver
import requests

TARGET = ("127.0.0.1", 8788)  # freeinference proxy (dashboard + api + v1)
LISTEN_PORT = 8791
ALLOW_PREFIX = "/v1/"  # OpenAI-compatible proxy API only
STREAM_TIMEOUT = 900   # per-read idle timeout while relaying SSE (matches proxy READ_TIMEOUT)

# Hop-by-hop headers must not be forwarded in either direction.
_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-length",
}


class ProxyHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if not self.path.startswith(ALLOW_PREFIX):
            self.reject()
            return
        self.forward("GET")

    def do_POST(self):
        if not self.path.startswith(ALLOW_PREFIX):
            self.reject()
            return
        self.forward("POST")

    def do_PUT(self):
        if not self.path.startswith(ALLOW_PREFIX):
            self.reject()
            return
        self.forward("PUT")

    def do_DELETE(self):
        if not self.path.startswith(ALLOW_PREFIX):
            self.reject()
            return
        self.forward("DELETE")

    def do_PATCH(self):
        if not self.path.startswith(ALLOW_PREFIX):
            self.reject()
            return
        self.forward("PATCH")

    def reject(self):
        body = b"not found"
        self.send_response(404)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def forward(self, method):
        data = None
        if method in ("POST", "PUT", "PATCH"):
            length = int(self.headers.get("Content-Length") or 0)
            data = self.rfile.read(length) if length else None
        fwd_headers = {k: v for k, v in self.headers.items()
                       if k.lower() not in _HOP_BY_HOP}
        fwd_headers["Host"] = "127.0.0.1:8788"
        url = "http://%s:%d%s" % (TARGET[0], TARGET[1], self.path)
        try:
            upstream = requests.request(
                method, url, headers=fwd_headers, data=data,
                stream=True, timeout=(30, STREAM_TIMEOUT),
            )
        except requests.RequestException as exc:
            self._error(502, "upstream connection failed: %s" % exc)
            return

        ctype = upstream.headers.get("Content-Type", "")
        is_stream = "text/event-stream" in ctype

        self.send_response(upstream.status_code)
        for key, value in upstream.headers.items():
            if key.lower() in _HOP_BY_HOP:
                continue
            self.send_header(key, value)

        try:
            if is_stream:
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                for chunk in upstream.iter_content(chunk_size=4096):
                    if not chunk:
                        continue
                    self.wfile.write(b"%x\r\n" % len(chunk) + chunk + b"\r\n")
                self.wfile.write(b"0\r\n\r\n")  # terminate the chunked stream
            else:
                body = upstream.content
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        except requests.RequestException:
            # Upstream broke mid-stream (client may also have gone away).
            log("upstream_stream_error %s %s" % (method, self.path))
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass  # client closed; nothing more to write

    def _error(self, status, message):
        body = message.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def log(msg: str) -> None:
    import time
    line = "%s %s" % (time.strftime("%Y-%m-%dT%H:%M:%S"), msg)
    try:
        with open("/home/hermes/.hermes/logs/freeinference-ngrok-proxy.log", "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


class Threaded(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    srv = Threaded(("127.0.0.1", LISTEN_PORT), ProxyHandler)
    print("ngrok proxy listening on 127.0.0.1:%d -> %s:%d (only %s*)" % (LISTEN_PORT, *TARGET, ALLOW_PREFIX), flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()