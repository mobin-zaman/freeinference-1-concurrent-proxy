"""Optional reverse proxy: expose only the freeinference dashboard
(http://127.0.0.1:8788/__dashboard) on a second interface:port, so the
dashboard is reachable on a network host (e.g. a Tailscale IP) without
exposing the raw localhost-only proxy API on that network.

The proxy binds 127.0.0.1 only by default, so it cannot listen on a tailnet
IP directly. This bridge forwards the /__dashboard path (and its
/__api/requests fetch) from the chosen interface to the localhost process.
Uses requests (already a dependency of the core proxy) for streaming.
Not required for the core proxy to work.
"""
import argparse
import http.server
import socketserver

import requests

DEFAULT_TARGET_HOST = "127.0.0.1"
DEFAULT_TARGET_PORT = 8788      # freeinference proxy (dashboard + api)
DEFAULT_BROKER_HOST = "0.0.0.0"  # override with your Tailscale IP
DEFAULT_BROKER_PORT = 8789


class ProxyHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    TARGET = (DEFAULT_TARGET_HOST, DEFAULT_TARGET_PORT)

    def do_GET(self):
        try:
            headers = {k: v for k, v in dict(self.headers).items()
                       if k.lower() not in ("content-length", "transfer-encoding", "connection", "host")}
            upstream = requests.request(
                "GET",
                "http://%s:%d%s" % (self.TARGET[0], self.TARGET[1], self.path),
                headers=headers, stream=True, timeout=60,
            )
            ctype = upstream.headers.get("Content-Type", "")
            self.send_response(upstream.status_code)
            for k, v in upstream.headers.items():
                if k.lower() not in ("content-length", "transfer-encoding", "connection"):
                    self.send_header(k, v)
            if "text/event-stream" in ctype:
                # SSE: stream via iter_content (transparent chunked de-framing).
                # The previous resp.fp.raw._sock recv() bypassed urllib's read
                # buffer and stalled/truncated when data sat in the internal
                # buffer instead of on the wire.
                self.send_header("Connection", "keep-alive")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                for chunk in upstream.iter_content(chunk_size=4096):
                    if not chunk:
                        continue
                    self.wfile.write(chunk)
                    self.wfile.flush()
            else:
                body = upstream.content
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        except Exception as exc:
            body = ("proxy error: %s" % exc).encode()
            self.send_response(502)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, *a):
        pass


class Threaded(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    parser = argparse.ArgumentParser(description="Tailscale/network bridge for the freeinference dashboard")
    parser.add_argument("--listen-host", default=DEFAULT_BROKER_HOST,
                        help="interface to listen on (default 0.0.0.0; set to your Tailscale IP for the tailnet)")
    parser.add_argument("--port", type=int, default=DEFAULT_BROKER_PORT, help="default 8789")
    parser.add_argument("--target-host", default=DEFAULT_TARGET_HOST, help="default 127.0.0.1")
    parser.add_argument("--target-port", type=int, default=DEFAULT_TARGET_PORT,
                        help="the freeinference proxy port (default 8788)")
    args = parser.parse_args()

    ProxyHandler.TARGET = (args.target_host, args.target_port)
    srv = Threaded((args.listen_host, args.port), ProxyHandler)
    print("bridge listening on %s:%d -> %s:%d"
          % (args.listen_host, args.port, args.target_host, args.target_port), flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()