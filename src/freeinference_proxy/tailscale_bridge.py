"""Optional reverse proxy: expose only the freeinference dashboard
(http://127.0.0.1:8788/__dashboard) on a second interface:port, so the
dashboard is reachable on a network host (e.g. a Tailscale IP) without
exposing the raw localhost-only proxy API on that network.

The proxy binds 127.0.0.1 only by default, so it cannot listen on a tailnet
IP directly. This bridge forwards the /__dashboard path (and its
/__api/requests fetch) from the chosen interface to the localhost process.
Pure stdlib, no third-party deps. Not required for the core proxy to work.
"""
import argparse
import http.server
import socket
import socketserver
import urllib.request

DEFAULT_TARGET_HOST = "127.0.0.1"
DEFAULT_TARGET_PORT = 8788      # freeinference proxy (dashboard + api)
DEFAULT_BROKER_HOST = "0.0.0.0"  # override with your Tailscale IP
DEFAULT_BROKER_PORT = 8789


class ProxyHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    TARGET = (DEFAULT_TARGET_HOST, DEFAULT_TARGET_PORT)

    def do_GET(self):
        try:
            req = urllib.request.Request(
                "http://%s:%d%s" % (self.TARGET[0], self.TARGET[1], self.path),
                headers=dict(self.headers),
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                ctype = resp.headers.get("Content-Type", "")
                headers = [(k, v) for k, v in resp.getheaders()
                           if k.lower() not in ("content-length", "transfer-encoding", "connection")]
                self.send_response(resp.status)
                for k, v in headers:
                    self.send_header(k, v)
                if "text/event-stream" in ctype:
                    # SSE: stream from the raw socket (sock.recv returns available
                    # bytes immediately). fp.read(4096) on a blocking socket waits
                    # for the FULL 4096 bytes — a drip-fed SSE handshake/event
                    # stalls forever. recv(4096) returns whatever arrived.
                    self.send_header("Connection", "keep-alive")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    raw = resp.fp.raw
                    sock = raw._sock  # the real socket underneath SocketIO/BufferedReader
                    sock.settimeout(30)
                    while True:
                        try:
                            chunk = sock.recv(4096)
                        except socket.timeout:
                            continue
                        if not chunk:
                            break
                        try:
                            self.wfile.write(chunk)
                            self.wfile.flush()
                        except (BrokenPipeError, ConnectionResetError):
                            return
                else:
                    body = resp.read()
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