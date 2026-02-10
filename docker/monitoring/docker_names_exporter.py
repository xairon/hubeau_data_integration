"""Tiny Prometheus exporter that maps Docker container IDs to names.

Queries the Docker API via Unix socket every 30s and exposes a
container_info metric with container_id and name labels.
Works with rootless Docker (unlike cAdvisor which requires containerd).

Metric exposed:
  container_info{container_id="6cfc0f0a939d", name="brgm-postgres"} 1
"""

import http.server
import json
import socket
import threading
import time

SOCKET_PATH = "/var/run/docker.sock"
metrics = "# Waiting for first scrape...\n"


def fetch_containers():
    global metrics
    while True:
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(SOCKET_PATH)
            sock.sendall(b"GET /containers/json HTTP/1.0\r\nHost: localhost\r\n\r\n")
            response = b""
            while True:
                data = sock.recv(4096)
                if not data:
                    break
                response += data
            sock.close()

            body = response.split(b"\r\n\r\n", 1)[1]
            containers = json.loads(body)

            lines = [
                "# HELP container_info Maps Docker container IDs to human-readable names",
                "# TYPE container_info gauge",
            ]
            for c in containers:
                cid = c["Id"][:12]
                name = c["Names"][0].lstrip("/")
                lines.append(f'container_info{{container_id="{cid}",name="{name}"}} 1')

            metrics = "\n".join(lines) + "\n"
        except Exception as e:
            metrics = f"# Error fetching containers: {e}\n"

        time.sleep(30)


class MetricsHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(metrics.encode())

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    threading.Thread(target=fetch_containers, daemon=True).start()
    print("docker-names-exporter listening on :9200")
    http.server.HTTPServer(("0.0.0.0", 9200), MetricsHandler).serve_forever()
