"""
HTTP Server pour exposer les métriques Prometheus
Lance un serveur HTTP léger sur le port 9091 pour le scraping Prometheus
"""
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, CollectorRegistry
from prometheus_client import multiprocess


class MetricsHandler(BaseHTTPRequestHandler):
    """Handler HTTP pour l'endpoint /metrics"""

    def do_GET(self):
        """Handle GET requests"""
        if self.path == '/metrics':
            try:
                # Get multiprocess registry if configured
                if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
                    registry = CollectorRegistry()
                    multiprocess.MultiProcessCollector(registry)
                else:
                    from prometheus_client import REGISTRY
                    registry = REGISTRY

                # Generate metrics output
                metrics_output = generate_latest(registry)

                # Send response
                self.send_response(200)
                self.send_header('Content-Type', CONTENT_TYPE_LATEST)
                self.end_headers()
                self.wfile.write(metrics_output)

            except Exception as e:
                self.send_error(500, f"Error generating metrics: {str(e)}")

        elif self.path == '/health':
            # Simple healthcheck
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')

        else:
            self.send_error(404, "Not Found")

    def log_message(self, format, *args):
        """Suppress default logging (too verbose)"""
        pass


def start_metrics_server(port: int = 9091, host: str = "0.0.0.0"):
    """
    Start HTTP server for Prometheus metrics in background thread

    Args:
        port: Port to listen on (default: 9091)
        host: Host to bind to (default: 0.0.0.0 for all interfaces)
    """
    server = HTTPServer((host, port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"✅ Metrics server started on http://{host}:{port}/metrics")
    return server


# Auto-start si PROMETHEUS_MULTIPROC_DIR est défini
if __name__ == "__main__":
    start_metrics_server()
    print("Metrics server running. Press Ctrl+C to exit.")
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down metrics server...")
