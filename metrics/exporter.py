"""
HTTP server that serves the latest metrics.prom file for Prometheus to scrape.
Run with: python -m metrics.exporter
"""
from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

PROM_FILE = Path(__file__).parent / "metrics.prom"
JSON_FILE = Path(__file__).parent / "results.json"
PORT = 8765  # 9090 is used by the Prometheus container


class MetricsHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/metrics":
            self._serve_prometheus()
        elif self.path == "/results":
            self._serve_json()
        elif self.path == "/health":
            self._respond(200, "OK", "text/plain")
        else:
            self._respond(404, "Not found", "text/plain")

    def _serve_prometheus(self):
        if PROM_FILE.exists():
            content = PROM_FILE.read_text()
        else:
            content = "# No metrics yet — run pytest first\n"
        self._respond(200, content, "text/plain; version=0.0.4")

    def _serve_json(self):
        if JSON_FILE.exists():
            content = JSON_FILE.read_text()
            self._respond(200, content, "application/json")
        else:
            self._respond(404, '{"error": "No results yet"}', "application/json")

    def _respond(self, code: int, body: str, content_type: str):
        encoded = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, fmt, *args):
        # suppress default access log noise
        pass


def run(port: int = PORT):
    # Bind to localhost only — not exposed to the network
    server = HTTPServer(("127.0.0.1", port), MetricsHandler)
    print(f"Metrics server running on http://localhost:{port}/metrics")
    print(f"Results JSON at       http://localhost:{port}/results")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
