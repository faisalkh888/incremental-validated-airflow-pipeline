from __future__ import annotations

import json
import os
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor


ROOT = Path(__file__).resolve().parent
PORT = int(os.getenv("DASHBOARD_PORT", "8050"))
TABLES = ("stg_orders", "core_orders", "stg_customers", "core_customers", "quarantine")


def json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def get_summary() -> dict:
    conn = psycopg2.connect(
        host=os.getenv("PIPELINE_DB_HOST", "localhost"),
        port=int(os.getenv("PIPELINE_DB_PORT", "5432")),
        dbname=os.getenv("PIPELINE_DB_NAME", "retail"),
        user=os.getenv("PIPELINE_DB_USER", "retail"),
        password=os.getenv("PIPELINE_DB_PASSWORD", "retail"),
        connect_timeout=4,
    )
    try:
        counts = {table: 0 for table in TABLES}
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT to_regclass('public.pipeline_runs') IS NOT NULL AS ready")
            schema_ready = bool(cur.fetchone()["ready"])
            if schema_ready:
                cur.execute("""SELECT run_date, orders_extracted, customers_extracted, orders_loaded,
                    customers_loaded, rows_quarantined, duration_seconds, status, completed_at
                    FROM pipeline_runs ORDER BY run_date DESC LIMIT 30""")
                runs = [
                    {key: json_value(value) for key, value in row.items()}
                    for row in cur.fetchall()
                ]
                for table in TABLES:
                    cur.execute(f"SELECT count(*) FROM {table}")
                    counts[table] = cur.fetchone()["count"]
            else:
                runs = []
        return {"connected": True, "schema_ready": schema_ready, "counts": counts, "runs": runs}
    finally:
        conn.close()


class DashboardHandler(BaseHTTPRequestHandler):
    def _send(self, body: bytes, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._send(b"ok", "text/plain; charset=utf-8")
            return
        if self.path == "/api/summary":
            try:
                body = json.dumps(get_summary()).encode("utf-8")
                self._send(body, "application/json; charset=utf-8")
            except psycopg2.Error:
                body = json.dumps({"connected": False, "schema_ready": False, "counts": {}, "runs": []}).encode("utf-8")
                self._send(body, "application/json; charset=utf-8", 503)
            return
        if self.path in ("/", "/index.html"):
            self._send((ROOT / "index.html").read_bytes(), "text/html; charset=utf-8")
            return
        self._send(b"Not found", "text/plain; charset=utf-8", 404)

    def log_message(self, fmt: str, *args) -> None:
        print(f"{self.address_string()} - {fmt % args}")


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), DashboardHandler)
    print(f"Pipeline dashboard listening on :{PORT}", flush=True)
    server.serve_forever()
