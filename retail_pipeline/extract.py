from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

from retail_pipeline.config import source_base_url


def fetch_collection(endpoint: str, timeout: int = 30) -> list[dict[str, Any]]:
    """Fetch every page of a DummyJSON-style collection."""
    url = f"{source_base_url()}/{endpoint.lstrip('/')}"
    limit, skip, rows = 100, 0, []
    while True:
        response = requests.get(url, params={"limit": limit, "skip": skip}, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        page = payload.get(endpoint.rstrip("s"), payload.get("carts" if endpoint == "carts" else "users", []))
        rows.extend(page)
        total = int(payload.get("total", len(rows)))
        if not page or len(rows) >= total:
            break
        skip += len(page)
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, separators=(",", ":"), default=str) + "\n")


def extract_run(run_date: str, high_water_mark: int, bronze_root: Path) -> dict[str, Any]:
    """Write orders above the persisted ID watermark and a complete customer snapshot."""
    all_orders = fetch_collection("carts")
    customers = fetch_collection("users")
    run_dir = bronze_root / run_date
    orders_path = run_dir / "orders.jsonl"
    # Keep the original extraction for a logical date so rerunning/backfilling it
    # cannot make already loaded orders disappear after the global watermark moved.
    if orders_path.exists():
        with orders_path.open(encoding="utf-8") as stream:
            orders = [json.loads(line) for line in stream if line.strip()]
    else:
        orders = [row for row in all_orders if int(row.get("id", 0)) > high_water_mark]
        for row in orders:
            row.setdefault("date", run_date)
        _write_jsonl(orders_path, orders)
    _write_jsonl(run_dir / "customers.jsonl", customers)
    return {
        "run_date": run_date,
        "orders_path": str(orders_path),
        "customers_path": str(run_dir / "customers.jsonl"),
        "orders_extracted": len(orders),
        "customers_extracted": len(customers),
        "max_order_id": max((int(row["id"]) for row in orders), default=high_water_mark),
    }

