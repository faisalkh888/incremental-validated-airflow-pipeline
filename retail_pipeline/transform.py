from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


ORDER_COLUMNS = ["order_id", "customer_id", "order_date", "total", "discounted_total", "total_products", "total_quantity"]
CUSTOMER_COLUMNS = ["customer_id", "first_name", "last_name", "email", "phone", "age", "city", "state"]


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return []
    with file_path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def transform_orders(rows: list[dict[str, Any]]) -> pd.DataFrame:
    typed = [{
        "order_id": row.get("id"), "customer_id": row.get("userId"),
        "order_date": pd.to_datetime(row.get("date", "1970-01-01"), errors="coerce", utc=True),
        "total": row.get("total"), "discounted_total": row.get("discountedTotal"),
        "total_products": row.get("totalProducts"), "total_quantity": row.get("totalQuantity"),
    } for row in rows]
    frame = pd.DataFrame(typed, columns=ORDER_COLUMNS)
    if not frame.empty:
        for col in ("order_id", "customer_id", "total_products", "total_quantity"):
            frame[col] = pd.to_numeric(frame[col], errors="coerce").astype("Int64")
        for col in ("total", "discounted_total"):
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame["order_date"] = pd.to_datetime(frame["order_date"], errors="coerce", utc=True)
    return frame.drop_duplicates(subset=["order_id"], keep="last").reset_index(drop=True)


def transform_customers(rows: list[dict[str, Any]]) -> pd.DataFrame:
    typed = []
    for row in rows:
        address = row.get("address") or {}
        typed.append({
            "customer_id": row.get("id"), "first_name": row.get("firstName"),
            "last_name": row.get("lastName"), "email": row.get("email"), "phone": row.get("phone"),
            "age": row.get("age"), "city": address.get("city"), "state": address.get("state"),
        })
    frame = pd.DataFrame(typed, columns=CUSTOMER_COLUMNS)
    if not frame.empty:
        frame["customer_id"] = pd.to_numeric(frame["customer_id"], errors="coerce").astype("Int64")
        frame["age"] = pd.to_numeric(frame["age"], errors="coerce").astype("Int64")
        for col in CUSTOMER_COLUMNS[1:5] + CUSTOMER_COLUMNS[6:]:
            frame[col] = frame[col].astype("string")
    return frame.drop_duplicates(subset=["customer_id"], keep="last").reset_index(drop=True)


def write_parquet(frame: pd.DataFrame, path: str | Path) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(target, index=False)
    return str(target)

