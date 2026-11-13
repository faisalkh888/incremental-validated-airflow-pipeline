from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

from retail_pipeline import db
from retail_pipeline.config import data_dir
from retail_pipeline.extract import extract_run
from retail_pipeline.transform import read_jsonl, transform_customers, transform_orders, write_parquet
from retail_pipeline.validate import validate_customers, validate_orders


def run_pipeline(run_date: str) -> dict:
    started = time.monotonic()
    watermark = db.get_high_water_mark()
    extracted = extract_run(run_date, watermark, data_dir() / "bronze")
    orders = transform_orders(read_jsonl(extracted["orders_path"]))
    customers = transform_customers(read_jsonl(extracted["customers_path"]))
    orders, order_q = validate_orders(orders)
    customers, customer_q = validate_customers(customers)
    write_parquet(orders, data_dir() / "silver" / run_date / "orders.parquet")
    write_parquet(customers, data_dir() / "silver" / run_date / "customers.parquet")
    quarantine = pd.concat([order_q, customer_q], ignore_index=True)
    summary = {**extracted, "duration_seconds": time.monotonic() - started}
    loaded = db.commit_run(run_date, orders, customers, quarantine, summary)
    return {**summary, **loaded, "duration_seconds": time.monotonic() - started}

