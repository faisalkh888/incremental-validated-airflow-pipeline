from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2.extras import execute_values


@contextmanager
def connection() -> Iterator:
    conn = psycopg2.connect(
        host=os.getenv("PIPELINE_DB_HOST", "localhost"), port=int(os.getenv("PIPELINE_DB_PORT", "5432")),
        dbname=os.getenv("PIPELINE_DB_NAME", "retail"), user=os.getenv("PIPELINE_DB_USER", "retail"),
        password=os.getenv("PIPELINE_DB_PASSWORD", "retail"),
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_schema() -> None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_state (name TEXT PRIMARY KEY, high_water_mark BIGINT NOT NULL);
        INSERT INTO pipeline_state(name, high_water_mark) VALUES ('orders', 0) ON CONFLICT (name) DO NOTHING;
        CREATE TABLE IF NOT EXISTS stg_orders (
          order_id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, order_date TIMESTAMPTZ NOT NULL,
          total DOUBLE PRECISION NOT NULL, discounted_total DOUBLE PRECISION NOT NULL,
          total_products INTEGER NOT NULL, total_quantity INTEGER NOT NULL, run_date DATE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stg_customers (
          customer_id BIGINT PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL, email TEXT NOT NULL,
          phone TEXT NOT NULL, age INTEGER NOT NULL, city TEXT NOT NULL, state TEXT NOT NULL, run_date DATE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS core_orders (LIKE stg_orders INCLUDING ALL);
        CREATE TABLE IF NOT EXISTS core_customers (LIKE stg_customers INCLUDING ALL);
        CREATE TABLE IF NOT EXISTS quarantine (
          quarantine_id BIGSERIAL PRIMARY KEY, run_date DATE NOT NULL, entity TEXT NOT NULL,
          rule_name TEXT NOT NULL, row_data JSONB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pipeline_runs (
          run_date DATE PRIMARY KEY, orders_extracted INTEGER NOT NULL, customers_extracted INTEGER NOT NULL,
          orders_loaded INTEGER NOT NULL, customers_loaded INTEGER NOT NULL, rows_quarantined INTEGER NOT NULL,
          duration_seconds DOUBLE PRECISION NOT NULL, status TEXT NOT NULL, completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)


def get_high_water_mark() -> int:
    ensure_schema()
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT high_water_mark FROM pipeline_state WHERE name='orders'")
        return int(cur.fetchone()[0])


def _records(frame, run_date: str, columns: list[str]) -> list[tuple]:
    records = []
    for row in frame[columns].itertuples(index=False, name=None):
        values = tuple(None if value is None else (value.item() if hasattr(value, "item") else value) for value in row)
        records.append(values + (run_date,))
    return records


def commit_run(run_date: str, orders, customers, quarantine, summary: dict) -> dict:
    """Idempotently replace that logical-date partition and advance the order watermark."""
    ensure_schema()
    order_cols = ["order_id", "customer_id", "order_date", "total", "discounted_total", "total_products", "total_quantity"]
    customer_cols = ["customer_id", "first_name", "last_name", "email", "phone", "age", "city", "state"]
    with connection() as conn, conn.cursor() as cur:
        # Logical-date replacement makes retries and explicit day reruns safe.
        for table in ("stg_orders", "core_orders", "stg_customers", "core_customers"):
            cur.execute(f"DELETE FROM {table} WHERE run_date=%s", (run_date,))
        cur.execute("DELETE FROM quarantine WHERE run_date=%s", (run_date,))
        order_records = _records(orders, run_date, order_cols)
        customer_records = _records(customers, run_date, customer_cols)
        order_insert = "INSERT INTO {table} (order_id,customer_id,order_date,total,discounted_total,total_products,total_quantity,run_date) VALUES %s ON CONFLICT (order_id) DO UPDATE SET customer_id=EXCLUDED.customer_id,order_date=EXCLUDED.order_date,total=EXCLUDED.total,discounted_total=EXCLUDED.discounted_total,total_products=EXCLUDED.total_products,total_quantity=EXCLUDED.total_quantity,run_date=EXCLUDED.run_date"
        customer_insert = "INSERT INTO {table} (customer_id,first_name,last_name,email,phone,age,city,state,run_date) VALUES %s ON CONFLICT (customer_id) DO UPDATE SET first_name=EXCLUDED.first_name,last_name=EXCLUDED.last_name,email=EXCLUDED.email,phone=EXCLUDED.phone,age=EXCLUDED.age,city=EXCLUDED.city,state=EXCLUDED.state,run_date=EXCLUDED.run_date"
        for table in ("stg_orders", "core_orders"):
            if order_records:
                execute_values(cur, order_insert.format(table=table), order_records)
        for table in ("stg_customers", "core_customers"):
            if customer_records:
                execute_values(cur, customer_insert.format(table=table), customer_records)
        for row in quarantine.to_dict("records"):
            cur.execute("INSERT INTO quarantine(run_date,entity,rule_name,row_data) VALUES (%s,%s,%s,%s::jsonb)",
                        (run_date, row["entity"], row["rule_name"], json.dumps(row["row_data"], default=str)))
        high_water = int(summary.get("max_order_id", 0))
        cur.execute("UPDATE pipeline_state SET high_water_mark=GREATEST(high_water_mark,%s) WHERE name='orders'", (high_water,))
        cur.execute("""INSERT INTO pipeline_runs(run_date,orders_extracted,customers_extracted,orders_loaded,customers_loaded,rows_quarantined,duration_seconds,status)
          VALUES (%s,%s,%s,%s,%s,%s,%s,'success') ON CONFLICT(run_date) DO UPDATE SET orders_extracted=EXCLUDED.orders_extracted,customers_extracted=EXCLUDED.customers_extracted,orders_loaded=EXCLUDED.orders_loaded,customers_loaded=EXCLUDED.customers_loaded,rows_quarantined=EXCLUDED.rows_quarantined,duration_seconds=EXCLUDED.duration_seconds,status='success',completed_at=now()""",
          (run_date, summary["orders_extracted"], summary["customers_extracted"], len(orders), len(customers), len(quarantine), summary["duration_seconds"]))
    return {"orders_loaded": len(orders), "customers_loaded": len(customers), "rows_quarantined": len(quarantine)}

