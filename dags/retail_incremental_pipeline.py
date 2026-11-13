from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="retail_incremental_validated_pipeline",
    description="Incrementally extract orders and snapshot customers into PostgreSQL",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=2), "execution_timeout": timedelta(minutes=20)},
    tags=["retail", "incremental", "validated"],
)
def retail_incremental_validated_pipeline():
    @task
    def ensure_db():
        from retail_pipeline.db import ensure_schema
        ensure_schema()

    @task
    def extract(logical_date=None):
        from retail_pipeline.db import get_high_water_mark
        from retail_pipeline.config import data_dir
        from retail_pipeline.extract import extract_run
        return extract_run(logical_date.strftime("%Y-%m-%d"), get_high_water_mark(), data_dir() / "bronze")

    @task
    def transform_validate(extracted: dict, logical_date=None):
        import json
        import pandas as pd
        from retail_pipeline.config import data_dir
        from retail_pipeline.transform import read_jsonl, transform_customers, transform_orders, write_parquet
        from retail_pipeline.validate import validate_customers, validate_orders
        run_date = logical_date.strftime("%Y-%m-%d")
        orders = transform_orders(read_jsonl(extracted["orders_path"]))
        customers = transform_customers(read_jsonl(extracted["customers_path"]))
        orders, order_q = validate_orders(orders)
        customers, customer_q = validate_customers(customers)
        orders_path = write_parquet(orders, data_dir() / "silver" / run_date / "orders.parquet")
        customers_path = write_parquet(customers, data_dir() / "silver" / run_date / "customers.parquet")
        quarantine = pd.concat([order_q, customer_q], ignore_index=True)
        quarantine_path = data_dir() / "silver" / run_date / "quarantine.jsonl"
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        with quarantine_path.open("w", encoding="utf-8") as stream:
            for row in quarantine.to_dict("records"):
                stream.write(json.dumps(row, default=str) + "\n")
        return {"orders_path": orders_path, "customers_path": customers_path, "quarantine_path": str(quarantine_path),
                "orders_extracted": extracted["orders_extracted"], "customers_extracted": extracted["customers_extracted"],
                "max_order_id": extracted["max_order_id"]}

    @task
    def load_and_summarize(payload: dict, logical_date=None, **context):
        import pandas as pd
        from pathlib import Path
        from retail_pipeline.db import commit_run
        run_date = logical_date.strftime("%Y-%m-%d")
        orders = pd.read_parquet(payload["orders_path"])
        customers = pd.read_parquet(payload["customers_path"])
        quarantine_path = Path(payload["quarantine_path"])
        quarantine = pd.read_json(quarantine_path, lines=True) if quarantine_path.stat().st_size else pd.DataFrame(columns=["entity", "rule_name", "row_data"])
        started = context["dag_run"].start_date
        duration = max(0.0, (pendulum.now("UTC") - started).total_seconds()) if started else 0.0
        summary = {**payload, "duration_seconds": duration}
        loaded = commit_run(run_date, orders, customers, quarantine, summary)
        return {**summary, **loaded}

    ready = ensure_db()
    extracted = extract()
    ready >> extracted
    validated = transform_validate(extracted)
    load_and_summarize(validated)


retail_incremental_validated_pipeline()

