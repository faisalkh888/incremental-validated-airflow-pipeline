from __future__ import annotations

import pandas as pd
import pandera as pa


ORDER_SCHEMA = pa.DataFrameSchema({
    "order_id": pa.Column("Int64", checks=pa.Check.greater_than(0), nullable=False),
    "customer_id": pa.Column("Int64", checks=pa.Check.greater_than(0), nullable=False),
    "order_date": pa.Column("datetime64[ns, UTC]", nullable=False),
    "total": pa.Column(float, checks=pa.Check.ge(0), nullable=False),
    "discounted_total": pa.Column(float, checks=pa.Check.ge(0), nullable=False),
    "total_products": pa.Column("Int64", checks=pa.Check.ge(0), nullable=False),
    "total_quantity": pa.Column("Int64", checks=pa.Check.ge(0), nullable=False),
}, strict=True, coerce=True)

CUSTOMER_SCHEMA = pa.DataFrameSchema({
    "customer_id": pa.Column("Int64", checks=pa.Check.greater_than(0), nullable=False),
    "first_name": pa.Column("string", checks=pa.Check.str_length(min_value=1), nullable=False),
    "last_name": pa.Column("string", checks=pa.Check.str_length(min_value=1), nullable=False),
    "email": pa.Column("string", checks=pa.Check.str_matches(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), nullable=False),
    "phone": pa.Column("string", checks=pa.Check.str_length(min_value=1), nullable=False),
    "age": pa.Column("Int64", checks=pa.Check.in_range(0, 125), nullable=False),
    "city": pa.Column("string", checks=pa.Check.str_length(min_value=1), nullable=False),
    "state": pa.Column("string", checks=pa.Check.str_length(min_value=1), nullable=False),
}, strict=True, coerce=True)


def _validate(frame: pd.DataFrame, schema: pa.DataFrameSchema, entity: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        return frame.copy(), pd.DataFrame(columns=["entity", "rule_name", "row_data"])
    try:
        clean = schema.validate(frame, lazy=True)
        return clean, pd.DataFrame(columns=["entity", "rule_name", "row_data"])
    except pa.errors.SchemaErrors as exc:
        failed_indices: dict[int, list[str]] = {}
        for _, error in exc.failure_cases.iterrows():
            idx = error.get("index")
            if idx in frame.index:
                name = str(error.get("column", "schema"))
                failed_indices.setdefault(int(idx), []).append(f"{entity}_{name}_check")
        quarantine = []
        for idx, rules in failed_indices.items():
            row_data = frame.loc[idx].to_dict()
            for rule in sorted(set(rules)):
                quarantine.append({"entity": entity, "rule_name": rule, "row_data": row_data})
        valid = frame.drop(index=list(failed_indices)).copy()
        if not valid.empty:
            valid = schema.validate(valid, lazy=True)
        return valid, pd.DataFrame(quarantine, columns=["entity", "rule_name", "row_data"])


def validate_orders(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _validate(frame, ORDER_SCHEMA, "orders")


def validate_customers(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _validate(frame, CUSTOMER_SCHEMA, "customers")

