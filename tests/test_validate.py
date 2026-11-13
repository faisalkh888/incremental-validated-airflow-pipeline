import pandas as pd

from retail_pipeline.transform import transform_customers, transform_orders
from retail_pipeline.validate import validate_customers, validate_orders


def test_valid_orders_pass_and_bad_total_is_quarantined(raw_orders):
    frame = transform_orders(raw_orders)
    valid, quarantine = validate_orders(frame)
    assert len(valid) == 1
    bad = frame.copy()
    bad.loc[0, "total"] = -1
    valid, quarantine = validate_orders(bad)
    assert valid.empty
    assert "orders_total_check" in set(quarantine.rule_name)


def test_valid_customers_pass_and_bad_email_is_quarantined(raw_customers):
    frame = transform_customers(raw_customers)
    valid, quarantine = validate_customers(frame)
    assert len(valid) == 1
    bad = frame.copy()
    bad.loc[0, "email"] = "not-an-email"
    valid, quarantine = validate_customers(bad)
    assert valid.empty
    assert "customers_email_check" in set(quarantine.rule_name)


def test_checks_cover_ten_or_more_rules():
    from retail_pipeline.validate import CUSTOMER_SCHEMA, ORDER_SCHEMA
    total = sum(len(col.checks) for schema in (ORDER_SCHEMA, CUSTOMER_SCHEMA) for col in schema.columns.values())
    assert total >= 10


def test_null_required_value_is_quarantined(raw_customers):
    frame = transform_customers(raw_customers)
    frame.loc[0, "first_name"] = None
    valid, quarantine = validate_customers(frame)
    assert valid.empty
    assert not quarantine.empty

