from retail_pipeline.transform import transform_customers, transform_orders


def test_orders_are_typed_and_deduplicated(raw_orders):
    frame = transform_orders(raw_orders)
    assert len(frame) == 1
    assert str(frame.order_id.dtype) == "Int64"
    assert frame.iloc[0].order_id == 1


def test_customer_fields_are_flattened_and_typed(raw_customers):
    frame = transform_customers(raw_customers)
    assert frame.iloc[0].city == "London"
    assert str(frame.customer_id.dtype) == "Int64"
    assert frame.iloc[0].email == "ada@example.com"


def test_transform_empty_input_has_stable_columns():
    assert list(transform_orders([]).columns)[0] == "order_id"
    assert list(transform_customers([]).columns)[0] == "customer_id"

