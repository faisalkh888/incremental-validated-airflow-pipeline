import pytest


@pytest.fixture
def raw_orders():
    return [
        {"id": 1, "userId": 2, "total": 100.0, "discountedTotal": 90.0, "totalProducts": 2, "totalQuantity": 3},
        {"id": 1, "userId": 2, "total": 100.0, "discountedTotal": 90.0, "totalProducts": 2, "totalQuantity": 3},
    ]


@pytest.fixture
def raw_customers():
    return [{"id": 1, "firstName": "Ada", "lastName": "Lovelace", "email": "ada@example.com",
             "phone": "555-0100", "age": 36, "address": {"city": "London", "state": "Greater London"}}]

