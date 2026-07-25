import pandas as pd
import pytest

from pipeline import (
    convert_currency_to_usd,
    deduplicate_customers,
    fill_null_values,
    filter_positive_order_amounts,
    standardize_phone_number,
)

@pytest.fixture
def sample_orders():
    return pd.DataFrame(
        [
            {
                "order_id": 1,
                "customer_id": 1,
                "total_amount": 100.0,
                "currency": "USD",
                "order_date": "2023-01-01",
            },
            {
                "order_id": 2,
                "customer_id": 2,
                "total_amount": 50.0,
                "currency": "EUR",
                "order_date": "2023-01-01",
            },
            {
                "order_id": 3,
                "customer_id": 3,
                "total_amount": 50.0,
                "currency": None,
                "order_date": "2023-01-01",
            },
        ]
    )

@pytest.fixture
def sample_customers_df():
    return pd.DataFrame(
        [
            {
                "customer_id": 1,
                "email": "old@example.com",
                "phone": "(555) 123-4567",
                "signup_date": "2023-01-01",
            },
            {
                "customer_id": 1,
                "email": "new@example.com",
                "phone": "555.123.4567",
                "signup_date": "2023-06-15",
            },
            {
                "customer_id": 2,
                "email": None,
                "phone": None,
                "signup_date": "2023-02-10",
            },
        ]
    )


@pytest.fixture
def sample_exchange_rates_df():
    return pd.DataFrame(
        [
            {"currency": "USD", "date": "2023-01-01", "rate_to_usd": 1.0},
            {"currency": "EUR", "date": "2023-01-01", "rate_to_usd": 1.08},
            {"currency": "THB", "date": "2023-01-01", "rate_to_usd": 0.027},
        ]
    )




class TestStandardizePhoneNumber:
    def test_removes_formatting_from_multiple_phone_styles(self):
        customers = pd.DataFrame(
            {"phone": ["(555) 123-4567", "555.123.4567", "+66 81-234-5678"]}
        )

        result = standardize_phone_number(customers)

        assert result["phone"].tolist() == [
            "5551234567",
            "5551234567",
            "66812345678",
        ]

    def test_preserves_missing_values(self):
        customers = pd.DataFrame({"phone": [None, "02-123-4567"]})

        result = standardize_phone_number(customers)

        assert pd.isna(result.loc[0, "phone"])
        assert result.loc[1, "phone"] == "021234567"

    def test_does_not_mutate_input(self):
        customers = pd.DataFrame({"phone": ["02-123-4567"]})

        standardize_phone_number(customers)

        assert customers.loc[0, "phone"] == "02-123-4567"


class TestConvertCurrencyToUsd:
    def test_converts_orders_using_rate_for_order_date(
        self, sample_exchange_rates_df
    ):
        orders = pd.DataFrame(
            [
                {
                    "total_amount": 100.0,
                    "currency": "USD",
                    "order_date": "2023-01-01",
                },
                {
                    "total_amount": 50.0,
                    "currency": "EUR",
                    "order_date": "2023-01-01",
                },
                {
                    "total_amount": 1000.0,
                    "currency": "THB",
                    "order_date": "2023-01-01",
                },
            ]
        )

        result = convert_currency_to_usd(orders, sample_exchange_rates_df)

        assert result["usd_amount"].tolist() == [100.0, 54.0, 27.0]

    def test_missing_rate_falls_back_to_original_amount(
        self, sample_exchange_rates_df
    ):
        orders = pd.DataFrame(
            [
                {
                    "total_amount": 25.0,
                    "currency": "THB",
                    "order_date": "2099-01-01",
                }
            ]
        )

        result = convert_currency_to_usd(orders, sample_exchange_rates_df)

        assert result.loc[0, "usd_amount"] == 25.0

    def test_does_not_mutate_input(self, sample_exchange_rates_df):
        orders = pd.DataFrame(
            [
                {
                    "total_amount": 10.0,
                    "currency": "USD",
                    "order_date": "2023-01-01",
                }
            ]
        )

        convert_currency_to_usd(orders, sample_exchange_rates_df)

        assert "usd_amount" not in orders.columns


class TestFillNullValues:
    def test_fills_missing_customer_email(self, sample_customers_df):
        result = fill_null_values(
            sample_customers_df, "email", "unknown@domain.com"
        )

        customer_2 = result.loc[result["customer_id"] == 2].iloc[0]
        assert customer_2["email"] == "unknown@domain.com"

    def test_fills_missing_order_currency(self, sample_orders):
        result = fill_null_values(sample_orders, "currency", "USD")

        order_3 = result.loc[result["order_id"] == 3].iloc[0]
        assert order_3["currency"] == "USD"

    def test_does_not_mutate_input(self, sample_orders):
        fill_null_values(sample_orders, "currency", "USD")

        assert pd.isna(sample_orders.loc[2, "currency"])


class TestDeduplicateCustomers:
    def test_keeps_latest_customer_row(self, sample_customers_df):
        result = deduplicate_customers(sample_customers_df)

        expected = pd.DataFrame(
            [
                {
                    "customer_id": 2,
                    "email": None,
                    "phone": None,
                    "signup_date": "2023-02-10",
                },
                {
                    "customer_id": 1,
                    "email": "new@example.com",
                    "phone": "555.123.4567",
                    "signup_date": "2023-06-15",
                },
            ]
        )
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True),
            expected,
        )

    def test_removes_older_duplicate_row(self, sample_customers_df):
        result = deduplicate_customers(sample_customers_df)

        assert not result["email"].eq("old@example.com").any()
        assert result["customer_id"].is_unique


class TestFilterPositiveOrderAmounts:
    def test_keeps_only_positive_amounts(self):
        orders = pd.DataFrame(
            {"order_id": [1, 2, 3], "total_amount": [10.0, 0.0, -5.0]}
        )

        result = filter_positive_order_amounts(orders)

        assert result["order_id"].tolist() == [1]
        assert (result["total_amount"] > 0).all()

