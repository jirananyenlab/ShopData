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
def sample_orders_df():
    return pd.DataFrame(
        [
            {
                "order_id": 9001,
                "customer_id": 9001,
                "total_amount": 321.45,
                "currency": "USD",
                "order_date": "2040-01-20",
                "status": "COMPLETED",
            },
            {
                "order_id": 9002,
                "customer_id": 9002,
                "total_amount": 80.0,
                "currency": "EUR",
                "order_date": "2040-01-10",
                "status": "COMPLETED",
            },
            {
                "order_id": 9003,
                "customer_id": 9003,
                "total_amount": 7250.0,
                "currency": "THB",
                "order_date": "2040-01-11",
                "status": "COMPLETED",
            },
            {
                "order_id": 9004,
                "customer_id": 9004,
                "total_amount": 45.75,
                "currency": None,
                "order_date": "2040-01-12",
                "status": "PENDING",
            },
            {
                "order_id": 9005,
                "customer_id": 9005,
                "total_amount": 0.0,
                "currency": "USD",
                "order_date": "2040-01-13",
                "status": "COMPLETED",
            },
            {
                "order_id": 9006,
                "customer_id": 9006,
                "total_amount": -12.5,
                "currency": "EUR",
                "order_date": "2040-01-14",
                "status": "SYSTEM_ERROR",
            },
            {
                "order_id": 9007,
                "customer_id": 9007,
                "total_amount": 80.0,
                "currency": "EUR",
                "order_date": "2040-01-11",
                "status": "COMPLETED",
            },
        ]
    )

@pytest.fixture
def sample_customers_df():
    return pd.DataFrame(
        [
            {
                "customer_id": 9001,
                "full_name": "Nora Winters",
                "email": "old.nora@example.test",
                "phone": "(212) 400-9001",
                "signup_date": "2040-01-02",
            },
            {
                "customer_id": 9001,
                "full_name": "Nora Winters",
                "email": "nora.latest@example.test",
                "phone": "212.400.9001",
                "signup_date": "2040-06-18",
            },
            {
                "customer_id": 9002,
                "full_name": "Theo Rivers",
                "email": None,
                "phone": None,
                "signup_date": "2040-02-14",
            },
            {
                "customer_id": 9003,
                "full_name": "Kai Arun",
                "email": "kai@example.test",
                "phone": "+66 92-840-9003",
                "signup_date": "2040-03-09",
            },
            {
                "customer_id": 9004,
                "full_name": "Maya Sterling",
                "email": "maya.old@example.test",
                "phone": "020 7946 9004",
                "signup_date": "2040-04-01",
            },
            {
                "customer_id": 9004,
                "full_name": "Maya Sterling",
                "email": "maya.latest@example.test",
                "phone": "020-7946-9999",
                "signup_date": "2040-08-22",
            },
        ]
    )


@pytest.fixture
def sample_exchange_rates_df():
    return pd.DataFrame(
        [
            {"currency": "USD", "date": "2040-01-10", "rate_to_usd": 9.99},
            {"currency": "EUR", "date": "2040-01-10", "rate_to_usd": 1.11},
            {"currency": "EUR", "date": "2040-01-11", "rate_to_usd": 1.25},
            {"currency": "EUR", "date": "2040-01-12", "rate_to_usd": 1.05},
            {"currency": "THB", "date": "2040-01-10", "rate_to_usd": 0.031},
            {"currency": "THB", "date": "2040-01-11", "rate_to_usd": 0.029},
            {"currency": "GBP", "date": "2040-01-10", "rate_to_usd": 1.42},
        ]
    )


class TestStandardizePhoneNumber:
    # ตรวจว่าฟังก์ชันรับ DataFrame ลูกค้าทั้งก้อนและลบอักขระที่ไม่ใช่ตัวเลข
    def test_removes_formatting_from_multiple_phone_styles(
        self, sample_customers_df
    ):
        result = standardize_phone_number(sample_customers_df)

        assert result["phone"].tolist() == [
            "2124009001",
            "2124009001",
            None,
            "66928409003",
            "02079469004",
            "02079469999",
        ]

    # ตรวจว่าหมายเลขที่เป็นค่าว่างยังคงเป็นค่าว่างหลังแปลงข้อมูล
    def test_preserves_missing_values(self, sample_customers_df):
        result = standardize_phone_number(sample_customers_df)

        customer_9002 = result.loc[result["customer_id"] == 9002].iloc[0]
        assert pd.isna(customer_9002["phone"])


class TestConvertCurrencyToUsd:
    # ตรวจว่า order สกุล USD คงยอดเดิมและไม่คูณ exchange rate
    def test_usd_order_keeps_original_amount_without_conversion(
        self,sample_orders_df, sample_exchange_rates_df
    ):
        result = convert_currency_to_usd(
           sample_orders_df, sample_exchange_rates_df
        )
        usd_order = result.loc[result["order_id"] == 9001].iloc[0]

        assert usd_order["usd_amount"] == pytest.approx(321.45)

    # ตรวจว่าแต่ละ order ใช้ rate ของ currency และวันที่ตรงกับ order_date
    def test_uses_rate_matching_each_order_date(
        self,sample_orders_df, sample_exchange_rates_df
    ):
        result = convert_currency_to_usd(
           sample_orders_df, sample_exchange_rates_df
        )
        converted = result.set_index("order_id")

        assert converted.loc[9002, "usd_amount"] == pytest.approx(80.0 * 1.11)
        assert converted.loc[9007, "usd_amount"] == pytest.approx(80.0 * 1.25)
        assert converted.loc[9003, "usd_amount"] == pytest.approx(
            7250.0 * 0.029
        )

    # ตรวจว่าเมื่อไม่พบ rate ของวันนั้น ระบบจะใช้ยอดเดิมตาม fallback rule
    def test_missing_rate_falls_back_to_original_amount(
        self,sample_orders_df, sample_exchange_rates_df
    ):
        result = convert_currency_to_usd(
            sample_orders_df, sample_exchange_rates_df
        )

        assert result.loc[0, "usd_amount"] == 321.45



class TestFillNullValues:
    # ตรวจว่า email ที่หายไปถูกแทนด้วยค่าเริ่มต้น
    def test_fills_missing_customer_email(self, sample_customers_df):
        result = fill_null_values(
            sample_customers_df, "email", "unknown@domain.com"
        )

        customer_2 = result.loc[result["customer_id"] == 9002].iloc[0]
        assert customer_2["email"] == "unknown@domain.com"

    # ตรวจว่า currency ที่หายไปถูกแทนด้วย USD
    def test_fills_missing_order_currency(self,sample_orders_df):
        result = fill_null_values(sample_orders_df, "currency", "USD")

        order_3 = result.loc[result["order_id"] == 9004].iloc[0]
        assert order_3["currency"] == "USD"


class TestDeduplicateCustomers:
    # ตรวจว่า customer_id ที่ซ้ำจะเหลือเฉพาะ row ที่มี signup_date ล่าสุด
    def test_keeps_only_latest_customer_row(self, sample_customers_df):
        result = deduplicate_customers(sample_customers_df)

        latest_9001 = result.loc[result["customer_id"] == 9001].iloc[0]
        latest_9004 = result.loc[result["customer_id"] == 9004].iloc[0]

        assert len(result) == 4
        assert result["customer_id"].is_unique

        assert latest_9001["email"] == "nora.latest@example.test"
        assert latest_9001["signup_date"] == "2040-06-18"
        assert latest_9004["email"] == "maya.latest@example.test"
        assert latest_9004["signup_date"] == "2040-08-22"

        removed_emails = {
            "old.nora@example.test",
            "maya.old@example.test",
        }
        assert removed_emails.isdisjoint(set(result["email"].dropna()))


class TestFilterPositiveOrderAmounts:
    # ตรวจว่าเก็บเฉพาะ order ที่ total_amount มากกว่า 0
    def test_keeps_only_positive_amounts(self,sample_orders_df):
        result = filter_positive_order_amounts(sample_orders_df)

        assert result["order_id"].tolist() == [
            9001,
            9002,
            9003,
            9004,
            9007,
        ]
        assert (result["total_amount"] > 0).all()
