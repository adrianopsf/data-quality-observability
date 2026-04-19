"""setup_expectations.py — Programmatically create all GE expectation suites.

Run once after `make setup-ge` to seed Great Expectations with the four mart suites.

Usage:
    python scripts/setup_expectations.py

Environment variables (loaded from .env):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from great_expectations.core import ExpectationConfiguration
from great_expectations.data_context import FileDataContext
from loguru import logger

load_dotenv()

GE_ROOT = Path(__file__).parent.parent / "great_expectations"

# ─────────────────────────────────── helpers ──────────────────────────────────


def _now_minus_24h() -> str:
    """Return ISO-8601 timestamp 24 h in the past (UTC)."""
    dt = datetime.now(tz=UTC) - timedelta(hours=24)
    return dt.isoformat()


def _current_month_minus_1() -> str:
    """Return YYYY-MM-01 for the first day of last month."""
    today = datetime.now(tz=UTC).date()
    first_of_this_month = today.replace(day=1)
    last_month = first_of_this_month - timedelta(days=1)
    return last_month.replace(day=1).isoformat()


# ─────────────────────────────── suite builders ───────────────────────────────


def build_mart_orders_suite(context: FileDataContext) -> None:
    """Create expectation suite for mart.mart_orders."""
    suite_name = "mart_orders_suite"
    suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    expectations: list[tuple[str, dict]] = [
        ("expect_table_row_count_to_be_between", {"min_value": 1}),
        ("expect_column_values_to_not_be_null", {"column": "order_id"}),
        ("expect_column_values_to_be_unique", {"column": "order_id"}),
        ("expect_column_values_to_not_be_null", {"column": "customer_id"}),
        ("expect_column_values_to_not_be_null", {"column": "order_status"}),
        (
            "expect_column_values_to_be_in_set",
            {
                "column": "order_status",
                "value_set": [
                    "delivered",
                    "shipped",
                    "processing",
                    "canceled",
                    "invoiced",
                    "approved",
                    "unavailable",
                    "created",
                ],
            },
        ),
        ("expect_column_values_to_not_be_null", {"column": "order_purchase_timestamp"}),
        (
            "expect_column_values_to_be_between",
            {"column": "total_order_value", "min_value": 0, "max_value": 100_000},
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "order_items_qty", "min_value": 1, "max_value": 999},
        ),
        (
            "expect_column_max_to_be_between",
            {
                "column": "order_purchase_timestamp",
                "min_value": _now_minus_24h(),
            },
        ),
    ]

    for exp_type, kwargs in expectations:
        suite.add_expectation(ExpectationConfiguration(expectation_type=exp_type, kwargs=kwargs))

    context.save_expectation_suite(suite)
    logger.info("Saved suite: {}", suite_name)


def build_mart_customers_suite(context: FileDataContext) -> None:
    """Create expectation suite for mart.mart_customers."""
    suite_name = "mart_customers_suite"
    suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    expectations: list[tuple[str, dict]] = [
        ("expect_table_row_count_to_be_between", {"min_value": 1}),
        ("expect_column_values_to_not_be_null", {"column": "customer_id"}),
        ("expect_column_values_to_be_unique", {"column": "customer_id"}),
        ("expect_column_values_to_not_be_null", {"column": "customer_unique_id"}),
        ("expect_column_values_to_not_be_null", {"column": "customer_state"}),
        (
            "expect_column_value_lengths_to_equal",
            {"column": "customer_state", "value": 2},
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "total_orders", "min_value": 0, "max_value": 10_000},
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "total_spent", "min_value": 0, "max_value": 1_000_000},
        ),
        (
            "expect_column_max_to_be_between",
            {"column": "last_order_date", "min_value": _now_minus_24h()},
        ),
    ]

    for exp_type, kwargs in expectations:
        suite.add_expectation(ExpectationConfiguration(expectation_type=exp_type, kwargs=kwargs))

    context.save_expectation_suite(suite)
    logger.info("Saved suite: {}", suite_name)


def build_mart_products_suite(context: FileDataContext) -> None:
    """Create expectation suite for mart.mart_products."""
    suite_name = "mart_products_suite"
    suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    expectations: list[tuple[str, dict]] = [
        ("expect_table_row_count_to_be_between", {"min_value": 1}),
        ("expect_column_values_to_not_be_null", {"column": "product_id"}),
        ("expect_column_values_to_be_unique", {"column": "product_id"}),
        ("expect_column_values_to_not_be_null", {"column": "product_category_name"}),
        (
            "expect_column_values_to_be_between",
            {"column": "avg_price", "min_value": 0.01, "max_value": 50_000},
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "total_units_sold", "min_value": 0, "max_value": 1_000_000},
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "avg_review_score", "min_value": 1.0, "max_value": 5.0},
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "product_weight_g", "min_value": 1, "max_value": 100_000},
        ),
        (
            "expect_column_max_to_be_between",
            {"column": "last_sold_at", "min_value": _now_minus_24h()},
        ),
    ]

    for exp_type, kwargs in expectations:
        suite.add_expectation(ExpectationConfiguration(expectation_type=exp_type, kwargs=kwargs))

    context.save_expectation_suite(suite)
    logger.info("Saved suite: {}", suite_name)


def build_mart_monthly_sales_suite(context: FileDataContext) -> None:
    """Create expectation suite for mart.mart_monthly_sales."""
    suite_name = "mart_monthly_sales_suite"
    suite = context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

    expectations: list[tuple[str, dict]] = [
        ("expect_table_row_count_to_be_between", {"min_value": 1}),
        ("expect_column_values_to_not_be_null", {"column": "sale_month"}),
        ("expect_column_values_to_be_unique", {"column": "sale_month"}),
        ("expect_column_values_to_not_be_null", {"column": "total_revenue"}),
        (
            "expect_column_values_to_be_between",
            {"column": "total_revenue", "min_value": 0, "max_value": 100_000_000},
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "total_orders", "min_value": 0, "max_value": 10_000_000},
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "avg_order_value", "min_value": 0, "max_value": 100_000},
        ),
        (
            "expect_column_values_to_be_between",
            {"column": "total_customers", "min_value": 0, "max_value": 10_000_000},
        ),
        (
            "expect_column_max_to_be_between",
            {"column": "sale_month", "min_value": _current_month_minus_1()},
        ),
    ]

    for exp_type, kwargs in expectations:
        suite.add_expectation(ExpectationConfiguration(expectation_type=exp_type, kwargs=kwargs))

    context.save_expectation_suite(suite)
    logger.info("Saved suite: {}", suite_name)


# ─────────────────────────────────── main ─────────────────────────────────────


def main() -> None:
    """Entry point: create all expectation suites."""
    logger.info("Initialising Great Expectations context at {}", GE_ROOT)
    context = FileDataContext(context_root_dir=str(GE_ROOT))

    builders = [
        build_mart_orders_suite,
        build_mart_customers_suite,
        build_mart_products_suite,
        build_mart_monthly_sales_suite,
    ]

    for builder in builders:
        builder(context)

    logger.success("All expectation suites created successfully.")


if __name__ == "__main__":
    main()
