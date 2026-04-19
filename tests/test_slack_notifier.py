"""test_slack_notifier.py — Unit tests for SlackNotifier.

All HTTP calls are mocked with the `responses` library — no real Slack
webhook is required.
"""

from __future__ import annotations

import json

import pytest
import requests
import responses as rsps_lib

from alerts.slack_notifier import SlackNotifier

FAKE_WEBHOOK = "https://hooks.slack.com/services/T000/B000/FAKE"


# ──────────────────────────────── helpers ─────────────────────────────────────


def _body(call_index: int = 0) -> dict:
    """Decode the JSON body from the n-th captured request.

    Args:
        call_index: Index into rsps_lib.calls (default 0).

    Returns:
        Parsed JSON dict.
    """
    raw = rsps_lib.calls[call_index].request.body
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


# ──────────────────────────────── fixtures ────────────────────────────────────


@pytest.fixture()
def notifier() -> SlackNotifier:
    """Return a SlackNotifier wired to the fake webhook URL."""
    return SlackNotifier(webhook_url=FAKE_WEBHOOK)


# ──────────────────────────── send_alert tests ────────────────────────────────


@rsps_lib.activate
def test_send_alert_posts_to_webhook(notifier: SlackNotifier) -> None:
    """send_alert should POST a JSON payload to the Slack webhook."""
    rsps_lib.add(rsps_lib.POST, FAKE_WEBHOOK, body="ok", status=200)

    notifier.send_alert(
        table="mart.mart_orders",
        expectation="expect_column_values_to_not_be_null",
        column="order_id",
        unexpected_count=5,
        run_date="2024-06-01",
    )

    assert len(rsps_lib.calls) == 1
    payload = _body()
    full_text = json.dumps(payload)
    assert "mart.mart_orders" in full_text
    assert "order_id" in full_text


@rsps_lib.activate
def test_send_alert_contains_required_fields(notifier: SlackNotifier) -> None:
    """Slack payload must include table, expectation, column, unexpected count."""
    rsps_lib.add(rsps_lib.POST, FAKE_WEBHOOK, body="ok", status=200)

    notifier.send_alert(
        table="mart.mart_customers",
        expectation="expect_column_values_to_be_unique",
        column="customer_id",
        unexpected_count=12,
        run_date="2024-06-02",
    )

    full_text = json.dumps(_body())
    assert "mart.mart_customers" in full_text
    assert "customer_id" in full_text
    assert "12" in full_text
    assert "2024-06-02" in full_text


@rsps_lib.activate
def test_send_alert_raises_on_http_error(notifier: SlackNotifier) -> None:
    """send_alert should raise requests.HTTPError on non-2xx response."""
    rsps_lib.add(rsps_lib.POST, FAKE_WEBHOOK, body="invalid_payload", status=400)

    with pytest.raises(requests.HTTPError):
        notifier.send_alert(
            table="mart.mart_orders",
            expectation="expect_table_row_count_to_be_between",
            column="table-level",
            unexpected_count="N/A",
            run_date="2024-06-01",
        )


@rsps_lib.activate
def test_send_alert_table_level_expectation(notifier: SlackNotifier) -> None:
    """send_alert should handle table-level expectations (no specific column)."""
    rsps_lib.add(rsps_lib.POST, FAKE_WEBHOOK, body="ok", status=200)

    notifier.send_alert(
        table="mart.mart_products",
        expectation="expect_table_row_count_to_be_between",
        column="table-level",
        unexpected_count="N/A",
        run_date="2024-06-01",
    )

    full_text = json.dumps(_body())
    assert "table-level" in full_text
    assert "N/A" in full_text


# ──────────────────────────── send_report tests ───────────────────────────────


@rsps_lib.activate
def test_send_report_posts_to_webhook(notifier: SlackNotifier) -> None:
    """send_report should POST exactly one request to the webhook."""
    rsps_lib.add(rsps_lib.POST, FAKE_WEBHOOK, body="ok", status=200)

    summary = {
        "mart.mart_orders": {"runs": 7, "passed": 7, "failed": 0, "total_expectations": 70},
        "mart.mart_customers": {"runs": 7, "passed": 6, "failed": 1, "total_expectations": 63},
    }

    notifier.send_report(
        summary_dict=summary,
        period="2024-05-27 → 2024-06-03",
        dag_run_date="2024-06-03",
    )

    assert len(rsps_lib.calls) == 1
    full_text = json.dumps(_body())
    assert "mart.mart_orders" in full_text
    assert "mart.mart_customers" in full_text


@rsps_lib.activate
def test_send_report_computes_success_rate(notifier: SlackNotifier) -> None:
    """send_report should embed correct success rate in the Slack payload."""
    rsps_lib.add(rsps_lib.POST, FAKE_WEBHOOK, body="ok", status=200)

    # 4 passed + 4 passed = 8 total runs, 6 passed → 75%
    summary = {
        "mart.mart_orders": {"runs": 4, "passed": 4, "failed": 0, "total_expectations": 40},
        "mart.mart_products": {"runs": 4, "passed": 2, "failed": 2, "total_expectations": 36},
    }

    notifier.send_report(
        summary_dict=summary,
        period="2024-06-01 → 2024-06-07",
        dag_run_date="2024-06-07",
    )

    full_text = json.dumps(_body())
    # 6 passed out of 8 total runs = 75.0%
    assert "75.0" in full_text


@rsps_lib.activate
def test_send_report_empty_summary(notifier: SlackNotifier) -> None:
    """send_report with empty summary should still succeed without crashing."""
    rsps_lib.add(rsps_lib.POST, FAKE_WEBHOOK, body="ok", status=200)

    notifier.send_report(
        summary_dict={},
        period="2024-06-01 → 2024-06-07",
        dag_run_date="2024-06-07",
    )

    assert len(rsps_lib.calls) == 1
    full_text = json.dumps(_body())
    assert "No validations found" in full_text


@rsps_lib.activate
def test_send_report_raises_on_http_error(notifier: SlackNotifier) -> None:
    """send_report should propagate HTTP errors from the webhook."""
    rsps_lib.add(rsps_lib.POST, FAKE_WEBHOOK, body="channel_not_found", status=404)

    with pytest.raises(requests.HTTPError):
        notifier.send_report(
            summary_dict={"mart.mart_orders": {"runs": 1, "passed": 1, "failed": 0}},
            period="2024-06-01 → 2024-06-07",
            dag_run_date="2024-06-07",
        )
