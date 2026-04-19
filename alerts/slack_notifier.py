"""slack_notifier.py — Slack webhook integration for DQ alerts and reports.

Provides SlackNotifier with two public methods:
  - send_alert: targeted failure notification per table/expectation.
  - send_report: weekly consolidated summary across all mart tables.
"""

from __future__ import annotations

from typing import Any

import requests
from loguru import logger


class SlackNotifier:
    """Send data quality notifications to a Slack channel via Incoming Webhook.

    Args:
        webhook_url: Slack Incoming Webhook URL.
        timeout: HTTP request timeout in seconds.

    Example:
        >>> notifier = SlackNotifier(webhook_url="https://hooks.slack.com/...")
        >>> notifier.send_alert(
        ...     table="mart.mart_orders",
        ...     expectation="expect_column_values_to_not_be_null",
        ...     column="order_id",
        ...     unexpected_count=42,
        ...     run_date="2024-06-01",
        ... )
    """

    def __init__(self, webhook_url: str, timeout: int = 10) -> None:
        self._webhook_url = webhook_url
        self._timeout = timeout

    # ──────────────────────────── public API ──────────────────────────────────

    def send_alert(
        self,
        table: str,
        expectation: str,
        column: str,
        unexpected_count: int | str,
        run_date: str,
    ) -> None:
        """Send a targeted DQ failure alert to Slack.

        Args:
            table: Fully-qualified table name, e.g. ``mart.mart_orders``.
            expectation: GE expectation type that failed.
            column: Column name (or ``"table-level"`` for table expectations).
            unexpected_count: Number of unexpected rows / ``"N/A"``.
            run_date: ISO date string of the Airflow run, e.g. ``"2024-06-01"``.
        """
        readable_exp = expectation.replace("expect_", "").replace("_", " ").title()

        payload: dict[str, Any] = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🚨 Data Quality Alert",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Table*\n`{table}`"},
                        {"type": "mrkdwn", "text": f"*Run Date*\n{run_date}"},
                        {"type": "mrkdwn", "text": f"*Failed Check*\n{readable_exp}"},
                        {"type": "mrkdwn", "text": f"*Column*\n`{column}`"},
                        {
                            "type": "mrkdwn",
                            "text": f"*Unexpected Rows*\n{unexpected_count}",
                        },
                    ],
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": (
                                "Check the <http://localhost:8090|GE Data Docs> "
                                "for the full validation report."
                            ),
                        }
                    ],
                },
                {"type": "divider"},
            ]
        }

        self._post(payload)
        logger.info(
            "Slack alert sent — table={}, expectation={}, unexpected_count={}",
            table,
            expectation,
            unexpected_count,
        )

    def send_report(
        self,
        summary_dict: dict[str, dict[str, int]],
        period: str,
        dag_run_date: str,
    ) -> None:
        """Send a weekly consolidated DQ report to Slack.

        Args:
            summary_dict: Mapping of table name → stats dict with keys
                ``runs``, ``passed``, ``failed``, ``total_expectations``.
            period: Human-readable date range, e.g. ``"2024-05-27 → 2024-06-03"``.
            dag_run_date: ISO date of the Airflow run.
        """
        total_runs = sum(v.get("runs", 0) for v in summary_dict.values())
        total_passed = sum(v.get("passed", 0) for v in summary_dict.values())
        total_failed = sum(v.get("failed", 0) for v in summary_dict.values())
        success_rate = round(total_passed / total_runs * 100, 1) if total_runs else 0.0

        status_emoji = "✅" if total_failed == 0 else "⚠️"

        table_lines: list[str] = []
        for table, stats in sorted(summary_dict.items()):
            runs = stats.get("runs", 0)
            passed = stats.get("passed", 0)
            failed = stats.get("failed", 0)
            rate = round(passed / runs * 100, 1) if runs else 0.0
            icon = "✅" if failed == 0 else "❌"
            table_lines.append(f"{icon} `{table}` — {passed}/{runs} runs passed ({rate}%)")

        table_summary = "\n".join(table_lines) if table_lines else "No validations found."

        payload: dict[str, Any] = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{status_emoji} Weekly DQ Report — {period}",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Overall Success Rate*\n{success_rate}%",
                        },
                        {
                            "type": "mrkdwn",
                            "text": (
                                f"*Total Runs*\n{total_passed} passed / {total_failed} failed"
                            ),
                        },
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Per-Table Breakdown*\n{table_summary}",
                    },
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": (
                                f"Generated on {dag_run_date}  •  "
                                "<http://localhost:8090|GE Data Docs>  •  "
                                "<http://localhost:3000|Grafana Dashboard>"
                            ),
                        }
                    ],
                },
                {"type": "divider"},
            ]
        }

        self._post(payload)
        logger.info(
            "Slack weekly report sent — period={}, success_rate={}%",
            period,
            success_rate,
        )

    # ──────────────────────────── private ─────────────────────────────────────

    def _post(self, payload: dict[str, Any]) -> None:
        """POST a JSON payload to the Slack webhook.

        Args:
            payload: Slack Block Kit message payload.

        Raises:
            requests.HTTPError: If Slack returns a non-2xx status code.
        """
        response = requests.post(
            self._webhook_url,
            json=payload,
            timeout=self._timeout,
        )
        response.raise_for_status()
        logger.debug("Slack webhook response: {}", response.text)
