"""dq_report_dag.py — Weekly consolidated data quality report sent to Slack.

DAG: olist_dq_report
Schedule: @weekly (Sunday 08:00 UTC)
Owner: data-engineering

Queries the GE validations store for the past 7 days, builds a summary dict,
and sends it as a rich Slack message via SlackNotifier.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from loguru import logger

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────── constants ────────────────────────────────────────

GE_ROOT = Path(__file__).parent.parent.parent / "great_expectations"
VALIDATIONS_DIR = GE_ROOT / "uncommitted" / "validations"

MART_TABLES: list[str] = [
    "mart.mart_orders",
    "mart.mart_customers",
    "mart.mart_products",
    "mart.mart_monthly_sales",
]

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─────────────────────────── task functions ───────────────────────────────────


def collect_weekly_results(**context: Any) -> dict[str, Any]:
    """Scan GE validations store and aggregate results for the past 7 days.

    Args:
        **context: Airflow context dictionary.

    Returns:
        Summary dict keyed by table name, each containing pass/fail counts.
    """
    cutoff = datetime.now() - timedelta(days=7)
    summary: dict[str, dict[str, int]] = defaultdict(
        lambda: {"runs": 0, "passed": 0, "failed": 0, "total_expectations": 0}
    )

    if not VALIDATIONS_DIR.exists():
        logger.warning("Validations directory not found: {}", VALIDATIONS_DIR)
        return dict(summary)

    for result_file in VALIDATIONS_DIR.rglob("*.json"):
        try:
            stat = result_file.stat()
            if datetime.fromtimestamp(stat.st_mtime) < cutoff:
                continue

            with result_file.open() as fh:
                data = json.load(fh)

            # Derive table name from suite name
            suite_name: str = data.get("meta", {}).get("expectation_suite_name", "unknown")
            table = suite_name.replace("_suite", "").replace("_", ".", 1)

            statistics = data.get("statistics", {})
            summary[table]["runs"] += 1
            summary[table]["total_expectations"] += statistics.get("evaluated_expectations", 0)
            if data.get("success", False):
                summary[table]["passed"] += 1
            else:
                summary[table]["failed"] += 1

        except (json.JSONDecodeError, KeyError, OSError) as exc:
            logger.warning("Could not parse {}: {}", result_file, exc)

    logger.info(
        "Weekly report collected {} table entries from validations store.",
        len(summary),
    )
    return dict(summary)


def send_weekly_report(**context: Any) -> None:
    """Build and send the weekly Slack report.

    Args:
        **context: Airflow context dictionary.
    """
    from alerts.slack_notifier import SlackNotifier

    ti = context["task_instance"]
    summary: dict[str, Any] = ti.xcom_pull(task_ids="collect_weekly_results") or {}

    webhook_url = os.environ["SLACK_WEBHOOK_URL"]
    notifier = SlackNotifier(webhook_url=webhook_url)

    report_period = (
        f"{(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')} → "
        f"{datetime.now().strftime('%Y-%m-%d')}"
    )

    notifier.send_report(
        summary_dict=summary,
        period=report_period,
        dag_run_date=context["ds"],
    )

    logger.success("Weekly DQ report sent to Slack for period: {}", report_period)


# ─────────────────────────── DAG definition ───────────────────────────────────

with DAG(
    dag_id="olist_dq_report",
    description="Weekly consolidated data quality report sent to Slack",
    schedule_interval="0 8 * * 0",  # Sunday 08:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["data-quality", "reporting", "slack", "olist"],
    doc_md=__doc__,
) as dag:
    collect_task = PythonOperator(
        task_id="collect_weekly_results",
        python_callable=collect_weekly_results,
        do_xcom_push=True,
    )

    report_task = PythonOperator(
        task_id="send_weekly_report",
        python_callable=send_weekly_report,
    )

    collect_task >> report_task
