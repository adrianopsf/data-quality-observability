"""dq_validation_dag.py — Daily data quality validation for all mart tables.

DAG: olist_dq_validation
Schedule: @daily (00:00 UTC)
Owner: data-engineering

Runs four Great Expectations checkpoints in sequence.
On failure, sends a Slack alert via SlackNotifier.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from loguru import logger

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────── constants ────────────────────────────────────────

CHECKPOINTS: list[str] = [
    "mart_orders_checkpoint",
    "mart_customers_checkpoint",
    "mart_products_checkpoint",
    "mart_monthly_sales_checkpoint",
]

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─────────────────────────── task functions ───────────────────────────────────


def _run_checkpoint_task(checkpoint_name: str, **context: Any) -> dict[str, Any]:
    """Airflow task wrapper: run a GE checkpoint and push summary to XCom.

    Args:
        checkpoint_name: Name of the checkpoint to run.
        **context: Airflow context dictionary.

    Returns:
        Checkpoint summary dict (automatically pushed to XCom).
    """
    # Imported at call-time so unit tests can mock GE without Airflow present
    from checkpoint_runner import run_checkpoint  # noqa: PLC0415

    run_id = f"{context['ds']}-{checkpoint_name}"
    summary = run_checkpoint(checkpoint_name=checkpoint_name, run_id=run_id)

    if not summary["success"]:
        logger.warning(
            "Checkpoint {} FAILED — {} unexpected failures.",
            checkpoint_name,
            len(summary["failed_expectations"]),
        )
    return summary


def notify_slack_on_failure(**context: Any) -> None:
    """Send Slack alert if any checkpoint failed.

    Args:
        **context: Airflow context dictionary, expects ``task_instance``.
    """
    from alerts.slack_notifier import SlackNotifier  # noqa: PLC0415

    ti = context["task_instance"]
    webhook_url = os.environ["SLACK_WEBHOOK_URL"]
    notifier = SlackNotifier(webhook_url=webhook_url)

    any_failure = False
    for checkpoint_name in CHECKPOINTS:
        task_id = f"run_{checkpoint_name}"
        result: dict[str, Any] | None = ti.xcom_pull(task_ids=task_id)
        if result and not result.get("success", True):
            any_failure = True
            table = checkpoint_name.replace("_checkpoint", "").replace("_", ".", 1)
            for failure in result.get("failed_expectations", []):
                notifier.send_alert(
                    table=table,
                    expectation=failure["expectation"],
                    column=failure["column"],
                    unexpected_count=failure.get("unexpected_count", "N/A"),
                    run_date=context["ds"],
                )
            logger.warning("Slack alert sent for table: {}", table)

    if not any_failure:
        logger.info("All checkpoints passed — no Slack alert needed.")


# ─────────────────────────── DAG definition ───────────────────────────────────

with DAG(
    dag_id="olist_dq_validation",
    description="Daily data quality validation for all mart tables using Great Expectations",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["data-quality", "great-expectations", "olist"],
    doc_md=__doc__,
) as dag:
    checkpoint_tasks = []
    for cp in CHECKPOINTS:
        task = PythonOperator(
            task_id=f"run_{cp}",
            python_callable=_run_checkpoint_task,
            op_kwargs={"checkpoint_name": cp},
            do_xcom_push=True,
        )
        checkpoint_tasks.append(task)

    slack_notify = PythonOperator(
        task_id="notify_slack_on_failure",
        python_callable=notify_slack_on_failure,
        trigger_rule="all_done",  # always runs, even if upstream tasks fail
    )

    # Sequential: orders → customers → products → monthly_sales → slack
    for i in range(len(checkpoint_tasks) - 1):
        checkpoint_tasks[i] >> checkpoint_tasks[i + 1]

    checkpoint_tasks[-1] >> slack_notify
