"""checkpoint_runner.py — Pure-Python GE checkpoint execution logic.

Isolated from Airflow so it can be unit-tested without the Airflow runtime.
Imported by dq_validation_dag.py at task execution time (inside the callable).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

GE_ROOT = Path(__file__).parent.parent.parent / "great_expectations"


def run_checkpoint(checkpoint_name: str, run_id: str) -> dict[str, Any]:
    """Run a single GE checkpoint and return a structured summary.

    Args:
        checkpoint_name: Name of the GE checkpoint to run.
        run_id: Unique identifier for this run (e.g. ``"20240601-mart-orders"``).

    Returns:
        Dictionary with keys:
            - ``success`` (bool)
            - ``checkpoint`` (str)
            - ``failed_expectations`` (list[dict])
            - ``evaluated_expectations`` (int)
            - ``successful_expectations`` (int)
            - ``unsuccessful_expectations`` (int)
    """
    from great_expectations.data_context import FileDataContext  # noqa: PLC0415

    logger.info("Running checkpoint: {}", checkpoint_name)
    ge_context = FileDataContext(context_root_dir=str(GE_ROOT))
    result = ge_context.run_checkpoint(checkpoint_name=checkpoint_name, run_id=run_id)

    failed: list[dict[str, Any]] = []
    stats: dict[str, int] = {}

    for validation_result in result.run_results.values():
        vr = validation_result["validation_result"]
        stats = vr.get("statistics", {})
        if not vr["success"]:
            for exp_result in vr["results"]:
                if not exp_result["success"]:
                    failed.append(
                        {
                            "expectation": exp_result["expectation_config"]["expectation_type"],
                            "column": exp_result["expectation_config"]["kwargs"].get(
                                "column", "table-level"
                            ),
                            "unexpected_count": exp_result["result"].get("unexpected_count", "N/A"),
                        }
                    )

    summary: dict[str, Any] = {
        "success": result.success,
        "checkpoint": checkpoint_name,
        "failed_expectations": failed,
        "evaluated_expectations": stats.get("evaluated_expectations", 0),
        "successful_expectations": stats.get("successful_expectations", 0),
        "unsuccessful_expectations": stats.get("unsuccessful_expectations", 0),
    }

    logger.info(
        "Checkpoint {} — success={}, failed={}",
        checkpoint_name,
        result.success,
        len(failed),
    )
    return summary
