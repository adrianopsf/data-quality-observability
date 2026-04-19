"""test_checkpoints.py — Unit tests for GE checkpoint execution logic.

Tests target checkpoint_runner.run_checkpoint() directly, so neither
Great Expectations nor Airflow need to be installed.  GE is mocked via
sys.modules injection.
"""

from __future__ import annotations

import pathlib
import sys
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import pytest

# Paths are set by conftest.py at the project root (sys.path.append, not insert,
# so that real apache-airflow in site-packages wins over local airflow/ dir).
ROOT_DIR = pathlib.Path(__file__).parent.parent

# ──────────────────────────────── helpers ─────────────────────────────────────


def _make_ge_result(
    success: bool,
    failed_expectations: list[dict] | None = None,
) -> MagicMock:
    """Build a mock CheckpointResult that mimics the GE API surface.

    Args:
        success: Whether the checkpoint overall passed.
        failed_expectations: List of failed expectation dicts.

    Returns:
        MagicMock resembling a CheckpointResult.
    """
    failed_expectations = failed_expectations or []

    result_dicts: list[dict[str, Any]] = []
    for exp in failed_expectations:
        result_dicts.append(
            {
                "success": False,
                "expectation_config": {
                    "expectation_type": exp["expectation_type"],
                    "kwargs": {"column": exp.get("column", "table-level")},
                },
                "result": {"unexpected_count": exp.get("unexpected_count", 0)},
            }
        )

    validation_result = {
        "success": success,
        "results": result_dicts,
        "statistics": {
            "evaluated_expectations": len(result_dicts) + 5,
            "successful_expectations": 5,
            "unsuccessful_expectations": len(result_dicts),
        },
    }

    mock_result = MagicMock()
    mock_result.success = success
    mock_result.run_results = {"validation_key": {"validation_result": validation_result}}
    return mock_result


def _mock_ge_context(ge_result: MagicMock) -> dict[str, ModuleType]:
    """Return sys.modules patches that make GE importable with a mocked context.

    Args:
        ge_result: The mock CheckpointResult to return from run_checkpoint.

    Returns:
        Dict of module name → MagicMock suitable for patch.dict(sys.modules, ...).
    """
    mock_ctx_instance = MagicMock()
    mock_ctx_instance.run_checkpoint.return_value = ge_result

    mock_ge_ctx_mod = MagicMock()
    mock_ge_ctx_mod.FileDataContext.return_value = mock_ctx_instance

    return {
        "great_expectations": MagicMock(),
        "great_expectations.data_context": mock_ge_ctx_mod,
    }


# ─────────────────────────────── test cases ───────────────────────────────────


class TestRunCheckpoint:
    """Tests for checkpoint_runner.run_checkpoint()."""

    def _call(
        self,
        checkpoint_name: str,
        ge_result: MagicMock,
    ) -> dict[str, Any]:
        """Call run_checkpoint with GE mocked.

        Args:
            checkpoint_name: Checkpoint name to pass through.
            ge_result: Pre-built mock result.

        Returns:
            Summary dict from run_checkpoint.
        """
        from unittest.mock import patch

        fake_modules = _mock_ge_context(ge_result)
        with patch.dict(sys.modules, fake_modules):
            # Force fresh import so it picks up the patched sys.modules
            if "checkpoint_runner" in sys.modules:
                del sys.modules["checkpoint_runner"]
            from checkpoint_runner import run_checkpoint  # type: ignore[import]

            return run_checkpoint(
                checkpoint_name=checkpoint_name,
                run_id=f"2024-06-01-{checkpoint_name}",
            )

    def test_successful_checkpoint_returns_success_true(self) -> None:
        """A passing checkpoint should return summary with success=True."""
        result = self._call(
            checkpoint_name="mart_orders_checkpoint",
            ge_result=_make_ge_result(success=True),
        )
        assert result["success"] is True
        assert result["checkpoint"] == "mart_orders_checkpoint"
        assert result["failed_expectations"] == []

    def test_failed_checkpoint_returns_success_false(self) -> None:
        """A failing checkpoint should return summary with success=False."""
        failed = [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "column": "order_id",
                "unexpected_count": 3,
            }
        ]
        result = self._call(
            checkpoint_name="mart_orders_checkpoint",
            ge_result=_make_ge_result(success=False, failed_expectations=failed),
        )
        assert result["success"] is False
        assert len(result["failed_expectations"]) == 1
        assert result["failed_expectations"][0]["expectation"] == (
            "expect_column_values_to_not_be_null"
        )

    def test_failed_checkpoint_captures_unexpected_count(self) -> None:
        """Failed expectations should include the unexpected_count."""
        failed = [
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "column": "customer_id",
                "unexpected_count": 42,
            }
        ]
        result = self._call(
            checkpoint_name="mart_customers_checkpoint",
            ge_result=_make_ge_result(success=False, failed_expectations=failed),
        )
        assert result["failed_expectations"][0]["unexpected_count"] == 42

    def test_checkpoint_statistics_are_included(self) -> None:
        """Summary should include evaluated/successful/unsuccessful counts."""
        result = self._call(
            checkpoint_name="mart_products_checkpoint",
            ge_result=_make_ge_result(success=True),
        )
        assert "evaluated_expectations" in result
        assert "successful_expectations" in result
        assert "unsuccessful_expectations" in result

    def test_multiple_failures_all_captured(self) -> None:
        """All failed expectations must appear in the returned list."""
        failed = [
            {"expectation_type": "expect_column_values_to_not_be_null", "column": "order_id"},
            {"expectation_type": "expect_column_values_to_be_unique", "column": "order_id"},
        ]
        result = self._call(
            checkpoint_name="mart_orders_checkpoint",
            ge_result=_make_ge_result(success=False, failed_expectations=failed),
        )
        assert len(result["failed_expectations"]) == 2


# ──────────────────────────── checkpoint YAML ─────────────────────────────────


class TestCheckpointYaml:
    """Validate checkpoint YAML files exist and reference correct suites."""

    @pytest.fixture()
    def checkpoint_dir(self) -> pathlib.Path:
        """Return the checkpoints directory path."""
        return ROOT_DIR / "great_expectations" / "checkpoints"

    def test_all_checkpoints_exist(self, checkpoint_dir: pathlib.Path) -> None:
        """All four checkpoint YAML files should be present."""
        expected = [
            "mart_orders_checkpoint.yml",
            "mart_customers_checkpoint.yml",
            "mart_products_checkpoint.yml",
            "mart_monthly_sales_checkpoint.yml",
        ]
        for filename in expected:
            assert (checkpoint_dir / filename).exists(), f"Missing: {filename}"

    def test_checkpoint_references_correct_suite(self, checkpoint_dir: pathlib.Path) -> None:
        """Each checkpoint YAML should reference its matching expectation suite."""
        import yaml

        mapping = {
            "mart_orders_checkpoint.yml": "mart_orders_suite",
            "mart_customers_checkpoint.yml": "mart_customers_suite",
            "mart_products_checkpoint.yml": "mart_products_suite",
            "mart_monthly_sales_checkpoint.yml": "mart_monthly_sales_suite",
        }
        for filename, expected_suite in mapping.items():
            path = checkpoint_dir / filename
            with path.open() as fh:
                content = yaml.safe_load(fh)
            validations = content.get("validations", [])
            assert validations, f"{filename} has no validations"
            suite_name = validations[0].get("expectation_suite_name")
            assert suite_name == expected_suite, (
                f"{filename}: expected suite '{expected_suite}', got '{suite_name}'"
            )

    def test_checkpoints_have_run_name_template(self, checkpoint_dir: pathlib.Path) -> None:
        """Each checkpoint should define a run_name_template."""
        import yaml

        for path in checkpoint_dir.glob("*.yml"):
            with path.open() as fh:
                content = yaml.safe_load(fh)
            assert "run_name_template" in content, f"{path.name} is missing run_name_template"
