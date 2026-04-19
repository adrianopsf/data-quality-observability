"""test_dags.py — Airflow DAG loading and structure tests.

Verifies that both DAGs can be parsed without errors and have the expected
tasks, schedule, and tags.

When Airflow IS installed (CI / production), the real Airflow runtime is used.
When Airflow is NOT installed (lightweight local dev), the tests are skipped
gracefully so the rest of the suite still runs.
"""

from __future__ import annotations

import importlib
import pathlib
import sys
from unittest.mock import MagicMock

import pytest

# Paths are set by conftest.py at the project root (sys.path.append, not insert,
# so that real apache-airflow in site-packages wins over local airflow/ dir).
DAG_DIR = pathlib.Path(__file__).parent.parent / "airflow" / "dags"
ROOT_DIR = pathlib.Path(__file__).parent.parent

# ─────────────── detect Airflow availability ─────────────────────────────────

try:
    from airflow import DAG as _AirflowDAG  # noqa: F401

    AIRFLOW_AVAILABLE = True
except (ImportError, AttributeError):
    AIRFLOW_AVAILABLE = False

requires_airflow = pytest.mark.skipif(
    not AIRFLOW_AVAILABLE,
    reason="apache-airflow not installed — DAG loading tests skipped in lightweight env",
)

# ──────────────────────────── mock helpers ────────────────────────────────────


def _build_airflow_mocks() -> dict:
    """Return a minimal set of sys.modules mocks that make Airflow DAGs importable.

    Returns:
        Dict of module name → MagicMock for patch.dict(sys.modules, ...).
    """
    # We need a real-ish DAG object to inspect tasks/schedule/tags
    # Use actual Airflow if available; otherwise fall back to a structural mock
    if AIRFLOW_AVAILABLE:
        return {}  # nothing to mock — use real Airflow

    dag_mock = MagicMock()
    op_mock = MagicMock()

    airflow_mods: dict = {
        "airflow": MagicMock(),
        "airflow.models": MagicMock(),
        "airflow.operators": MagicMock(),
        "airflow.operators.python": MagicMock(),
    }
    airflow_mods["airflow"].DAG = dag_mock
    airflow_mods["airflow.operators.python"].PythonOperator = op_mock
    return airflow_mods


# ─────────────────────────── DAG loading tests ────────────────────────────────


@requires_airflow
class TestDqValidationDag:
    """Tests for olist_dq_validation DAG (requires Airflow installed)."""

    @pytest.fixture(autouse=True)
    def _import_dag(self) -> None:
        """Import the validation DAG module with GE mocked."""
        ge_mock = MagicMock()
        with pytest.MonkeyPatch.context() as mp:
            mp.setitem(sys.modules, "great_expectations", ge_mock)
            mp.setitem(sys.modules, "great_expectations.data_context", ge_mock)
            if "dq_validation_dag" in sys.modules:
                del sys.modules["dq_validation_dag"]
            self.module = importlib.import_module("dq_validation_dag")

    def test_dag_loads_without_error(self) -> None:
        """The DAG module must be importable without raising exceptions."""
        assert hasattr(self.module, "dag")

    def test_dag_id(self) -> None:
        """DAG id must be 'olist_dq_validation'."""
        assert self.module.dag.dag_id == "olist_dq_validation"

    def test_dag_has_correct_schedule(self) -> None:
        """DAG schedule must be @daily."""
        assert self.module.dag.schedule_interval == "@daily"

    def test_dag_tags(self) -> None:
        """DAG must have expected tags."""
        tags = self.module.dag.tags
        assert "data-quality" in tags
        assert "great-expectations" in tags

    def test_checkpoint_tasks_present(self) -> None:
        """One task per checkpoint must exist in the DAG."""
        task_ids = {t.task_id for t in self.module.dag.tasks}
        expected = {
            "run_mart_orders_checkpoint",
            "run_mart_customers_checkpoint",
            "run_mart_products_checkpoint",
            "run_mart_monthly_sales_checkpoint",
        }
        assert expected.issubset(task_ids), f"Missing tasks: {expected - task_ids}"

    def test_slack_notify_task_present(self) -> None:
        """Slack notification task must be in the DAG."""
        task_ids = {t.task_id for t in self.module.dag.tasks}
        assert "notify_slack_on_failure" in task_ids

    def test_task_count(self) -> None:
        """DAG must have exactly 5 tasks (4 checkpoints + 1 slack)."""
        assert len(self.module.dag.tasks) == 5

    def test_catchup_is_false(self) -> None:
        """Catchup must be disabled to avoid back-filling on first deploy."""
        assert self.module.dag.catchup is False


@requires_airflow
class TestDqReportDag:
    """Tests for olist_dq_report DAG (requires Airflow installed)."""

    @pytest.fixture(autouse=True)
    def _import_dag(self) -> None:
        """Import the report DAG module."""
        if "dq_report_dag" in sys.modules:
            del sys.modules["dq_report_dag"]
        self.module = importlib.import_module("dq_report_dag")

    def test_dag_loads_without_error(self) -> None:
        """The DAG module must be importable without raising exceptions."""
        assert hasattr(self.module, "dag")

    def test_dag_id(self) -> None:
        """DAG id must be 'olist_dq_report'."""
        assert self.module.dag.dag_id == "olist_dq_report"

    def test_dag_has_weekly_schedule(self) -> None:
        """DAG schedule must be weekly (Sunday 08:00)."""
        assert self.module.dag.schedule_interval == "0 8 * * 0"

    def test_dag_tags(self) -> None:
        """DAG must have expected tags."""
        tags = self.module.dag.tags
        assert "data-quality" in tags
        assert "reporting" in tags

    def test_tasks_present(self) -> None:
        """Both collect and report tasks must be present."""
        task_ids = {t.task_id for t in self.module.dag.tasks}
        assert "collect_weekly_results" in task_ids
        assert "send_weekly_report" in task_ids

    def test_task_count(self) -> None:
        """DAG must have exactly 2 tasks."""
        assert len(self.module.dag.tasks) == 2

    def test_catchup_is_false(self) -> None:
        """Catchup must be disabled."""
        assert self.module.dag.catchup is False

    def test_task_dependency(self) -> None:
        """collect_weekly_results must run before send_weekly_report."""
        dag = self.module.dag
        collect_task = dag.get_task("collect_weekly_results")
        report_task = dag.get_task("send_weekly_report")
        assert report_task.task_id in [t.task_id for t in collect_task.downstream_list]


# ─────────────────── DAG file syntax tests (no Airflow needed) ───────────────


class TestDagFileSyntax:
    """Pure Python AST checks — run without Airflow installed."""

    @pytest.mark.parametrize(
        "dag_file",
        [
            "dq_validation_dag.py",
            "dq_report_dag.py",
            "checkpoint_runner.py",
        ],
    )
    def test_dag_file_is_valid_python(self, dag_file: str) -> None:
        """Each DAG file must parse as valid Python (AST check)."""
        import ast

        path = DAG_DIR / dag_file
        assert path.exists(), f"DAG file missing: {dag_file}"
        source = path.read_text()
        try:
            ast.parse(source)
        except SyntaxError as exc:
            pytest.fail(f"{dag_file} has a syntax error: {exc}")

    def test_validation_dag_defines_checkpoints_list(self) -> None:
        """dq_validation_dag.py must define a CHECKPOINTS list with 4 entries."""
        import ast

        source = (DAG_DIR / "dq_validation_dag.py").read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            # Handle both plain Assign and type-annotated AnnAssign
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "CHECKPOINTS":
                        assert isinstance(node.value, ast.List), "CHECKPOINTS must be a list"
                        assert len(node.value.elts) == 4, (
                            f"Expected 4 checkpoints, got {len(node.value.elts)}"
                        )
                        return
            elif isinstance(node, ast.AnnAssign):
                if isinstance(node.target, ast.Name) and node.target.id == "CHECKPOINTS":
                    assert node.value is not None, "CHECKPOINTS must have a value"
                    assert isinstance(node.value, ast.List), "CHECKPOINTS must be a list"
                    assert len(node.value.elts) == 4, (
                        f"Expected 4 checkpoints, got {len(node.value.elts)}"
                    )
                    return
        pytest.fail("CHECKPOINTS list not found in dq_validation_dag.py")

    def test_checkpoint_runner_defines_run_checkpoint(self) -> None:
        """checkpoint_runner.py must define a run_checkpoint function."""
        import ast

        source = (DAG_DIR / "checkpoint_runner.py").read_text()
        tree = ast.parse(source)
        func_names = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
        assert "run_checkpoint" in func_names, (
            "run_checkpoint function not found in checkpoint_runner.py"
        )
