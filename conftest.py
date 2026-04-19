"""conftest.py — pytest configuration for data-quality-observability.

Adds the required paths to sys.path in the correct order so that:
  1. The real `apache-airflow` package (in site-packages) is found BEFORE
     the local `airflow/` directory.
  2. `checkpoint_runner` is importable from `airflow/dags/`.
  3. The `alerts` package is importable from the project root.
"""

from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).parent
DAGS = ROOT / "airflow" / "dags"

# airflow/dags → importable as a flat namespace (checkpoint_runner, etc.)
if str(DAGS) not in sys.path:
    sys.path.append(str(DAGS))

# Project root → importable for `alerts` package.
# Appended (not prepended) so that site-packages / real airflow wins.
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
