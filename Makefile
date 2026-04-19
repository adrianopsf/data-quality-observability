# ─────────────────────────────────────────────────────────────────────────────
# data-quality-observability — Makefile
# ─────────────────────────────────────────────────────────────────────────────
.PHONY: up down setup-ge run-checks test lint help

PYTHON   := python3
PIP      := pip
COMPOSE  := docker compose

# ── Infrastructure ──────────────────────────────────────────────────────────

## up: Start all services (PostgreSQL, Airflow, Grafana, Prometheus)
up:
	cp -n .env.example .env 2>/dev/null || true
	$(COMPOSE) up -d --build
	@echo ""
	@echo "✅  Stack is running:"
	@echo "   Airflow    → http://localhost:8080  (admin/admin)"
	@echo "   Grafana    → http://localhost:3000  (admin/admin)"
	@echo "   Prometheus → http://localhost:9090"
	@echo "   GE Docs    → http://localhost:8090  (after make setup-ge)"

## down: Stop and remove all containers and networks
down:
	$(COMPOSE) down --volumes --remove-orphans

## logs: Tail logs from all services
logs:
	$(COMPOSE) logs -f

# ── Great Expectations ───────────────────────────────────────────────────────

## setup-ge: Create all expectation suites programmatically
setup-ge:
	$(PYTHON) scripts/setup_expectations.py
	@echo "✅  Expectation suites created."

## run-checks: Run all GE checkpoints via Airflow CLI (requires running stack)
run-checks:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger olist_dq_validation
	@echo "✅  olist_dq_validation DAG triggered."

# ── Quality ──────────────────────────────────────────────────────────────────

## lint: Run ruff linter + formatter check
lint:
	$(PYTHON) -m ruff check alerts/ scripts/ airflow/dags/ tests/
	$(PYTHON) -m ruff format --check alerts/ scripts/ airflow/dags/ tests/
	@echo "✅  Lint passed."

## format: Auto-fix lint issues
format:
	$(PYTHON) -m ruff check --fix alerts/ scripts/ airflow/dags/ tests/
	$(PYTHON) -m ruff format alerts/ scripts/ airflow/dags/ tests/

## typecheck: Run mypy type checker
typecheck:
	$(PYTHON) -m mypy alerts/ scripts/

## test: Run pytest with coverage
test:
	$(PYTHON) -m pytest tests/ -v --cov=alerts --cov=scripts --cov-report=term-missing

## test-ci: Run pytest in CI mode (no .env needed)
test-ci:
	$(PYTHON) -m pytest tests/ -v --cov=alerts --cov=scripts \
		--cov-report=xml:coverage.xml --cov-report=term-missing

# ── Dependencies ─────────────────────────────────────────────────────────────

## install: Install Python dependencies
install:
	$(PIP) install -r requirements.txt

## install-dev: Install dev dependencies
install-dev: install
	$(PIP) install ruff mypy pytest pytest-cov pytest-mock responses

# ── Help ─────────────────────────────────────────────────────────────────────

## help: Show this help message
help:
	@grep -E '^##' Makefile | sed 's/^## //'
