# data-quality-observability

> **Portfolio Layer 4** — Data Quality & Observability on top of the Olist ecommerce stack.
> Connects directly to [`ecommerce-modern-stack`](https://github.com/adrianopsf/ecommerce-modern-stack) and validates the four mart tables produced by its dbt models.

[![CI](https://github.com/adrianopsf/data-quality-observability/actions/workflows/ci.yml/badge.svg)](https://github.com/adrianopsf/data-quality-observability/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-0.18-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.8-red)
![Grafana](https://img.shields.io/badge/Grafana-10-yellow)

---

## Architecture & Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ecommerce-modern-stack                              │
│  PostgreSQL 15                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │  schema: mart                                                      │     │
│  │  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────────┐ │     │
│  │  │ mart_orders  │  │ mart_customers   │  │    mart_products     │ │     │
│  │  └──────┬───────┘  └────────┬─────────┘  └──────────┬───────────┘ │     │
│  │         │                   │                        │             │     │
│  │         └───────────────────┼────────────────────────┘             │     │
│  │                             │                                      │     │
│  │                  ┌──────────┴──────────┐                           │     │
│  │                  │  mart_monthly_sales │                           │     │
│  │                  └─────────────────────┘                           │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │  SQL queries
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Great Expectations 0.18                                  │
│                                                                             │
│  Expectation Suites          Checkpoints                                    │
│  ┌──────────────────┐        ┌──────────────────────────────────────────┐   │
│  │ mart_orders_suite│──────▶ │ mart_orders_checkpoint                   │   │
│  │ mart_customers.. │──────▶ │ mart_customers_checkpoint                │   │
│  │ mart_products..  │──────▶ │ mart_products_checkpoint                 │   │
│  │ mart_monthly..   │──────▶ │ mart_monthly_sales_checkpoint            │   │
│  └──────────────────┘        └──────────────────────────────────────────┘   │
│                                           │                                 │
│                              Data Docs → localhost:8090                     │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │  run_checkpoint()
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       Apache Airflow 2.8                                    │
│                                                                             │
│  olist_dq_validation (@daily)                                               │
│  ┌────────────┐   ┌──────────────┐   ┌─────────────┐   ┌────────────────┐  │
│  │run_orders_ │──▶│run_customers_│──▶│run_products_│──▶│run_monthly_    │  │
│  │checkpoint  │   │checkpoint    │   │checkpoint   │   │sales_checkpoint│  │
│  └────────────┘   └──────────────┘   └─────────────┘   └───────┬────────┘  │
│                                                                 │           │
│                                                    ┌────────────▼─────────┐ │
│                                                    │notify_slack_on_failure│ │
│                                                    └────────────┬─────────┘ │
│                                                                 │           │
│  olist_dq_report (@weekly)                                      │           │
│  ┌──────────────────────┐   ┌──────────────────────┐            │           │
│  │collect_weekly_results│──▶│   send_weekly_report  │           │           │
│  └──────────────────────┘   └──────────┬────────────┘           │           │
└─────────────────────────────────────────┼──────────────────────-┼───────────┘
                                          │                        │
                    ┌─────────────────────┘                        │
                    ▼                                              ▼
┌───────────────────────────────┐           ┌──────────────────────────────────┐
│       Slack Channel           │           │     SlackNotifier                │
│  #data-quality-alerts         │◀──────────│  send_alert(table, expectation,  │
│                               │           │    column, unexpected_count)     │
│  🚨 Alert: mart_orders        │           │  send_report(summary_dict)       │
│     Column: order_id          │           └──────────────────────────────────┘
│     Failed: NOT NULL          │
│     Unexpected rows: 5        │
└───────────────────────────────┘
                    ▲
                    │  metrics
┌───────────────────────────────────────────────────────────────────────────┐
│   Prometheus + StatsD Exporter + postgres_exporter                        │
│              │                                                            │
│              ▼                                                            │
│   ┌──────────────────────────────┐                                        │
│   │  Grafana — localhost:3000    │                                        │
│   │  Dashboard: DQ Observability │                                        │
│   │  • Success rate by table     │                                        │
│   │  • Row volume over time      │                                        │
│   │  • Failure history           │                                        │
│   └──────────────────────────────┘                                        │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- Docker & Docker Compose v2
- `make`
- Python 3.11 (for local dev / `make setup-ge`)
- A running [`ecommerce-modern-stack`](https://github.com/adrianopsf/ecommerce-modern-stack) instance **or** a PostgreSQL 15 database with the `mart` schema

---

## Connecting to ecommerce-modern-stack

This project is designed to sit on top of `ecommerce-modern-stack`. The two projects share the same PostgreSQL instance.

**Option A — shared Docker network (recommended)**

Both projects expose a Docker network called `dq-net` / `ecommerce-net`. Link them in `docker-compose.yml`:

```yaml
# In ecommerce-modern-stack/docker-compose.yml — add to external networks:
networks:
  dq-net:
    external: true
    name: dq-net
```

Then in your `.env` set:

```bash
POSTGRES_HOST=ecommerce-postgres   # container name from ecommerce-modern-stack
POSTGRES_DB=ecommerce
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

**Option B — external PostgreSQL**

Point `POSTGRES_HOST` to your external host and ensure the `mart` schema is accessible.

**Required tables** (created by dbt in `ecommerce-modern-stack`):

```
mart.mart_orders
mart.mart_customers
mart.mart_products
mart.mart_monthly_sales
```

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/adrianopsf/data-quality-observability.git
cd data-quality-observability

# 2. Configure
cp .env.example .env
# Edit .env: set POSTGRES_* and SLACK_WEBHOOK_URL

# 3. Start the stack
make up

# 4. Create GE expectation suites
make setup-ge

# 5. Trigger a manual DQ run
make run-checks

# 6. Run tests
make test
```

---

## Services & Ports

| Service            | URL                          | Credentials     |
|--------------------|------------------------------|-----------------|
| Airflow Webserver  | http://localhost:8080        | admin / admin   |
| Grafana            | http://localhost:3000        | admin / admin   |
| Prometheus         | http://localhost:9090        | —               |
| GE Data Docs       | http://localhost:8090        | —               |
| PostgreSQL         | localhost:5432               | see `.env`      |

---

## Expectation Suites

Each mart table has a dedicated GE suite with the following checks:

| Check | mart_orders | mart_customers | mart_products | mart_monthly_sales |
|-------|:-----------:|:--------------:|:-------------:|:------------------:|
| PK not null | ✅ | ✅ | ✅ | ✅ |
| PK unique | ✅ | ✅ | ✅ | ✅ |
| Row count > 0 | ✅ | ✅ | ✅ | ✅ |
| Numeric ranges | ✅ | ✅ | ✅ | ✅ |
| Value sets | ✅ (status) | — | — | — |
| Freshness (24h) | ✅ | ✅ | ✅ | ✅ (prev month) |

Run `python scripts/setup_expectations.py` to create or refresh all suites programmatically.

---

## Interpreting Slack Alerts

### 🚨 Alert message

```
🚨 Data Quality Alert

Table:          mart.mart_orders
Run Date:       2024-06-01
Failed Check:   Column Values To Not Be Null
Column:         order_id
Unexpected Rows: 42
```

**What to do:**

- **"Column Values To Not Be Null"** on a PK column → upstream pipeline produced NULL keys. Check the `ecommerce-modern-stack` dbt run for errors in the `stg_orders` model.
- **"Column Values To Be Unique"** on a PK → duplicate records were loaded. Inspect deduplication logic in the staging layer.
- **"Table Row Count To Be Between"** → table is empty. Check if the Airflow ETL DAG failed or if the source data feed was interrupted.
- **"Column Max To Be Between"** (freshness) → no new data in the last 24 hours. Check the ingestion pipeline and database connectivity.

### ✅ / ⚠️ Weekly Report

The weekly report (every Sunday 08:00 UTC) summarises all daily runs over the past 7 days. A `⚠️` header means at least one run failed during the week — drill down by clicking the GE Data Docs link embedded in the message.

---

## Importing the Grafana Dashboard

1. Navigate to **Grafana → Dashboards → Import** (http://localhost:3000/dashboard/import).
2. Click **"Upload JSON file"** and select `monitoring/grafana/dashboards/dq_dashboard.json`.
3. Select the **Prometheus** data source from the dropdown.
4. Click **Import**.

The dashboard auto-provisions if you start Grafana with the Docker Compose stack — no manual import needed.

**Dashboard panels:**

- **Overall DQ Success Rate (%)** — Gauge showing the aggregate pass rate across all checkpoints.
- **Failed DAG Runs (Today)** — Stat panel, red background if > 0.
- **Success Rate by Table** — One gauge per mart table.
- **Table Row Volume by Day** — Time-series using `pg_stat_user_tables` from `postgres_exporter`.
- **DQ Failures Over Time** — Stacked bar chart of daily failures per checkpoint.

---

## Project Structure

```
data-quality-observability/
├── alerts/
│   └── slack_notifier.py         # SlackNotifier class
├── airflow/
│   └── dags/
│       ├── dq_validation_dag.py  # @daily checkpoint runner
│       └── dq_report_dag.py      # @weekly Slack report
├── great_expectations/
│   ├── great_expectations.yml
│   ├── expectations/             # 4 expectation suite JSONs
│   └── checkpoints/              # 4 checkpoint YAMLs
├── monitoring/
│   ├── grafana/
│   │   ├── dashboards/           # dq_dashboard.json
│   │   └── provisioning/        # Auto-provisioning configs
│   └── prometheus/
│       └── prometheus.yml
├── scripts/
│   └── setup_expectations.py    # Programmatic suite creation
├── tests/
│   ├── test_slack_notifier.py
│   ├── test_checkpoints.py
│   └── test_dags.py
├── .github/workflows/ci.yml
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── requirements.txt
```

---

## Development

```bash
# Install dependencies
make install-dev

# Run lint
make lint

# Auto-fix formatting
make format

# Run tests with coverage
make test
```

---

## Portfolio Context

This project is **Layer 4** of a data engineering portfolio built on the [Brazilian Olist ecommerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce):

| Layer | Project | Focus |
|-------|---------|-------|
| 1 | [retail-etl-pipeline](https://github.com/adrianopsf/retail-etl-pipeline) | Batch ETL with Python + PostgreSQL |
| 2 | [document-intelligence-pipeline](https://github.com/adrianopsf/document-intelligence-pipeline) | Unstructured data processing |
| 3 | [ecommerce-modern-stack](https://github.com/adrianopsf/ecommerce-modern-stack) | dbt + Airflow + modern data stack |
| **4** | **data-quality-observability** | **Great Expectations + Grafana + Slack** |

---

## License

MIT
