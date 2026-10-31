# Phase 8 — Orchestration and automation

## Workflow

The `retail_online_retail` DAG runs three ordered tasks:

1. `ingest_source` fingerprints the selected XLSX/CSV input and writes Bronze Parquet.
2. `transform_and_quarantine` validates Bronze rows, writes rejected rows with reason codes, and writes a fingerprinted Silver batch.
3. `load_warehouse_and_publish_gold` idempotently updates PostgreSQL (when enabled), writes/reconciles Gold outputs, and marks the source fingerprint complete.

Tasks pass only small metadata dictionaries through Airflow XCom; Parquet and warehouse data stay in the existing storage layers. A failed task is retried twice with exponential delay. `max_active_runs=1` prevents two DAG runs from racing on the same local output paths.

## Reruns and backfills

- Each Airflow run gets a stable, path-safe run ID. Task retries use that same ID.
- The source SHA-256 is the batch ID. Completed fingerprints are skipped before Spark starts unless the DAG run is triggered with `{"force": true}`.
- PostgreSQL's transactional batch ledger protects forced retries after a warehouse commit. If the database commit succeeds but a later snapshot or manifest write fails, a retry refreshes Gold from PostgreSQL without applying duplicate lines.
- Local fallback Silver/Gold is rebuilt with stable line IDs, so retrying the same source does not create duplicate rows.
- Backfills must select the appropriate retained source file per run, for example by triggering with `{"input_path": "/absolute/path/to/online_retail_2025_01.xlsx"}`. A backfill of the same file is safely skipped after completion. Historical data must be retained as separate source files; Airflow cannot reconstruct snapshots that were overwritten.

The DAG is scheduled daily with `catchup=False`. It reads `INPUT_PATH` (or the default project input path); unchanged content is skipped. For backfills or one-off reprocessing, supply `input_path` and optionally `force` in the DAG run configuration.

## Local Windows setup

Apache Airflow does not run natively on Windows; use WSL2. Keep Airflow in a separate Linux virtual environment from the Windows project `.venv`. Airflow's installation guide recommends WSL2 on Windows and a version-specific constraints file for reproducible installation: [Airflow Quick Start](https://airflow.apache.org/docs/apache-airflow/stable/start.html), [Airflow installation](https://airflow.apache.org/docs/apache-airflow/stable/installation.html).

From WSL2, install Airflow and the project in a Linux Python 3.10 environment. Use the matching constraints URL:

```bash
python3.10 -m venv ~/venvs/retail-airflow
source ~/venvs/retail-airflow/bin/activate
python -m pip install --upgrade pip
python -m pip install "apache-airflow==3.3.2" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.10.txt"
python -m pip install -e /path/to/retail-data-pipeline
```

Point Airflow and the pipeline at absolute paths visible inside WSL2. Set the database connection variables in the Airflow process environment; do not put passwords in the DAG or Git. Example:

```bash
export AIRFLOW_HOME="$HOME/airflow-retail"
export AIRFLOW__CORE__DAGS_FOLDER="/path/to/retail-data-pipeline/airflow/dags"
export INPUT_PATH="/path/to/retail-data-pipeline/data/input/online_retail.xlsx"
export DATA_DIR="/path/to/retail-data-pipeline/data"
export LOG_DIR="/path/to/retail-data-pipeline/logs"
export WAREHOUSE_ENABLED=true
export PGHOST=127.0.0.1
export PGPORT=5432
export PGDATABASE=retail
export PGUSER=postgres
export PGPASSWORD="your-local-password"
airflow standalone
```

Open the Airflow UI at `http://localhost:8080`, enable `retail_online_retail`, then trigger it. Confirm PostgreSQL host connectivity from WSL2 before enabling the warehouse target. Airflow standalone is for local development, not production deployment.

## GitHub Actions

`.github/workflows/ci.yml` runs on pushes, pull requests, and manual dispatch. It has two jobs:

- `pipeline-tests`: Python 3.10 + Java 17, installs the project dev extra, and runs the existing Spark/test suite on Ubuntu.
- `airflow-dag`: installs Apache Airflow 3.3.2 using the official Python 3.10 constraints and checks DAG imports, task dependencies, retries, and concurrency settings.

No PostgreSQL server or source workbook is required in CI; unit tests use small in-memory data.
