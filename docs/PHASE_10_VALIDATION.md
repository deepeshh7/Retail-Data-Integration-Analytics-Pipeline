# Phase 10 — Final validation and interview readiness

## Acceptance matrix

The executable acceptance runner is `scripts/verify_phase10.py`. It creates a uniquely named database in the PostgreSQL instance named by the current environment, initializes it with `sql/schema.sql`, and uses five tiny CSV batches. It refuses to reuse a database name. It never points the pipeline at the existing `retail` database and does not automatically drop its scratch database or artifacts.

| Scenario | Test action | Expected result / evidence |
| --- | --- | --- |
| Fresh pipeline | Load batch 1 with two valid line IDs | Source, Bronze, Silver, and all Gold Parquet counts reconcile; 2 facts, 1 batch, daily/product/country rows are 1/2/1, net revenue 8.00 |
| Same batch twice | Force the same fingerprint through a second invocation | PostgreSQL ledger skips the already committed batch; fact and batch counts remain 2 and 1 |
| New batch | Send a correction to one stable line and one new line; omit an older unchanged line | Audit shows only 2 incoming Silver rows; current state has 3 unique lines, updated quantity 3, retains the omitted older line, and totals 13.50 |
| Mid-pipeline failure | A test-only PostgreSQL trigger aborts the fact upsert after the batch row and staging work have begun | Run audit says failed at `warehouse`; transaction rollback leaves prior facts, Gold, and batch ledger unchanged |
| Recovery | Remove the test trigger and retry the exact failed source | The source applies once; state becomes 4 unique lines, 3 batches, revenue 21.50 |
| Invalid records | Send one valid row, one nonnumeric quantity, and one missing country | 1 accepted, 2 quarantined with reason codes, 2 rows in quarantine Parquet; invalid lines do not reach facts |
| Additive schema | Add a `Discount` column to a valid source | Audit records `Discount`; the existing schema-evolution fixture verifies the extra value survives into Silver; revenue remains based on the current explicit `quantity * unit_price` contract |
| Historical backfill | Replay batch 1 twice after later updates and new batches | The later correction stays current; no duplicate facts or batch records appear; all three Gold revenue totals still match facts |
| Warehouse queries | Compare fact revenue with daily, product, and country aggregate sums at each checkpoint | All Gold totals equal fact net revenue; exact row counts and corrected quantity match expected values |

The trigger is created only inside the scratch database and removed before recovery. If the verifier stops unexpectedly, that isolated database and its files are retained so the result can be inspected; the existing Windows `retail` database is not used.

## Run the final checks in WSL

Start Ubuntu from PowerShell with `wsl -d Ubuntu`, then run from the project directory:

```bash
cd /mnt/c/Users/deepe/Documents/Codex/2026-10-25/referenced-chatgpt-conversation-this-is-an/work/retail-data-pipeline
source ~/airflow-retail/.venv/bin/activate
python -m pytest
python -m pytest airflow/tests
```

For the disposable PostgreSQL check, start the WSL PostgreSQL service and set the local peer-authenticated role created during Phase 9 verification:

```bash
sudo service postgresql start
export PGHOST=/var/run/postgresql
export PGUSER=deepesh
export PGDATABASE=postgres
export PGPASSWORD=
python scripts/verify_phase10.py
```

The role must have `CREATEDB`; the local PostgreSQL cluster should use peer authentication for the socket. The script overrides only `PGDATABASE` for each pipeline process to use its generated scratch database. It records the database name and artifact path when it passes. Review them before any manual cleanup.

## Verification status

**Passed on 2026-10-31 in WSL:** `python -m pytest` reported **11 passed in 31.97s**; `python -m pytest airflow/tests` reported **1 passed in 4.53s**; and `scripts/verify_phase10.py` ended with `PHASE 10 END-TO-END VERIFICATION PASSED`.

The isolated database was `retail_phase10_verify_20261031100043379406`. The run verified a fresh 2-line load at revenue 8.0000, a forced repeat with no new facts or batch, an update plus new line retaining an untouched earlier line, transaction rollback and successful retry, two quarantined rows with expected reasons, preservation of an additive `Discount` column without changing revenue, and two safe historical replays. The final warehouse held 6 unique lines across 5 batches; fact, daily, product, and country revenue reconciled at 34.5000. The configured Windows `retail` database was not used.

Phase 9's full-dataset benchmark completed all eight trials, and its isolated backfill verifier passed separately.

## Limitations to state in interviews

- This is a local batch demonstration on one machine and one public retail dataset, not distributed-scale evidence.
- The UCI source has no immutable row ID; changed lines cannot be inferred as updates without a source-provided `LineID`.
- Additive fields are preserved as extras but do not affect measures until they are explicitly modeled and validated.
- Spark sends staged rows through the driver; larger volumes need a measured bulk-staging/database-merge strategy.
- PostgreSQL and Parquet cannot share a single cross-system transaction. The ledger makes warehouse retries safe and Gold Parquet is recoverable from PostgreSQL, but a stale Parquet snapshot can exist between a database commit and its subsequent refresh.
- There is no cloud deployment, lakehouse table format/catalog, centralized secrets manager, or external alerting.
- The 4% Spark timing gain is specific to the local 541,909-row dataset and WSL filesystem benchmark; it is not a universal tuning rule.
