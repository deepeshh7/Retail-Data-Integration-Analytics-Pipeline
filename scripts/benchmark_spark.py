"""Run repeatable local Spark tuning trials against the full retail source."""

from __future__ import annotations

import csv
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "input" / "online_retail.xlsx"
TRIALS = [
    ("baseline_8_0", 8, 0),
    ("shuffle_4_0", 4, 0),
    ("file_cap_8_100k", 8, 100_000),
    ("shuffle_12_0", 12, 0),
    ("shuffle_12_0", 12, 0),
    ("file_cap_8_100k", 8, 100_000),
    ("shuffle_4_0", 4, 0),
    ("baseline_8_0", 8, 0),
]


def latest_audit(directory: Path) -> dict:
    records = sorted((directory / "audit").glob("run_*.json"), key=lambda p: p.stat().st_mtime)
    if not records:
        raise RuntimeError(f"No audit record was written under {directory / 'audit'}")
    return json.loads(records[-1].read_text(encoding="utf-8"))


def main() -> int:
    if not SOURCE.is_file():
        raise SystemExit(f"Source file not found: {SOURCE}")

    benchmark_root = ROOT / "data" / "bench"
    benchmark_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    result_path = benchmark_root / f"spark_benchmark_{stamp}.csv"
    trial_rows: list[dict] = []
    grouped: dict[str, list[float]] = {}

    print("Spark benchmark: full source, warehouse disabled, independent output per run")
    print(f"Source: {SOURCE}")
    print("Trials: 8 runs (two repeats per configuration); this may take several minutes.\n", flush=True)

    for number, (label, partitions, max_records) in enumerate(TRIALS, start=1):
        trial_name = f"{stamp}_{number:02d}_{label}"
        data_dir = benchmark_root / trial_name
        if data_dir.exists():
            raise SystemExit(f"Refusing to reuse an existing trial directory: {data_dir}")
        log_dir = ROOT / "logs" / "bench" / trial_name
        env = os.environ.copy()
        env.update({
            "DATA_DIR": str(data_dir),
            "LOG_DIR": str(log_dir),
            "WAREHOUSE_ENABLED": "false",
            "SPARK_SHUFFLE_PARTITIONS": str(partitions),
            "SPARK_MAX_RECORDS_PER_FILE": str(max_records),
        })
        log_dir.mkdir(parents=True, exist_ok=True)
        output_log = log_dir / "console.log"
        command = [sys.executable, "-m", "retail_pipeline", "--input", str(SOURCE), "--dry-run"]

        print(f"[{number}/8] {label}: shuffle={partitions}, max_records_per_file={max_records}", flush=True)
        start = time.perf_counter()
        with output_log.open("w", encoding="utf-8") as stream:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=stream,
                                    stderr=subprocess.STDOUT, check=False)
        elapsed = time.perf_counter() - start
        if result.returncode:
            tail = output_log.read_text(encoding="utf-8", errors="replace")[-5000:]
            raise SystemExit(f"Trial {trial_name} failed (exit {result.returncode}). Log: {output_log}\n{tail}")

        audit = latest_audit(data_dir)
        parquet_files = list(data_dir.rglob("*.parquet"))
        row = {
            "trial": trial_name,
            "configuration": label,
            "repeat": 1 if label not in grouped or len(grouped[label]) == 0 else 2,
            "shuffle_partitions": partitions,
            "max_records_per_file": max_records,
            "elapsed_seconds": round(elapsed, 3),
            "pipeline_seconds": audit.get("duration_seconds"),
            "source_rows": audit.get("source"),
            "silver_rows": audit.get("silver"),
            "parquet_file_count": len(parquet_files),
            "parquet_bytes": sum(path.stat().st_size for path in parquet_files),
            "status": audit.get("status"),
            "log": str(output_log),
        }
        trial_rows.append(row)
        grouped.setdefault(label, []).append(elapsed)
        print(f"    elapsed={elapsed:.1f}s; parquet={row['parquet_file_count']} files / {row['parquet_bytes']:,} bytes; status={row['status']}", flush=True)
        # Keep the measured totals and logs, but avoid leaving several complete
        # copies of the source and all layer outputs on the user's disk.
        if data_dir.parent == benchmark_root:
            shutil.rmtree(data_dir)

    columns = list(trial_rows[0])
    with result_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(trial_rows)

    baseline = statistics.median(grouped["baseline_8_0"])
    print("\nMedian elapsed time by configuration (lower is faster):")
    print("configuration          median_s  vs_baseline")
    for label in ("baseline_8_0", "shuffle_4_0", "shuffle_12_0", "file_cap_8_100k"):
        median = statistics.median(grouped[label])
        change = (median / baseline - 1) * 100
        print(f"{label:<22} {median:>8.1f}  {change:>+9.1f}%")
    print(f"\nDetailed results: {result_path}")
    print("All trial outputs are isolated under data/bench/ and do not load PostgreSQL.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
