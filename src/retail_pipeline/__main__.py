import argparse
import json
import logging
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings
from .runner import PipelineFailure, run


class JsonLineFormatter(logging.Formatter):
    """Structured file logs; console logs remain compact and human-readable."""
    EXTRA_FIELDS = (
        "event", "run_id", "batch_id", "stage", "source_file", "row_count",
        "accepted", "quarantined", "deduplicated", "rejection_reasons", "metrics",
        "status", "duration_seconds", "audit_path", "error_type", "error_message",
    )

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in self.EXTRA_FIELDS:
            if hasattr(record, field):
                payload[field] = getattr(record, field)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()
    file_handler = logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8")
    file_handler.setFormatter(JsonLineFormatter())
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(file_handler)
    root.addHandler(console_handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the retail batch ELT pipeline")
    parser.add_argument("--input", type=Path)
    parser.add_argument("--force", action="store_true", help="Retry a completed source fingerprint; committed warehouse batches remain idempotent")
    parser.add_argument("--backfill-batch", help="Reprocess a retained source batch by its 64-character SHA-256 fingerprint; rebuilds Gold and the warehouse state")
    parser.add_argument("--dry-run", action="store_true", help="Skip warehouse write and completion manifest")
    args = parser.parse_args()
    settings = Settings.from_env()
    configure_logging(settings.log_dir)
    settings = replace(settings, input_path=args.input or settings.input_path)
    try:
        result = run(settings, force=args.force, dry_run=args.dry_run,
                     backfill_batch_id=args.backfill_batch)
    except PipelineFailure as exc:
        parser.exit(1, f"ERROR: pipeline failed at stage '{exc.stage}' (run {exc.run_id}): {exc.cause}. See logs/pipeline.log and data/audit for details.\n")
    logging.info("Pipeline status: %s", result["status"], extra={"event": "pipeline_status", "run_id": result.get("run_id"), "batch_id": result.get("batch_id"), "status": result["status"]})


if __name__ == "__main__":
    main()
