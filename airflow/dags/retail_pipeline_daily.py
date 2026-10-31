"""Airflow orchestration for the fingerprinted Online Retail batch pipeline."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from retail_pipeline.airflow_tasks import ingest_batch, load_batch, transform_batch


@dag(
    dag_id="retail_online_retail",
    description="Ingest, validate/transform, and load the Online Retail batch pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "retail-data-pipeline",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    },
    tags=["retail", "pyspark", "batch-elt"],
)
def retail_online_retail():
    @task(task_id="ingest_source")
    def ingest_source_task() -> dict:
        context = get_current_context()
        dag_run = context["dag_run"]
        run_conf = dag_run.conf or {}
        return ingest_batch(
            source_path=run_conf.get("input_path"),
            airflow_run={"dag_id": context["dag"].dag_id, "run_id": context["run_id"]},
            force=bool(run_conf.get("force", False)),
        )

    @task(task_id="transform_and_quarantine")
    def transform_task(metadata: dict) -> dict:
        if metadata.get("skip"):
            raise AirflowSkipException(metadata.get("skip_reason", "Batch already completed"))
        return transform_batch(metadata)

    @task(task_id="load_warehouse_and_publish_gold")
    def load_task(metadata: dict) -> dict:
        if metadata.get("skip"):
            raise AirflowSkipException(metadata.get("skip_reason", "Batch already completed"))
        return load_batch(metadata)

    load_task(transform_task(ingest_source_task()))


retail_online_retail()
