from retail_pipeline.airflow_tasks import airflow_run_id


def test_airflow_run_id_is_retry_stable_and_path_safe():
    first_attempt = airflow_run_id("retail_online_retail", "scheduled__2026-10-29T00:00:00+00:00")
    retry_attempt = airflow_run_id("retail_online_retail", "scheduled__2026-10-29T00:00:00+00:00")
    different_run = airflow_run_id("retail_online_retail", "scheduled__2026-10-30T00:00:00+00:00")

    assert first_attempt == retry_attempt
    assert first_attempt != different_run
    assert len(first_attempt) == 32
    assert first_attempt.isalnum()
