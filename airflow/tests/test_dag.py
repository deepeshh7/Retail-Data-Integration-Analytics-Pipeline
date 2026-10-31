import importlib.util
from pathlib import Path


def test_retail_dag_imports_and_has_ordered_pipeline_tasks():
    dag_file = Path(__file__).resolve().parents[1] / "dags" / "retail_pipeline_daily.py"
    spec = importlib.util.spec_from_file_location("retail_pipeline_daily_test", dag_file)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dag = module.retail_online_retail()

    assert set(dag.task_dict) == {
        "ingest_source",
        "transform_and_quarantine",
        "load_warehouse_and_publish_gold",
    }
    assert dag.task_dict["transform_and_quarantine"].upstream_task_ids == {"ingest_source"}
    assert dag.task_dict["load_warehouse_and_publish_gold"].upstream_task_ids == {"transform_and_quarantine"}
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert dag.task_dict["ingest_source"].retries == 2
