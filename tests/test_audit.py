import json

from retail_pipeline.audit import fingerprint, is_completed, mark_completed


def test_fingerprint_and_manifest_are_repeatable(tmp_path):
    source = tmp_path / "input.csv"
    source.write_text("a,b\n1,2\n", encoding="utf-8")
    digest = fingerprint(source)
    assert digest == fingerprint(source)
    manifest = tmp_path / "manifest.json"
    assert not is_completed(manifest, digest)
    mark_completed(manifest, digest)
    mark_completed(manifest, digest)
    assert is_completed(manifest, digest)
    assert json.loads(manifest.read_text())["completed_fingerprints"] == [digest]


def test_failure_run_record_has_id_status_and_error(tmp_path):
    from retail_pipeline.audit import write_run_record

    path = write_run_record(tmp_path / "audit", {
        "run_id": "run-123", "status": "failed", "stage": "quality",
        "error": {"type": "ValueError", "message": "invalid fixture"},
    })
    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["run_id"] == "run-123"
    assert record["status"] == "failed"
    assert record["stage"] == "quality"
    assert record["error"]["message"] == "invalid fixture"
    assert record["recorded_at_utc"]
    assert not list(path.parent.glob("*.tmp"))
