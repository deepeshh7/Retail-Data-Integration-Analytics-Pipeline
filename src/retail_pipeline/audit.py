import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_completed(manifest: Path, digest: str) -> bool:
    if not manifest.exists():
        return False
    return digest in json.loads(manifest.read_text(encoding="utf-8")).get("completed_fingerprints", [])


def _atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, indent=2, default=str)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def mark_completed(manifest: Path, digest: str) -> None:
    state = json.loads(manifest.read_text(encoding="utf-8")) if manifest.exists() else {"completed_fingerprints": []}
    if digest not in state["completed_fingerprints"]:
        state["completed_fingerprints"].append(digest)
    _atomic_json(manifest, state)


def write_run_record(directory: Path, values: dict) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%S_%fZ}.json"
    record = {**values, "recorded_at_utc": datetime.now(timezone.utc).isoformat()}
    _atomic_json(path, record)
    return path
