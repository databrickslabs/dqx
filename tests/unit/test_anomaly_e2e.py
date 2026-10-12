"""Local contracts for the packaged anomaly test's artifacts and remote evidence."""

import hashlib
import json
from pathlib import Path
from zipfile import ZipFile

import pytest

from tests.anomaly_e2e import PackagedAnomalyCase, packaged_anomaly_tasks, safe_packaged_result, wheel_manifest


def test_wheel_manifest_rejects_stale_or_incomplete_source(tmp_path: Path) -> None:
    package = tmp_path / "src" / "databricks" / "labs" / "dqx"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("value = 1\n")
    wheel = tmp_path / "test.whl"
    with ZipFile(wheel, "w") as archive:
        archive.writestr("databricks/labs/dqx/__init__.py", "value = 1\n")
    manifest = wheel_manifest(wheel, tmp_path, "a" * 40)
    assert manifest["wheel_sha256"] == hashlib.sha256(wheel.read_bytes()).hexdigest()
    (package / "__init__.py").write_text("value = 2\n")
    with pytest.raises(ValueError, match="stale"):
        wheel_manifest(wheel, tmp_path, "a" * 40)
    (package / "new_module.py").write_text("value = 3\n")
    with pytest.raises(ValueError, match="complete"):
        wheel_manifest(wheel, tmp_path, "a" * 40)


def test_wheel_manifest_allows_only_build_version_difference(tmp_path: Path) -> None:
    package = tmp_path / "src" / "databricks" / "labs" / "dqx"
    package.mkdir(parents=True)
    (package / "__version__.py").write_text('__version__ = "1.0"\n')
    wheel = tmp_path / "test.whl"
    with ZipFile(wheel, "w") as archive:
        archive.writestr("databricks/labs/dqx/__version__.py", '__version__ = "1.0+ci"\n')
    assert wheel_manifest(wheel, tmp_path, "a" * 40)["files"]
    with pytest.raises(ValueError, match="revision"):
        wheel_manifest(wheel, tmp_path, "branch-name")


@pytest.mark.parametrize("serverless", [False, True])
def test_packaged_tasks_share_artifact_and_reload_in_new_task(serverless: bool) -> None:
    parameters = {"wheel_sha256": "a" * 64}
    tasks = packaged_anomaly_tasks("/test/notebook", parameters, serverless=serverless, cluster_id="cluster")
    assert [task.task_key for task in tasks] == ["train", "score"]
    assert tasks[1].depends_on and tasks[1].depends_on[0].task_key == "train"
    for task in tasks:
        assert task.notebook_task and task.notebook_task.base_parameters
        assert task.notebook_task.base_parameters["wheel_sha256"] == parameters["wheel_sha256"]
        assert task.notebook_task.base_parameters["phase"] == task.task_key
        assert task.existing_cluster_id == (None if serverless else "cluster")
        assert task.new_cluster is None and task.max_retries == 0
    assert "phase" not in parameters


def test_classic_packaged_tasks_require_explicit_configured_compute() -> None:
    with pytest.raises(ValueError, match="configured cluster"):
        packaged_anomaly_tasks("/test/notebook", {}, serverless=False, cluster_id=None)


def test_remote_provenance_is_verified_and_payload_free() -> None:
    case = PackagedAnomalyCase(1, "a" * 64, "b" * 40, True)
    result = {
        "status": "passed",
        "phase": "score",
        "wheel_sha256": case.wheel_sha256,
        "revision": case.revision,
        "spark": "4.2.0",
        "untrusted": "SECRET",
        "environments": [
            {
                "source": source,
                "sources_verified": True,
                "python": "3.12.3",
                "packages": {"shap": "0.49.1", "untrusted": "SECRET"},
                "hostname": "SECRET",
            }
            for source in ("installed", "worker")
        ],
    }
    safe = safe_packaged_result(json.dumps(result), case, "score")
    assert safe["requested_compute"] == "serverless" and safe["spark"] == "4.2.0"
    assert "SECRET" not in json.dumps(safe)
    result["wheel_sha256"] = "c" * 64
    with pytest.raises(ValueError, match="artifact"):
        safe_packaged_result(json.dumps(result), case, "score")


@pytest.mark.parametrize("payload", ["not json", "[]", "null", "{}"])
def test_invalid_remote_provenance_fails_safely(payload: str) -> None:
    with pytest.raises(ValueError, match="Packaged anomaly"):
        safe_packaged_result(payload, PackagedAnomalyCase(1, "a" * 64, "b" * 40, False), "score")


@pytest.mark.parametrize("source,verified", [("installed", True), ("worker", False), ("arbitrary", True)])
def test_both_verified_driver_and_worker_evidence_are_required(source: str, verified: bool) -> None:
    case = PackagedAnomalyCase(1, "a" * 64, "b" * 40, False)
    environment = {"source": source, "sources_verified": verified, "python": "3.12.3", "packages": {}}
    result = {
        "status": "passed",
        "phase": "score",
        "wheel_sha256": case.wheel_sha256,
        "revision": case.revision,
        "environments": [environment, environment],
    }
    with pytest.raises(ValueError, match="verify"):
        safe_packaged_result(json.dumps(result), case, "score")
