# Databricks notebook source
# The outer fixture owns the wheel, experiment, model, registry schema, and job.
# Training and scoring are separate tasks; both install the same immutable wheel.

import hashlib
import json
from pathlib import Path

for parameter in (
    "wheel_path", "manifest_path", "wheel_sha256", "source_revision", "phase",
    "model_name", "registry_table", "experiment_id",
):
    dbutils.widgets.text(parameter, "")

wheel_path = dbutils.widgets.get("wheel_path")
assert wheel_path.startswith("/Volumes/") and ".." not in Path(wheel_path).parts
assert hashlib.sha256(Path(wheel_path).read_bytes()).hexdigest() == dbutils.widgets.get("wheel_sha256")

%pip install --force-reinstall --no-deps '{wheel_path}'
%pip install '{wheel_path}[anomaly]'

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import hashlib
import json
import math
import os
import platform
from importlib.metadata import PackageNotFoundError, distribution, version
from pathlib import Path

import mlflow
import pandas as pd
from pyspark.sql.functions import pandas_udf

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule

manifest = json.loads(Path(dbutils.widgets.get("manifest_path")).read_text())
assert manifest["wheel_sha256"] == dbutils.widgets.get("wheel_sha256")
assert manifest["revision"] == dbutils.widgets.get("source_revision")
assert hashlib.sha256(Path(dbutils.widgets.get("wheel_path")).read_bytes()).hexdigest() == manifest["wheel_sha256"]
expected_files = manifest["files"]
packages = (
    "databricks-labs-dqx", "databricks-connect", "pyspark", "scikit-learn", "mlflow",
    "shap", "numba", "numpy", "pandas", "pyarrow", "cloudpickle", "coverage",
)


def installed_environment(source: str) -> dict[str, object]:
    """Check source bytes without returning machine paths or arbitrary metadata."""
    installed = distribution("databricks-labs-dqx")
    for filename, digest in expected_files.items():
        assert filename.startswith("databricks/labs/dqx/") and ".." not in Path(filename).parts
        assert hashlib.sha256(installed.locate_file(filename).read_bytes()).hexdigest() == digest, "Installed source mismatch"
    versions = {}
    for package in packages:
        try:
            versions[package] = version(package)
        except PackageNotFoundError:
            versions[package] = "unavailable"
    # Import the same dependency boundary used by contributions in each process.
    # A driver-only compatibility patch cannot make this worker check pass.
    import shap
    assert shap.__version__ == versions["shap"]
    return {"source": source, "python": platform.python_version(), "packages": versions, "sources_verified": True}


@pandas_udf("string")
def worker_environment(batch: pd.Series) -> pd.Series:
    return pd.Series([json.dumps(installed_environment("worker"))] * len(batch))


environments = [installed_environment("installed")]
environments.extend(
    json.loads(row["environment"])
    for row in spark.range(2).repartition(2).select(worker_environment("id").alias("environment")).collect()
)
phase = dbutils.widgets.get("phase")
assert phase in {"train", "score"}
ws = WorkspaceClient()
model_name = dbutils.widgets.get("model_name")
registry_table = dbutils.widgets.get("registry_table")
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_id=dbutils.widgets.get("experiment_id"))
os.environ["MLFLOW_EXPERIMENT_ID"] = dbutils.widgets.get("experiment_id")

# COMMAND ----------

if phase == "train":
    training = spark.createDataFrame(
        [(100.0 + index * 0.02, 2.0 + (index % 5) * 0.01) for index in range(100)],
        "amount double, quantity double",
    )
    AnomalyEngine(ws, spark).train(
        df=training,
        columns=["amount", "quantity"],
        model_name=model_name,
        registry_table=registry_table,
        params=AnomalyParams(algorithm_config=IsolationForestConfig(contamination=0.01, random_seed=42)),
    )

# COMMAND ----------

if phase == "score":
    # No fitted estimator is shared with the training task. Public check APIs load
    # the persisted model, and driver_only=False forces pandas UDF execution.
    payload = (0.42, 0.1, {"original": 0.2}, {"original basis": 0.3}, 12.0)
    probe = spark.createDataFrame(
        [
            (1, 101.0, 2.02, *payload),
            (2, 9999.0, 1.0, *payload),
            (2, 9999.0, 1.0, *payload),
        ],
        "transaction_id int, amount double, quantity double, anomaly_score double, anomaly_score_std double, "
        "anomaly_contributions map<string,double>, anomaly_basis_contributions map<string,double>, severity_percentile double",
    )
    arguments = {
        "model_name": model_name,
        "registry_table": registry_table,
        "threshold": 80.0,
        "enable_contributions": True,
        "enable_ai_explanation": False,
        "driver_only": False,
    }
    metadata_checks = [
        {"criticality": criticality, "check": {"function": "has_no_row_anomalies", "arguments": arguments}}
        for criticality in ("error", "warn")
    ]
    class_checks = [
        DQDatasetRule(criticality=criticality, check_func=has_no_row_anomalies, check_func_kwargs=arguments)
        for criticality in ("error", "warn")
    ]
    engine = DQEngine(ws, spark)
    metadata_result = engine.apply_checks_by_metadata(probe, metadata_checks)
    class_result = engine.apply_checks(probe, class_checks)
    assert metadata_result.schema == class_result.schema
    for original_field in probe.schema.fields:
        assert metadata_result.schema[original_field.name] == original_field
    # Collect the tiny synthetic probe through the DataFrame API: toJSON returns
    # an RDD and is unavailable when classic compute exposes Spark Connect.
    expected_rows = sorted(json.dumps(row.asDict(recursive=True), sort_keys=True) for row in probe.collect())
    results = []
    for result in (metadata_result, class_result):
        assert set(result.columns) == set(probe.columns) | {"_dq_info", "_errors", "_warnings"}
        restored_rows = sorted(
            json.dumps(row.asDict(recursive=True), sort_keys=True)
            for row in result.select(*probe.columns).collect()
        )
        assert restored_rows == expected_rows, "Input values or duplicates changed"
        rows = result.orderBy("transaction_id").collect()
        assert len(rows) == 3 and rows[1] == rows[2], "Duplicate rows must be preserved"
        for row in rows:
            assert len(row["_dq_info"]) == 2
            for info in row["_dq_info"]:
                anomaly = info["anomaly"]
                assert anomaly["check_name"] == "has_no_row_anomalies" and anomaly["model"] == model_name
                assert anomaly["is_anomaly"] is (row["transaction_id"] == 2)
                assert math.isfinite(anomaly["score"])
                assert math.isfinite(anomaly["severity_percentile"])
                if row["transaction_id"] == 2:
                    assert set(anomaly["contributions"]) == {"amount", "quantity"}
                    assert all(math.isfinite(value) for value in anomaly["contributions"].values())
                else:
                    assert anomaly["contributions"] is None
                    assert row["_errors"] is None and row["_warnings"] is None
            if row["transaction_id"] == 2:
                assert row["_errors"] and row["_warnings"]
                error = row["_errors"][0]
                assert error["name"] == "has_row_anomalies" and error["function"] == "has_no_row_anomalies"
                assert "reached threshold" in error["message"]
                assert row["_dq_info"][0]["anomaly"]["severity_percentile"] >= 80.0
        results.append(rows)
    for metadata_row, class_row in zip(*results, strict=True):
        for metadata_info, class_info in zip(metadata_row["_dq_info"], class_row["_dq_info"], strict=True):
            for field in ("score", "severity_percentile"):
                assert math.isclose(metadata_info["anomaly"][field], class_info["anomaly"][field], rel_tol=1e-10, abs_tol=1e-10)
            assert metadata_info["anomaly"]["contributions"] == class_info["anomaly"]["contributions"]

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "passed", "phase": phase, "wheel_sha256": manifest["wheel_sha256"],
    "revision": manifest["revision"], "spark": spark.version, "environments": environments,
}))
