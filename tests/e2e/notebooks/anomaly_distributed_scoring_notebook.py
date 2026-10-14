# Databricks notebook source
# The outer fixture owns the wheel, experiment, model, registry schema, and job.
# Training and scoring are separate tasks; both install the same immutable wheel.

import hashlib
import json
from pathlib import Path

for parameter in (
    "wheel_path", "manifest_path", "wheel_sha256", "source_revision", "phase",
    "model_name", "model_configs", "registry_table", "experiment_id",
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
from collections import Counter
from datetime import datetime, timedelta
import os
import platform
from importlib.metadata import PackageNotFoundError, distribution, version
from pathlib import Path

import mlflow
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql import functions as F

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

configurations = json.loads(dbutils.widgets.get("model_configs"))
completed_cases = []
start_time = datetime(2024, 1, 1)

if phase == "train":
    training = spark.createDataFrame(
        [
            (
                100.0 + day * 0.1 + group * 150.0 + 5.0 * math.sin(day * 2 * math.pi / 7),
                2.0 + group + day * 0.002 + (day % 5) * 0.03,
                f"group_{group}", start_time + timedelta(days=day),
            )
            for day in range(320) for group in range(2)
        ],
        "amount double, quantity double, region string, event_time timestamp",
    )
    for name, profile, ensemble_size in configurations:
        combined = name == "distribution_ensemble"
        AnomalyEngine(ws, spark).train(
            df=training, columns=["amount", "quantity"],
            model_name=f"{model_name}_{name}", registry_table=registry_table,
            profile=profile, baseline_by=["region"] if combined else [],
            baseline_over_time="event_time" if combined else None,
            params=AnomalyParams(
                sample_fraction=1.0, ensemble_size=ensemble_size,
                algorithm_config=IsolationForestConfig(contamination=0.01, random_seed=42),
            ),
        )
        completed_cases.append(name)

# COMMAND ----------


def original_multiset(rows, columns):
    return Counter(
        json.dumps({key: row[key] for key in columns}, sort_keys=True, default=str)
        for row in rows
    )


def assert_contract(result, rows, source, originals, *, combined, contributions):
    assert set(result.columns) == set(source.columns) | {"_dq_info", "_errors", "_warnings"}
    assert [name for name in result.columns if name in source.columns] == source.columns
    assert [result.schema[name] for name in source.columns] == list(source.schema)
    assert original_multiset(rows, source.columns) == originals, "Input values or duplicates changed"
    for row in rows:
        assert len(row["_dq_info"]) == 2, "Both consecutive checks must preserve their result"
        for info in row["_dq_info"]:
            anomaly = info["anomaly"]
            unseen = combined and row["transaction_id"] == 4
            assert anomaly["is_new_baseline"] is unseen
            assert anomaly["ai_explanation"] is None
            if unseen:
                assert anomaly["score"] is None and anomaly["severity_percentile"] is None
                assert anomaly["contributions"] is None and anomaly["basis_contributions"] is None
                assert anomaly["is_anomaly"] is False
                continue
            assert math.isfinite(anomaly["score"]) and math.isfinite(anomaly["severity_percentile"])
            assert anomaly["is_anomaly"] is (anomaly["severity_percentile"] >= anomaly["threshold"])
            assert anomaly["is_anomaly"] is True, "Threshold zero must flag every supported score"
            expected_stale = row["transaction_id"] == 5 if combined else None
            assert anomaly["is_stale_baseline"] is expected_stale
            if combined:
                assert math.isfinite(anomaly["confidence_std"]) and anomaly["confidence_std"] >= 0
            if contributions:
                shares = anomaly["contributions"]
                assert shares is not None and set(shares) == {"amount", "quantity"}
                values = [value for value in shares.values() if value is not None]
                assert all(math.isfinite(value) and value >= 0 for value in values)
                if values:
                    assert math.isclose(sum(values), 100.0, abs_tol=0.2)
                if row["transaction_id"] == 2:
                    assert values and max(values) > 0, "Controlled perturbation needs positive evidence"
                    if combined:
                        basis = anomaly["basis_contributions"]
                        assert basis, "Combined-model perturbation needs basis evidence"
                        assert all(math.isfinite(share) and 0.0 <= share <= 100.0 for share in basis.values())
                        assert any("group baseline" in label or "expected level at that time" in label for label in basis)
            else:
                assert anomaly["contributions"] is None and anomaly["basis_contributions"] is None
        assert bool(row["_errors"]) is not (combined and row["transaction_id"] == 4)
        assert bool(row["_warnings"]) is not (combined and row["transaction_id"] == 4)


def assert_parity(reference, observed, *, compare_contributions):
    left_rows = sorted(reference, key=lambda row: row.transaction_id)
    right_rows = sorted(observed, key=lambda row: row.transaction_id)
    assert len(left_rows) == len(right_rows)
    for left, right in zip(left_rows, right_rows, strict=True):
        assert left.transaction_id == right.transaction_id
        for first, second in zip(left["_dq_info"], right["_dq_info"], strict=True):
            a, b = first["anomaly"], second["anomaly"]
            for field in ("score", "severity_percentile", "confidence_std"):
                if a[field] is None:
                    assert b[field] is None
                else:
                    assert b[field] is not None and math.isclose(a[field], b[field], rel_tol=1e-9, abs_tol=1e-9)
            for field in ("is_anomaly", "is_new_baseline", "is_stale_baseline"):
                assert a[field] == b[field]
            if compare_contributions:
                for field in ("contributions", "basis_contributions"):
                    if a[field] is None:
                        assert b[field] is None
                    else:
                        assert b[field] is not None and set(a[field]) == set(b[field])
                        for key, value in a[field].items():
                            assert b[field][key] is None if value is None else math.isclose(value, b[field][key], abs_tol=0.1)


if phase == "score":
    # Every model is reloaded from the registry in this fresh task. Original
    # collision fields and duplicate rows exercise the public restoration boundary.
    payload = (0.42, 0.1, {"original": 0.2}, {"original basis": 0.3}, 12.0)
    probe = spark.createDataFrame(
        [
            (1, 110.0, 2.2, "group_0", start_time + timedelta(days=100), *payload),
            (2, 9999.0, 1.0, "group_0", start_time + timedelta(days=100), *payload),
            (2, 9999.0, 1.0, "group_0", start_time + timedelta(days=100), *payload),
            (3, None, 2.3, "group_1", start_time + timedelta(days=100), *payload),
            (4, 130.0, 2.2, "unseen", start_time + timedelta(days=100), *payload),
            (5, 135.0, 2.7, "group_0", start_time + timedelta(days=700), *payload),
        ],
        "transaction_id int, amount double, quantity double, region string, event_time timestamp, "
        "anomaly_score double, anomaly_score_std double, anomaly_contributions map<string,double>, "
        "anomaly_basis_contributions map<string,double>, severity_percentile double",
    )
    originals = original_multiset(probe.collect(), probe.columns)
    engine = DQEngine(ws, spark)
    for name, profile, ensemble_size in configurations:
        snapshots = {}
        for driver_only in (True, False):
            for contributions in (False, True):
                arguments = {
                    "model_name": f"{model_name}_{name}", "registry_table": registry_table,
                    "threshold": 0.0, "enable_contributions": contributions,
                    "enable_confidence_std": True, "enable_ai_explanation": False,
                    "driver_only": driver_only,
                }
                checks = [
                    {"criticality": criticality, "check": {"function": "has_no_row_anomalies", "arguments": arguments}}
                    for criticality in ("error", "warn")
                ]
                result = engine.apply_checks_by_metadata(probe, checks)
                rows = result.collect()
                assert_contract(result, rows, probe, originals, combined=name == "distribution_ensemble", contributions=contributions)
                snapshots[(driver_only, contributions)] = rows
                completed_cases.append(f"{name}:{'driver' if driver_only else 'distributed'}:{'on' if contributions else 'off'}")
        for (driver_only, contributions), rows in snapshots.items():
            assert_parity(snapshots[(True, False)], rows, compare_contributions=False)
            if contributions:
                assert_parity(snapshots[(True, True)], rows, compare_contributions=True)
        # Programmatic and metadata APIs must also agree after column/partition changes.
        reordered = probe.select(*reversed(probe.columns)).repartition(2, "transaction_id")
        class_checks = [
            DQDatasetRule(criticality=criticality, check_func=has_no_row_anomalies, check_func_kwargs=arguments)
            for criticality in ("error", "warn")
        ]
        class_result = engine.apply_checks(reordered, class_checks)
        class_rows = class_result.collect()
        assert_contract(class_result, class_rows, reordered, originals, combined=name == "distribution_ensemble", contributions=True)
        assert_parity(snapshots[(False, True)], class_rows, compare_contributions=True)
        repeated = engine.apply_checks_by_metadata(result.select(*probe.columns), checks)
        assert_parity(snapshots[(False, True)], repeated.collect(), compare_contributions=True)
        filtered_checks = [{**check, "filter": "transaction_id != 3"} for check in checks]
        filtered = engine.apply_checks_by_metadata(probe, filtered_checks)
        filtered_rows = filtered.collect()
        assert original_multiset(filtered_rows, probe.columns) == originals
        included = [row for row in filtered_rows if row.transaction_id != 3]
        assert_parity([row for row in snapshots[(False, True)] if row.transaction_id != 3], included, compare_contributions=True)
        for row in filtered_rows:
            if row.transaction_id == 3:
                assert row["_errors"] is None and row["_warnings"] is None

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "passed", "phase": phase, "wheel_sha256": manifest["wheel_sha256"],
    "revision": manifest["revision"], "spark": spark.version, "environments": environments,
    "completed_cases": sorted(completed_cases),
}))
