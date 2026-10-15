# Databricks notebook source
# The outer fixture owns the wheel, experiment, model, registry schema, and job.
# Training and scoring use separate tasks, each installing the same immutable wheel.

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
import importlib.abc
import json
import math
from collections import Counter
from datetime import datetime, timedelta
import os
import platform
import sys
from importlib.metadata import PackageNotFoundError, distribution, version
from pathlib import Path

# Reproduce an absent optional client dependency in a fresh task, before any DQX
# import. Workers retain the packaged dependencies; their normal scoring matrix
# remains separate from this client import-boundary regression.
phase = dbutils.widgets.get("phase")
assert phase in {"train", "score", "score_without_shap"}
if phase == "score_without_shap":
    assert "shap" not in sys.modules, "Dependency regression requires a fresh client"

    class UnavailableShap(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "shap" or fullname.startswith("shap."):
                raise ModuleNotFoundError("shap intentionally unavailable in this test client")
            return None

    sys.meta_path.insert(0, UnavailableShap())

import mlflow
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql import functions as F

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidParameterError
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
    # Only the additional client's import is unavailable. Do not claim that its
    # workers are missing SHAP: they still verify the installed package below.
    if source == "installed" and phase == "score_without_shap":
        try:
            import shap
        except ModuleNotFoundError:
            pass
        else:
            raise AssertionError("The dependency regression did not block the client import")
    else:
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
spark.conf.set("spark.sql.session.timeZone", "UTC")


def case_inputs(frame, name):
    """Use adversarial but valid source schemas through the public API."""
    if name == "distribution_single":
        return (
            frame.withColumnRenamed("amount", "__dqx_category_frequency")
            .withColumn("category", F.concat(F.lit("category_"), (F.dayofyear("event_time") % 25).cast("string"))),
            ["__dqx_category_frequency", "quantity", "category"],
        )
    if name == "distribution_ensemble":
        return (
            frame.withColumnRenamed("amount", "__dqx_baseline_key")
            .withColumn("event_time", F.col("event_time").cast("timestamp_ntz")),
            ["__dqx_baseline_key", "quantity"],
        )
    return frame, ["amount", "quantity"]

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
        case_training, columns = case_inputs(training, name)
        AnomalyEngine(ws, spark).train(
            df=case_training, columns=columns,
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


def assert_contract(result, rows, source, originals, *, combined, contributions, columns):
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
                assert shares is not None and set(shares) == set(columns)
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
            for field in ("is_anomaly", "is_new_baseline", "is_stale_baseline", "stale_baseline_horizon"):
                assert a[field] == b[field]
            if compare_contributions:
                for field in ("contributions", "basis_contributions"):
                    if a[field] is None:
                        assert b[field] is None
                    else:
                        assert b[field] is not None and set(a[field]) == set(b[field])
                        for key, value in a[field].items():
                            assert b[field][key] is None if value is None else math.isclose(value, b[field][key], abs_tol=0.1)


if phase in {"score", "score_without_shap"}:
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
    # Unselected payloads resembling lookup aliases must also survive unchanged.
    probe = (probe.withColumn("__DQX_GROUP_Q09500", F.lit(123.5))
             .withColumn("__dqx_severity_group_key", F.lit("original key"))
             .withColumn("__DQX_KNOWN_GROUP_KEY", F.lit("original membership")))
    engine = DQEngine(ws, spark)
    for name, profile, ensemble_size in configurations:
        source, columns = case_inputs(probe, name)
        # Materialize the synthetic input values in UTC before changing sessions.
        # Re-evaluating a timestamp-to-NTZ cast in another zone would change the input.
        source = spark.createDataFrame(source.collect(), source.schema)
        originals = original_multiset(source.collect(), source.columns)
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
                if phase == "score_without_shap" and profile == "distribution" and contributions:
                    try:
                        engine.apply_checks_by_metadata(source, checks)
                    except InvalidParameterError as error:
                        assert "requires the 'shap' dependency" in str(error) and "distribution" in str(error)
                    else:
                        raise AssertionError("Distribution contributions must reject unavailable SHAP")
                    completed_cases.append(f"{name}:{'driver' if driver_only else 'distributed'}:rejected")
                    continue
                result = engine.apply_checks_by_metadata(source, checks)
                rows = result.collect()
                assert_contract(result, rows, source, originals, combined=name == "distribution_ensemble", contributions=contributions, columns=columns)
                snapshots[(driver_only, contributions)] = rows
                completed_cases.append(f"{name}:{'driver' if driver_only else 'distributed'}:{'on' if contributions else 'off'}")
        for (driver_only, contributions), rows in snapshots.items():
            assert_parity(snapshots[(True, False)], rows, compare_contributions=False)
            if contributions:
                assert_parity(snapshots[(True, True)], rows, compare_contributions=True)
        if phase == "score_without_shap":
            continue
        # Programmatic and metadata APIs must also agree after column/partition changes.
        reordered = source.select(*reversed(source.columns)).repartition(2, "transaction_id")
        class_checks = [
            DQDatasetRule(criticality=criticality, check_func=has_no_row_anomalies, check_func_kwargs=arguments)
            for criticality in ("error", "warn")
        ]
        class_result = engine.apply_checks(reordered, class_checks)
        class_rows = class_result.collect()
        assert_contract(class_result, class_rows, reordered, originals, combined=name == "distribution_ensemble", contributions=True, columns=columns)
        assert_parity(snapshots[(False, True)], class_rows, compare_contributions=True)
        repeated = engine.apply_checks_by_metadata(result.select(*source.columns), checks)
        assert_parity(snapshots[(False, True)], repeated.collect(), compare_contributions=True)
        filtered_checks = [{**check, "filter": "transaction_id != 3"} for check in checks]
        filtered = engine.apply_checks_by_metadata(source, filtered_checks)
        filtered_rows = filtered.collect()
        assert original_multiset(filtered_rows, source.columns) == originals
        included = [row for row in filtered_rows if row.transaction_id != 3]
        assert_parity([row for row in snapshots[(False, True)] if row.transaction_id != 3], included, compare_contributions=True)
        for row in filtered_rows:
            if row.transaction_id == 3:
                assert row["_errors"] is None and row["_warnings"] is None
        if name == "distribution_ensemble":
            # The same registered NTZ model must survive a different scoring zone,
            # including its distributed attribution and stale-time verdict.
            try:
                spark.conf.set("spark.sql.session.timeZone", "America/New_York")
                shifted = engine.apply_checks_by_metadata(source, checks)
                shifted_rows = shifted.collect()
                assert_contract(shifted, shifted_rows, source, originals, combined=True, contributions=True, columns=columns)
                assert_parity(snapshots[(False, True)], shifted_rows, compare_contributions=True)
            finally:
                spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "passed", "phase": phase, "wheel_sha256": manifest["wheel_sha256"],
    "revision": manifest["revision"], "spark": spark.version, "environments": environments,
    "completed_cases": sorted(completed_cases),
}))
