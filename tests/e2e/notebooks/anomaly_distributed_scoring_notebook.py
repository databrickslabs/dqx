# Databricks notebook source
# Runs anomaly scoring in distributed (non-driver-only) mode so the pandas UDF path is exercised on cluster workers.

dbutils.widgets.text("test_library_ref", "", "Test Library Ref")
%pip install 'databricks-labs-dqx[anomaly] @ {dbutils.widgets.get("test_library_ref")}'

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import uuid
import yaml
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule
import mlflow
from mlflow.tracking import MlflowClient

# COMMAND ----------
# DBTITLE 1,Setup: catalog, schema (from params), train model

dbutils.widgets.text("test_catalog", "", "Test Catalog")
dbutils.widgets.text("test_schema", "", "Test Schema")

test_catalog = dbutils.widgets.get("test_catalog")
test_schema = dbutils.widgets.get("test_schema")

DEFAULT_SCORE_THRESHOLD = 80.0

# Use a dedicated experiment for this e2e run so we can delete it in cleanup
ws = WorkspaceClient()
user_name = ws.current_user.me().user_name
e2e_experiment_path = f"/Users/{user_name}/e2e_anomaly_{test_schema}_{uuid.uuid4().hex[:8]}"
if mlflow.get_experiment_by_name(e2e_experiment_path) is None:
    mlflow.create_experiment(e2e_experiment_path)
mlflow.set_experiment(e2e_experiment_path)
os.environ["MLFLOW_EXPERIMENT_NAME"] = e2e_experiment_path

registry_table = f"{test_catalog}.{test_schema}.reg_distributed"
model_name = f"{test_catalog}.{test_schema}.test_distributed"

train_data = [(100.0 + i * 0.02, 2.0) for i in range(50)]
train_df = spark.createDataFrame(train_data, "amount double, quantity double")

engine = AnomalyEngine(ws, spark)
params = AnomalyParams(algorithm_config=IsolationForestConfig(contamination=0.01, random_seed=42))

engine.train(
    df=train_df,
    columns=["amount", "quantity"],
    model_name=model_name,
    registry_table=registry_table,
    params=params,
)

# COMMAND ----------
# DBTITLE 1,test_apply_checks_by_metadata_distributed

def test_apply_checks_by_metadata_distributed():
    checks_yaml = f"""
    - criticality: error
      check:
        function: has_no_row_anomalies
        arguments:
          model_name: {model_name}
          registry_table: {registry_table}
          threshold: {DEFAULT_SCORE_THRESHOLD}
    """

    checks = yaml.safe_load(checks_yaml)
    test_df = spark.createDataFrame(
        [(1, 100.5, 2.0), (2, 9999.0, 1.0)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    rows = result_df.orderBy("transaction_id").collect()
    inlier_row = next(r for r in rows if r["transaction_id"] == 1)
    anomalous_row = next(r for r in rows if r["transaction_id"] == 2)

    assert "_dq_info" in result_df.columns
    assert "_errors" in result_df.columns
    assert "_warnings" in result_df.columns

    assert inlier_row["_dq_info"] is not None
    assert anomalous_row["_dq_info"] is not None

    assert inlier_row["_dq_info"][0]["anomaly"] is not None
    assert anomalous_row["_dq_info"][0]["anomaly"] is not None

    assert inlier_row["_dq_info"][0]["anomaly"]["is_anomaly"] is False, "Inlier (transaction_id=1) should not be flagged"
    
    anomaly_outlier = anomalous_row["_dq_info"][0]["anomaly"]
    assert anomaly_outlier["check_name"] == "has_no_row_anomalies"
    assert anomaly_outlier["model"] == model_name
    assert anomaly_outlier["is_anomaly"] is True, "Anomalous row (transaction_id=2) should be flagged"
    
    assert anomalous_row["_errors"] is not None, "Anomalous row (transaction_id=2) should have _errors"
    errs = anomalous_row["_errors"]
    assert len(errs) >= 1, "Outlier row should have at least one error entry"
    first_error = errs[0]
    assert first_error["name"] == "has_row_anomalies"
    assert first_error["function"] == "has_no_row_anomalies"
    assert "exceeded threshold" in first_error["message"]
    assert anomaly_outlier["severity_percentile"] >= DEFAULT_SCORE_THRESHOLD


# COMMAND ----------
# DBTITLE 1,test_apply_checks_distributed

def test_apply_checks_distributed():
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=has_no_row_anomalies,
            check_func_kwargs={
                "model_name": model_name,
                "registry_table": registry_table,
                "threshold": DEFAULT_SCORE_THRESHOLD,
            },
        ),
        DQDatasetRule(
            criticality="warn",
            check_func=has_no_row_anomalies,
            check_func_kwargs={
                "model_name": model_name,
                "registry_table": registry_table,
                "threshold": DEFAULT_SCORE_THRESHOLD,
            },
        )
    ]
    test_df = spark.createDataFrame(
        [(1, 100.5, 2.0), (2, 9999.0, 1.0)],
        "transaction_id int, amount double, quantity double",
    )

    dq_engine = DQEngine(ws, spark)
    result_df = dq_engine.apply_checks(test_df, checks)
    rows = result_df.orderBy("transaction_id").collect()
    inlier_row = next(r for r in rows if r["transaction_id"] == 1)
    anomalous_row = next(r for r in rows if r["transaction_id"] == 2)

    assert "_dq_info" in result_df.columns
    assert "_errors" in result_df.columns
    assert "_warnings" in result_df.columns
    
    assert len(rows) == 2
    assert len(inlier_row["_dq_info"]) == 2, "_dq_info should have 2 items (error check + warning check)"
    assert len(anomalous_row["_dq_info"]) == 2

    assert inlier_row["_dq_info"][0]["anomaly"] is not None
    assert inlier_row["_dq_info"][1]["anomaly"] is not None
    assert inlier_row["_dq_info"][0]["anomaly"]["is_anomaly"] is False, "Inlier (transaction_id=1) should not be flagged"
    assert inlier_row["_dq_info"][1]["anomaly"]["is_anomaly"] is False

    assert anomalous_row["_dq_info"][0]["anomaly"] is not None
    assert anomalous_row["_dq_info"][1]["anomaly"] is not None
    for i in (0, 1):
        anomaly = anomalous_row["_dq_info"][i]["anomaly"]
        assert anomaly["check_name"] == "has_no_row_anomalies"
        assert anomaly["model"] == model_name
        assert anomaly["is_anomaly"] is True, f"Anomalous row (transaction_id=2) _dq_info[{i}] should be flagged"
    assert anomalous_row["_errors"] is not None, "Anomalous row should have _errors (error criticality)"
    assert anomalous_row["_warnings"] is not None, "Anomalous row should have _warnings (warning criticality)"


# COMMAND ----------
# DBTITLE 1,Run tests and cleanup (cleanup always runs)

def _e2e_cleanup_mlflow(model_name: str, registry_table: str, experiment_path: str) -> None:
    """Delete the MLflow run, UC registered model, and e2e experiment created by this notebook."""
    registry = AnomalyModelRegistry(spark)
    record = registry.get_active_model(registry_table, model_name)
    if record:
        run_id = record.identity.mlflow_run_id
        if run_id:
            try:
                mlflow.delete_run(run_id)
            except Exception:
                pass
        try:
            MlflowClient().delete_registered_model(model_name)
        except Exception:
            pass
    if experiment_path:
        try:
            exp = mlflow.get_experiment_by_name(experiment_path)
            if exp is not None and getattr(exp, "experiment_id", None):
                mlflow.delete_experiment(exp.experiment_id)
        except Exception:
            pass


try:
    test_apply_checks_by_metadata_distributed()
    test_apply_checks_distributed()
finally:
    _e2e_cleanup_mlflow(model_name, registry_table, e2e_experiment_path)
