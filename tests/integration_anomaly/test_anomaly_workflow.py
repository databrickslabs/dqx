from types import SimpleNamespace

import pyspark.sql.functions as F
import pytest

from databricks.labs.dqx.anomaly.anomaly_workflow import AnomalyTrainerWorkflow
from databricks.labs.dqx.config import AnomalyConfig, InputConfig, RunConfig
from databricks.labs.dqx.errors import InvalidConfigError
from tests.constants import TEST_CATALOG
from tests.integration_anomaly.test_anomaly_utils import get_standard_2d_training_data


def test_anomaly_workflow_trains_with_explicit_model_name(ws, spark, make_schema, make_random):
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(8).lower()

    table_name = f"{TEST_CATALOG}.{schema.name}.workflow_train_{suffix}"
    registry_table = f"{TEST_CATALOG}.{schema.name}.workflow_registry_{suffix}"
    model_name = f"{TEST_CATALOG}.{schema.name}.dqx_anomaly_{suffix}"

    train_df = spark.createDataFrame(get_standard_2d_training_data(), "amount double, quantity double")
    train_df.write.saveAsTable(table_name)

    run_config = RunConfig(
        name="Orders Daily",
        input_config=InputConfig(location=table_name),
        anomaly_config=AnomalyConfig(
            columns=["amount", "quantity"],
            registry_table=registry_table,
            model_name=model_name,
        ),
    )
    ctx = SimpleNamespace(run_config=run_config, spark=spark, workspace_client=ws)

    AnomalyTrainerWorkflow().train_model(ctx)

    models_df = spark.table(registry_table).select(F.col("identity.model_name").alias("model_name"))

    assert models_df.filter(F.col("model_name") == model_name).count() > 0


def test_anomaly_workflow_missing_anomaly_config_raises(ws, spark):
    run_config = RunConfig(
        name="missing_anomaly_config",
        input_config=InputConfig(location="catalog.schema.table"),
        anomaly_config=None,
    )
    ctx = SimpleNamespace(run_config=run_config, spark=spark, workspace_client=ws)

    with pytest.raises(InvalidConfigError, match="anomaly_config is required"):
        AnomalyTrainerWorkflow().train_model(ctx)


def test_anomaly_workflow_missing_input_config_raises(ws, spark):
    run_config = RunConfig(
        name="missing_input_config",
        input_config=None,
        anomaly_config=AnomalyConfig(columns=["amount"], registry_table="catalog.schema.registry"),
    )
    ctx = SimpleNamespace(run_config=run_config, spark=spark, workspace_client=ws)

    with pytest.raises(InvalidConfigError, match="input_config is required"):
        AnomalyTrainerWorkflow().train_model(ctx)


def test_anomaly_workflow_missing_model_name_raises(ws, spark):
    run_config = RunConfig(
        name="missing_model_name",
        input_config=InputConfig(location="catalog.schema.table"),
        anomaly_config=AnomalyConfig(columns=["amount"], registry_table="catalog.schema.registry"),
    )
    ctx = SimpleNamespace(run_config=run_config, spark=spark, workspace_client=ws)

    with pytest.raises(InvalidConfigError, match="model_name is required"):
        AnomalyTrainerWorkflow().train_model(ctx)
