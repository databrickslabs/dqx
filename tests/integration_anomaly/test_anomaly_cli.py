import logging

import pyspark.sql.functions as F
import pytest

from databricks.labs.dqx.cli import logs, train_anomaly
from databricks.labs.dqx.errors import InvalidConfigError


def test_train_anomaly_cli(spark, setup_anomaly_deployed_workflow, caplog):
    """Run the anomaly-trainer workflow via the CLI command and verify the model is in the registry."""
    installation_ctx, run_config, registry_table, model_name = setup_anomaly_deployed_workflow()

    train_anomaly(
        installation_ctx.workspace_client, run_config=run_config.name, ctx=installation_ctx.workspace_installer
    )

    models_df = spark.table(registry_table).select(F.col("identity.model_name").alias("model_name"))
    assert models_df.filter(F.col("model_name") == model_name).count() > 0

    with caplog.at_level(logging.INFO):
        logs(installation_ctx.workspace_client, ctx=installation_ctx.workspace_installer)

    assert "Completed anomaly-trainer workflow run" in caplog.text


def test_train_anomaly_cli_when_run_config_missing(ws, installation_ctx):
    """Verify that the CLI raises an error when the requested run config does not exist."""
    installation_ctx.installation_service.run()

    with pytest.raises(InvalidConfigError, match="No run configurations available"):
        train_anomaly(ws, run_config="unavailable", ctx=installation_ctx.workspace_installer)
