"""
Workflow to train anomaly detection models on Databricks.

Requires the 'anomaly' extras: pip install databricks-labs-dqx[anomaly]
"""

from collections.abc import Callable
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.errors import InvalidConfigError
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task
from databricks.labs.dqx.io import read_input_data


if TYPE_CHECKING:
    from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine


class AnomalyTrainerWorkflow(Workflow):
    """Workflow wrapper for periodic anomaly model training."""

    def __init__(
        self,
        spark_conf: dict[str, str] | None = None,
        override_clusters: dict[str, str] | None = None,
        *,
        input_reader: Callable[[SparkSession, InputConfig], DataFrame] = read_input_data,
        engine_factory: Callable[[WorkspaceClient, SparkSession], "AnomalyEngine"] | None = None,
    ) -> None:
        """Initialize the workflow with optional data-reading and training boundaries."""
        super().__init__("anomaly-trainer", spark_conf=spark_conf, override_clusters=override_clusters)
        self._input_reader = input_reader
        self._engine_factory = engine_factory

    @workflow_task
    def train_model(self, ctx: WorkflowContext) -> None:
        """Train anomaly detection model for the configured run."""

        run_config = ctx.run_config
        anomaly_config = run_config.anomaly_config
        if not anomaly_config:
            raise InvalidConfigError("anomaly_config is required to run the anomaly trainer workflow.")

        if not run_config.input_config:
            raise InvalidConfigError("input_config is required to run the anomaly trainer workflow.")

        model_name = anomaly_config.model_name
        if not model_name:
            raise InvalidConfigError("model_name is required and must be fully qualified (catalog.schema.name).")

        registry_table = anomaly_config.registry_table
        if not registry_table:
            raise InvalidConfigError("registry_table is required and must be fully qualified (catalog.schema.name).")

        df = self._input_reader(ctx.spark, run_config.input_config)

        if self._engine_factory is None:
            # Keep optional anomaly dependencies out of workflow discovery during installation.
            from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine

            anomaly_engine = AnomalyEngine(ctx.workspace_client, ctx.spark)
        else:
            anomaly_engine = self._engine_factory(ctx.workspace_client, ctx.spark)
        anomaly_engine.train(
            df=df,
            columns=anomaly_config.columns,
            baseline_by=anomaly_config.baseline_by,
            profile=anomaly_config.profile,
            baseline_over_time=anomaly_config.baseline_over_time,
            model_name=model_name,
            registry_table=registry_table,
        )
