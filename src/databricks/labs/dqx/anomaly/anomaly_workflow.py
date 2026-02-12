"""
Workflow to train anomaly detection models on Databricks.

Requires the 'anomaly' extras: pip install databricks-labs-dqx[anomaly]
"""

from databricks.labs.dqx.anomaly import AnomalyEngine
from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.errors import InvalidConfigError
from databricks.labs.dqx.installer.workflow_task import Workflow, workflow_task
from databricks.labs.dqx.io import read_input_data


class AnomalyTrainerWorkflow(Workflow):
    """Workflow wrapper for periodic anomaly model training."""

    def __init__(self, spark_conf: dict[str, str] | None = None, override_clusters: dict[str, str] | None = None):
        super().__init__("anomaly-trainer", spark_conf=spark_conf, override_clusters=override_clusters)

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

        df = read_input_data(ctx.spark, run_config.input_config)

        anomaly_engine = AnomalyEngine(ctx.workspace_client, ctx.spark)
        anomaly_engine.train(
            df=df,
            columns=anomaly_config.columns,
            segment_by=anomaly_config.segment_by,
            model_name=model_name,
            registry_table=registry_table,
        )
