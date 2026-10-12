from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, create_autospec

import pytest
import mlflow
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import RegisteredModelsAPI, ModelVersionsAPI
from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.model_config import (
    AnomalyModelRecord,
    FeatureEngineering,
    GroupingConfig,
    ModelIdentity,
    TrainingMetadata,
    compute_config_hash,
)
from databricks.labs.dqx.config import AnomalyConfig, InputConfig, RunConfig
from databricks.labs.dqx.contexts.workflow_context import WorkflowContext
from databricks.labs.dqx.io import read_input_data
from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.delivery import WebhookClient
from databricks.labs.dqx.actions.secrets import SecretResolver
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler


@pytest.fixture
def training_inputs():
    """No Spark session or workspace is needed to reject malformed anomaly configuration."""
    spark = create_autospec(SparkSession, instance=True)
    spark.version = "3.5.0"
    df = create_autospec(DataFrame, instance=True)
    df.columns = ["amount", "region", "event_time"]
    df.schema = T.StructType(
        [
            T.StructField("amount", T.DoubleType()),
            T.StructField("region", T.StringType()),
            T.StructField("event_time", T.TimestampType()),
        ]
    )
    return spark, df


@pytest.fixture
def action_services() -> ActionServices:
    """Create an ActionServices with autospec'd secret resolver and webhook client."""
    secret_resolver = create_autospec(SecretResolver, instance=True)
    webhook_client = create_autospec(WebhookClient, instance=True)
    return ActionServices(secret_resolver=secret_resolver, webhook_client=webhook_client)


@pytest.fixture
def action_context() -> ActionContext:
    """Create an ActionContext with a fixed run time for deterministic tests."""
    return ActionContext(
        metrics={"error_row_count": 5},
        run_id="run-abc",
        run_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture(name="mock_workspace_client")
def fixture_mock_workspace_client():
    """Create mock WorkspaceClient."""
    return MagicMock(spec=WorkspaceClient)


@pytest.fixture(name="mock_spark")
def fixture_mock_spark():
    """Create mock SparkSession."""
    return Mock()


@pytest.fixture
def generator(mock_workspace_client, mock_spark):
    """Create DQGenerator instance."""
    inst = DQGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
    inst.llm_engine = None
    return inst


@pytest.fixture
def profiler(mock_workspace_client, mock_spark):
    """Create DQProfiler instance."""
    inst = DQProfiler(workspace_client=mock_workspace_client, spark=mock_spark)
    inst.llm_engine = None
    return inst


@pytest.fixture
def anomaly_workflow_context() -> WorkflowContext:
    """Isolated workflow input with strict Spark and workspace boundaries."""
    context = create_autospec(WorkflowContext, instance=True)
    context.spark = create_autospec(SparkSession, instance=True)
    context.workspace_client = create_autospec(WorkspaceClient, instance=True)
    context.run_config = RunConfig(
        name="Anomaly training",
        input_config=InputConfig(location="catalog.schema.input"),
        anomaly_config=AnomalyConfig(model_name="catalog.schema.model", registry_table="catalog.schema.registry"),
    )
    return context


@pytest.fixture
def anomaly_workflow_dependencies() -> tuple[Mock, Mock]:
    """Inject the real reader/engine signatures without opening external services."""
    reader = create_autospec(read_input_data)
    reader.return_value = create_autospec(DataFrame, instance=True)
    engine_factory = create_autospec(AnomalyEngine)
    return reader, engine_factory


@pytest.fixture
def anomaly_model_record() -> AnomalyModelRecord:
    """Real persisted-record value for discovery and scoring rejection tests."""
    return AnomalyModelRecord(
        identity=ModelIdentity("catalog.schema.model", "models:/catalog.schema.model/1", "IsolationForest", "run"),
        training=TrainingMetadata(["a", "b"], {}, 100, datetime(2024, 1, 1)),
        features=FeatureEngineering(),
        grouping=GroupingConfig(config_hash=compute_config_hash(["a", "b"], None)),
    )


@pytest.fixture
def anomaly_experiment_boundaries() -> tuple[Mock, Mock]:
    """Autospec MLflow lookup and deletion without any tracking backend."""
    lookup = create_autospec(mlflow.get_experiment_by_name)
    delete = create_autospec(mlflow.delete_experiment)
    return lookup, delete


@pytest.fixture
def anomaly_cleanup_workspace() -> Mock:
    """Strict catalog boundaries for test-resource cleanup."""
    workspace = create_autospec(WorkspaceClient, instance=True)
    workspace.registered_models = create_autospec(RegisteredModelsAPI, instance=True)
    workspace.model_versions = create_autospec(ModelVersionsAPI, instance=True)
    return workspace
