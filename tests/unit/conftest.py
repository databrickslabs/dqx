from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, create_autospec

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from databricks.sdk import WorkspaceClient
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
