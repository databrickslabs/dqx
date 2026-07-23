from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions.base import ActionContext, ActionServices
from databricks.labs.dqx.actions.delivery import WebhookClient
from databricks.labs.dqx.actions.secrets import SecretResolver
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler


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


@pytest.fixture
def mock_workspace_client():
    """Create mock WorkspaceClient."""
    return MagicMock(spec=WorkspaceClient)


@pytest.fixture
def mock_spark():
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
