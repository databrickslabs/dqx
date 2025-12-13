from unittest.mock import MagicMock, Mock

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.profiler import DQProfiler


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
