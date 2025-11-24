from unittest.mock import create_autospec
from unittest.mock import MagicMock
import pytest
from pyspark.sql import SparkSession
from databricks.sdk.errors import DatabricksError
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine, DQEngineCore


def test_engine_creation():
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    assert DQEngine(spark=spark_mock, workspace_client=ws)
    assert DQEngineCore(spark=spark_mock, workspace_client=ws)


def test_engine_creation_no_workspace_connection():
    spark_mock = create_autospec(SparkSession)
    ws = MagicMock(spec=WorkspaceClient)
    ws.clusters.select_spark_version.side_effect = DatabricksError()

    with pytest.raises(DatabricksError):
        DQEngine(spark=spark_mock, workspace_client=ws)
    with pytest.raises(DatabricksError):
        DQEngineCore(spark=spark_mock, workspace_client=ws)
