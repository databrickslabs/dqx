from datetime import datetime, timezone
from unittest.mock import create_autospec, Mock
import pytest
from pyspark.sql import SparkSession
from databricks.sdk.errors import DatabricksError
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.engine import DQEngine, DQEngineCore
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.engine import InvalidParameterError, OutputConfig


def test_engine_creation():
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    assert DQEngine(spark=spark_mock, workspace_client=ws)
    assert DQEngineCore(spark=spark_mock, workspace_client=ws)


def test_engine_core_creation_with_extra_params_overwrite():
    run_time = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
    run_id = "2f9120cf-e9f2-446a-8278-12d508b00639"
    extra_params = ExtraParams(run_time_overwrite=run_time.isoformat(), run_id_overwrite=run_id)
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)

    engine_core = DQEngineCore(spark=spark_mock, workspace_client=ws, extra_params=extra_params)

    assert engine_core.run_time_overwrite == run_time
    assert engine_core.run_id == run_id


def test_engine_core_creation_with_extra_params_overwrite_and_observer():
    run_id = "2f9120cf-e9f2-446a-8278-12d508b00639"
    extra_params = ExtraParams(run_id_overwrite=run_id)
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    observer = DQMetricsObserver()

    engine_core = DQEngineCore(spark=spark_mock, workspace_client=ws, extra_params=extra_params, observer=observer)

    assert engine_core.run_id == run_id


def test_engine_core_creation_with_observer():
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    observer = DQMetricsObserver()

    engine_core = DQEngineCore(spark=spark_mock, workspace_client=ws, observer=observer)

    assert engine_core.run_id == observer.id


def test_engine_creation_no_workspace_connection(mock_workspace_client, mock_spark):
    mock_workspace_client.clusters.select_spark_version.side_effect = DatabricksError()

    with pytest.raises(DatabricksError):
        DQEngine(spark=mock_spark, workspace_client=mock_workspace_client)
    with pytest.raises(DatabricksError):
        DQEngineCore(spark=mock_spark, workspace_client=mock_workspace_client)


def test_get_streaming_metrics_listener_invalid_engine(mock_workspace_client, mock_spark):
    engine = DQEngine(mock_workspace_client, mock_spark)
    with pytest.raises(InvalidParameterError, match="Metrics cannot be collected for engine"):
        engine.get_streaming_metrics_listener(metrics_config=OutputConfig(location="dummy"))


def test_get_streaming_metrics_listener_no_observer(mock_workspace_client, mock_spark):
    engine = DQEngine(mock_workspace_client, mock_spark)
    with pytest.raises(InvalidParameterError, match="Metrics cannot be collected for engine with no observer"):
        engine.get_streaming_metrics_listener(metrics_config=OutputConfig(location="dummy"))


def test_verify_workspace_client_with_null_product_info(mock_spark):
    ws = create_autospec(WorkspaceClient)
    mock_config = Mock()
    setattr(mock_config, "_product_info", None)
    ws.config = mock_config

    DQEngine(spark=mock_spark, workspace_client=ws)

    assert getattr(mock_config, "_product_info") == ('dqx', __version__)


def test_verify_workspace_client_with_non_dqx_product_info(mock_spark):
    ws = create_autospec(WorkspaceClient)
    mock_config = Mock()
    setattr(mock_config, "_product_info", ('other-product', '1.0.0'))
    ws.config = mock_config

    DQEngine(spark=mock_spark, workspace_client=ws)

    assert getattr(mock_config, "_product_info") == ('dqx', __version__)
