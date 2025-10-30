from datetime import datetime, timezone
from unittest.mock import create_autospec
from unittest.mock import MagicMock
import pytest
from pyspark.sql import SparkSession
from databricks.sdk.errors import DatabricksError
from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.engine import DQEngine, DQEngineCore
from databricks.labs.dqx.metrics_observer import DQMetricsObserver


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


def test_engine_creation_no_workspace_connection():
    spark_mock = create_autospec(SparkSession)
    ws = MagicMock(spec=WorkspaceClient)
    ws.clusters.select_spark_version.side_effect = DatabricksError()

    with pytest.raises(DatabricksError):
        DQEngine(spark=spark_mock, workspace_client=ws)
    with pytest.raises(DatabricksError):
        DQEngineCore(spark=spark_mock, workspace_client=ws)
