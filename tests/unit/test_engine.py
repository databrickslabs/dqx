from datetime import datetime, timezone
from unittest.mock import create_autospec, Mock

import pytest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.checks_storage import (
    BaseChecksStorageHandlerFactory,
    ChecksStorageHandler,
    BaseChecksStorageConfig,
)
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.engine import DQEngine, DQEngineCore
from databricks.labs.dqx.engine import InvalidParameterError
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import DQDatasetRule


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


def test_engine_core_skip_quietly_default():
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)

    engine_core = DQEngineCore(spark=spark_mock, workspace_client=ws)

    assert engine_core.skip_quietly is False


def test_engine_core_skip_quietly_enabled():
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    extra_params = ExtraParams(skip_quietly=True)

    engine_core = DQEngineCore(spark=spark_mock, workspace_client=ws, extra_params=extra_params)

    assert engine_core.skip_quietly is True


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


def _dummy_dataset_check(column: str):
    def apply(df):
        return df

    return F.lit(True).alias(f"{column}_dummy"), apply


def test_validate_result_column_collisions_errors_warns():
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    engine = DQEngineCore(spark=spark_mock, workspace_client=ws)
    df = Mock()
    df.columns = ["_errors", "a"]

    with pytest.raises(InvalidParameterError, match="reserved DQX result columns"):
        engine.apply_checks(df, [])


def test_validate_result_column_collisions_info_for_dataset_rule():
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    engine = DQEngineCore(spark=spark_mock, workspace_client=ws)
    df = Mock()
    df.columns = ["_dq_info", "a"]
    checks = [DQDatasetRule(check_func=_dummy_dataset_check, column="a")]

    with pytest.raises(InvalidParameterError, match="reserved DQX result columns"):
        engine.apply_checks(df, checks)


def test_apply_checks_and_save_in_table_raises_when_no_checks_and_no_location(mock_workspace_client, mock_spark):
    engine = DQEngine(mock_workspace_client, mock_spark)
    with pytest.raises(InvalidParameterError, match="Either 'checks_location' or 'checks' must be provided"):
        engine.apply_checks_and_save_in_table(
            input_config=InputConfig(location="catalog.schema.input"),
            output_config=OutputConfig(location="catalog.schema.output"),
        )


def test_apply_checks_by_metadata_and_save_in_table_raises_when_no_checks_and_no_location(
    mock_workspace_client, mock_spark
):
    engine = DQEngine(mock_workspace_client, mock_spark)
    with pytest.raises(InvalidParameterError, match="Either 'checks_location' or 'checks' must be provided"):
        engine.apply_checks_by_metadata_and_save_in_table(
            input_config=InputConfig(location="catalog.schema.input"),
            output_config=OutputConfig(location="catalog.schema.output"),
        )


def test_apply_checks_and_save_in_table_loads_checks_from_location(mock_workspace_client, mock_spark):
    mock_handler = create_autospec(ChecksStorageHandler)
    mock_handler.load.return_value = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}
    ]
    mock_config = create_autospec(BaseChecksStorageConfig)
    mock_factory = create_autospec(BaseChecksStorageHandlerFactory)
    mock_factory.create_for_location.return_value = (mock_handler, mock_config)

    engine = DQEngine(mock_workspace_client, mock_spark, checks_handler_factory=mock_factory)
    # We expect it to fail when trying to read input data (spark is mocked),
    # but the check-loading path should have been exercised successfully
    with pytest.raises(Exception):
        engine.apply_checks_and_save_in_table(
            input_config=InputConfig(location="catalog.schema.input"),
            output_config=OutputConfig(location="catalog.schema.output"),
            checks_location="catalog.schema.dq_checks",
            run_config_name="my_config",
        )

    mock_factory.create_for_location.assert_called_once_with(
        location="catalog.schema.dq_checks", run_config_name="my_config"
    )
    mock_handler.load.assert_called_once_with(mock_config)


def test_apply_checks_by_metadata_and_save_in_table_loads_checks_from_location(mock_workspace_client, mock_spark):
    mock_handler = create_autospec(ChecksStorageHandler)
    mock_handler.load.return_value = [
        {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}
    ]
    mock_config = create_autospec(BaseChecksStorageConfig)
    mock_factory = create_autospec(BaseChecksStorageHandlerFactory)
    mock_factory.create_for_location.return_value = (mock_handler, mock_config)

    engine = DQEngine(mock_workspace_client, mock_spark, checks_handler_factory=mock_factory)
    with pytest.raises(Exception):
        engine.apply_checks_by_metadata_and_save_in_table(
            input_config=InputConfig(location="catalog.schema.input"),
            output_config=OutputConfig(location="catalog.schema.output"),
            checks_location="catalog.schema.dq_checks",
            run_config_name="my_config",
        )

    mock_factory.create_for_location.assert_called_once_with(
        location="catalog.schema.dq_checks", run_config_name="my_config"
    )
    mock_handler.load.assert_called_once_with(mock_config)


def test_apply_checks_and_save_in_table_empty_checks_does_not_load_from_location(mock_workspace_client, mock_spark):
    mock_factory = create_autospec(BaseChecksStorageHandlerFactory)
    engine = DQEngine(mock_workspace_client, mock_spark, checks_handler_factory=mock_factory)

    # checks=[] with checks_location should NOT trigger loading from storage
    with pytest.raises(Exception):
        engine.apply_checks_and_save_in_table(
            input_config=InputConfig(location="catalog.schema.input"),
            output_config=OutputConfig(location="catalog.schema.output"),
            checks=[],
            checks_location="catalog.schema.dq_checks",
        )

    mock_factory.create_for_location.assert_not_called()
