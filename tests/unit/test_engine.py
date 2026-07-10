import re
from datetime import datetime, timezone
from unittest.mock import create_autospec, Mock

import pytest
import pyspark.sql.functions as F
from pyspark.errors import AnalysisException
from pyspark.sql import Column, SparkSession

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from databricks.labs.dqx import engine as engine_module
from databricks.labs.dqx.__about__ import __version__
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.checks_storage import (
    BaseChecksStorageHandlerFactory,
    ChecksStorageHandler,
    BaseChecksStorageConfig,
)
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.engine import DQEngine, DQEngineCore
from databricks.labs.dqx.engine import InvalidCheckError, InvalidParameterError
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import DQDatasetRule, CHECK_FUNC_REGISTRY_ORIGINAL_COLUMNS_PRESELECTION
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.rule import DQRowRule, register_rule, requires_dbr_version


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


def test_engine_core_suppress_skipped_default():
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)

    engine_core = DQEngineCore(spark=spark_mock, workspace_client=ws)

    assert engine_core.suppress_skipped is False


def test_engine_core_suppress_skipped_enabled():
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    extra_params = ExtraParams(suppress_skipped=True)

    engine_core = DQEngineCore(spark=spark_mock, workspace_client=ws, extra_params=extra_params)

    assert engine_core.suppress_skipped is True


def test_engine_creation_no_workspace_connection(mock_workspace_client, mock_spark):
    mock_workspace_client.clusters.select_spark_version.side_effect = DatabricksError()

    with pytest.raises(DatabricksError):
        DQEngine(spark=mock_spark, workspace_client=mock_workspace_client)
    with pytest.raises(DatabricksError):
        DQEngineCore(spark=mock_spark, workspace_client=mock_workspace_client)


def test_get_streaming_metrics_listener_invalid_engine(mock_workspace_client, mock_spark):
    # Inject a non-DQEngineCore engine (autospec of the base class) to exercise the engine-type guard.
    non_core_engine = create_autospec(DQEngineBase)
    engine = DQEngine(mock_workspace_client, mock_spark, engine=non_core_engine)
    with pytest.raises(InvalidParameterError, match="Metrics cannot be collected for engine with type"):
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


@pytest.mark.parametrize(
    ("save_method_name", "apply_method_name"),
    [
        ("apply_checks_and_save_in_table", "apply_checks"),
        ("apply_checks_by_metadata_and_save_in_table", "apply_checks_by_metadata"),
    ],
)
def test_apply_checks_and_save_in_table_allows_metrics_only(
    mock_workspace_client, mock_spark, monkeypatch, save_method_name, apply_method_name
):
    engine = DQEngine(mock_workspace_client, mock_spark, observer=DQMetricsObserver())
    checked_df = Mock()
    observation = Mock()
    observation.get = {"input_row_count": 2}
    apply_checks = Mock(return_value=(checked_df, observation))
    save_summary_metrics = Mock()
    read_input_data = Mock(return_value=Mock())

    monkeypatch.setattr(engine, apply_method_name, apply_checks)
    monkeypatch.setattr(engine, "save_summary_metrics", save_summary_metrics)
    monkeypatch.setattr(engine_module, "read_input_data", read_input_data)

    save_method = getattr(engine, save_method_name)
    save_method(
        input_config=InputConfig(location="catalog.schema.input"),
        checks=[],
        metrics_config=OutputConfig(location="catalog.schema.metrics"),
    )

    checked_df.count.assert_called_once_with()
    save_summary_metrics.assert_called_once()
    _, save_metrics_kwargs = save_summary_metrics.call_args
    assert save_metrics_kwargs["observed_metrics"] == {"input_row_count": 2}
    assert save_metrics_kwargs["metrics_config"] == OutputConfig(location="catalog.schema.metrics")


@pytest.mark.parametrize(
    "save_method_name",
    ["apply_checks_and_save_in_table", "apply_checks_by_metadata_and_save_in_table"],
)
def test_apply_checks_and_save_in_table_metrics_only_requires_observer(
    mock_workspace_client, mock_spark, save_method_name
):
    engine = DQEngine(mock_workspace_client, mock_spark)
    save_method = getattr(engine, save_method_name)

    with pytest.raises(InvalidParameterError, match="Metrics cannot be collected for engine with no observer"):
        save_method(
            input_config=InputConfig(location="catalog.schema.input"),
            checks=[],
            metrics_config=OutputConfig(location="catalog.schema.metrics"),
        )


@pytest.mark.parametrize(
    "save_method_name",
    ["apply_checks_and_save_in_table", "apply_checks_by_metadata_and_save_in_table"],
)
def test_apply_checks_and_save_in_table_metrics_with_output_requires_observer(
    mock_workspace_client, mock_spark, save_method_name
):
    """metrics_config requested alongside output_config but with no observer must fail fast rather than
    silently skipping the metrics table."""
    engine = DQEngine(mock_workspace_client, mock_spark)  # no observer
    save_method = getattr(engine, save_method_name)

    with pytest.raises(InvalidParameterError, match="Metrics cannot be collected for engine with no observer"):
        save_method(
            input_config=InputConfig(location="catalog.schema.input"),
            output_config=OutputConfig(location="catalog.schema.output"),
            checks=[],
            metrics_config=OutputConfig(location="catalog.schema.metrics"),
        )


@pytest.mark.parametrize(
    "save_method_name",
    ["apply_checks_and_save_in_table", "apply_checks_by_metadata_and_save_in_table"],
)
def test_apply_checks_and_save_in_table_metrics_only_streaming_unsupported(
    mock_workspace_client, mock_spark, save_method_name
):
    engine = DQEngine(mock_workspace_client, mock_spark, observer=DQMetricsObserver())
    save_method = getattr(engine, save_method_name)

    with pytest.raises(InvalidParameterError, match="Metrics-only writes are not supported for streaming input"):
        save_method(
            input_config=InputConfig(location="catalog.schema.input", is_streaming=True),
            checks=[],
            metrics_config=OutputConfig(location="catalog.schema.metrics"),
        )


def test_apply_checks_and_save_in_table_raises_when_no_destination_configs(mock_workspace_client, mock_spark):
    engine = DQEngine(mock_workspace_client, mock_spark)
    with pytest.raises(
        InvalidParameterError,
        match="At least one of 'output_config', 'quarantine_config' or 'metrics_config' must be provided",
    ):
        engine.apply_checks_and_save_in_table(
            input_config=InputConfig(location="catalog.schema.input"),
            checks=[],
        )


def test_apply_checks_by_metadata_and_save_in_table_raises_when_no_destination_configs(
    mock_workspace_client, mock_spark
):
    engine = DQEngine(mock_workspace_client, mock_spark)
    with pytest.raises(
        InvalidParameterError,
        match="At least one of 'output_config', 'quarantine_config' or 'metrics_config' must be provided",
    ):
        engine.apply_checks_by_metadata_and_save_in_table(
            input_config=InputConfig(location="catalog.schema.input"),
            checks=[],
        )


# ---------------------------------------------------------------------------
# Regression: _preselect_original_columns uses DQRule.replace() correctly
# ---------------------------------------------------------------------------


def _preselect_dataset_check():
    """Minimal dataset check accepted by _preselect_original_columns tests."""
    return F.lit(True), lambda df, col_name: df.withColumn(col_name, F.lit(True))


def test_preselect_original_columns_no_op_when_func_not_registered():
    """_preselect_original_columns returns the same rule object when the check function
    is not in CHECK_FUNC_REGISTRY_ORIGINAL_COLUMNS_PRESELECTION."""
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    engine = DQEngineCore(spark=spark_mock, workspace_client=ws)

    rule = DQDatasetRule(check_func=_preselect_dataset_check)
    assert _preselect_dataset_check.__name__ not in CHECK_FUNC_REGISTRY_ORIGINAL_COLUMNS_PRESELECTION

    df = Mock()
    df.columns = ["id", "name", "_errors", "_warnings"]
    result = engine._preselect_original_columns(df, rule)

    assert result is rule  # no change — same object returned


def test_preselect_original_columns_injects_df_columns(monkeypatch):
    """_preselect_original_columns must inject df columns (minus result columns) into
    check_func_kwargs["columns"] via DQRule.replace() when the check function is registered
    for original-columns preselection and no columns are already provided."""
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    engine = DQEngineCore(spark=spark_mock, workspace_client=ws)

    monkeypatch.setattr(
        engine_module,
        "CHECK_FUNC_REGISTRY_ORIGINAL_COLUMNS_PRESELECTION",
        {_preselect_dataset_check.__name__},
    )

    rule = DQDatasetRule(check_func=_preselect_dataset_check)

    df = Mock()
    df.columns = ["id", "name", "_errors", "_warnings"]

    result = engine._preselect_original_columns(df, rule)

    assert result is not rule  # new object produced by replace()
    assert result.check_func_kwargs["columns"] == ["id", "name"]


def test_preselect_original_columns_no_op_when_columns_already_in_kwargs(monkeypatch):
    """_preselect_original_columns must not overwrite columns already in check_func_kwargs."""
    spark_mock = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    engine = DQEngineCore(spark=spark_mock, workspace_client=ws)

    monkeypatch.setattr(
        engine_module,
        "CHECK_FUNC_REGISTRY_ORIGINAL_COLUMNS_PRESELECTION",
        {_preselect_dataset_check.__name__},
    )

    rule = DQDatasetRule(check_func=_preselect_dataset_check, check_func_kwargs={"columns": ["id"]})

    df = Mock()
    df.columns = ["id", "name", "_errors", "_warnings"]

    result = engine._preselect_original_columns(df, rule)

    assert result is rule  # early-return — columns already provided


# --- DBR version validation error path tests ---


@requires_dbr_version("15.0")
@register_rule("row")
def _check_requires_dbr_15(column: str) -> Column:
    return make_condition(F.col(column).isNull(), f"'{column}' is null", f"{column}_is_null")


@requires_dbr_version("17.1")
@register_rule("row")
def _check_requires_dbr_17_1(column: str) -> Column:
    return make_condition(F.col(column).isNull(), f"'{column}' is null", f"{column}_is_null")


@requires_dbr_version("999.0")
@register_rule("row")
def _check_requires_dbr_max(column: str) -> Column:
    return make_condition(F.col(column).isNull(), f"'{column}' is null", f"{column}_is_null")


def _engine_with_sql_side_effect(side_effect):
    ws = create_autospec(WorkspaceClient)
    spark = create_autospec(SparkSession)
    spark.sql.side_effect = side_effect
    return DQEngineCore(workspace_client=ws, spark=spark)


def _engine_returning_dbr_version(version_str):
    ws = create_autospec(WorkspaceClient)
    spark = create_autospec(SparkSession)
    row = Mock()
    row.__getitem__ = Mock(return_value=version_str)
    spark.sql.return_value.collect.return_value = [row]
    return DQEngineCore(workspace_client=ws, spark=spark)


def test_apply_checks_raises_invalid_check_error_when_spark_sql_fails():
    engine = _engine_with_sql_side_effect(AnalysisException("current_version() not found"))
    df = Mock()
    df.columns = ["a"]
    with pytest.raises(InvalidCheckError, match="can only run on Databricks Runtime"):
        engine.apply_checks(df, [DQRowRule(check_func=_check_requires_dbr_15, column="a")])


def test_apply_checks_raises_invalid_check_error_when_dbr_version_unparseable():
    engine = _engine_returning_dbr_version("custom-build")
    df = Mock()
    df.columns = ["a"]
    with pytest.raises(InvalidCheckError, match="Cannot parse Databricks Runtime version"):
        engine.apply_checks(df, [DQRowRule(check_func=_check_requires_dbr_15, column="a")])


@pytest.mark.parametrize("dbr_version", ["17.0", "16.4", "9.1"])
def test_apply_checks_raises_when_minor_version_below_required(dbr_version):
    # Required 17.1; a lower (major, minor) - including 17.0, the same major - must fail. Guards against a
    # regression to major-only comparison, which would wrongly let 17.0 through.
    engine = _engine_returning_dbr_version(dbr_version)
    df = Mock()
    df.columns = ["a"]
    with pytest.raises(InvalidCheckError, match=r"require Databricks Runtime >= 17\.1"):
        engine.apply_checks(df, [DQRowRule(check_func=_check_requires_dbr_17_1, column="a")])


@pytest.mark.parametrize(
    "dbr_version, required_check",
    [
        # A suffixed runtime string such as "15.4 LTS" must parse to (15, 4) rather than hard-fail.
        ("15.4 LTS", _check_requires_dbr_17_1),
        # Serverless reports e.g. "18.2.x-photon-scala2.13"; it must parse to (18, 2), not hard-fail.
        ("18.2.x-photon-scala2.13", _check_requires_dbr_max),
        # Bare-major serverless form below the required major: "16.x" must fail a 17.1 requirement.
        ("16.x-photon-scala2.13", _check_requires_dbr_17_1),
    ],
)
def test_apply_checks_parses_dbr_version_with_suffix(dbr_version, required_check):
    # Asserting the version is compared (and echoed back) against a higher requirement proves the leading
    # major.minor was parsed and the suffix ignored, rather than the parse hard-failing.
    engine = _engine_returning_dbr_version(dbr_version)
    df = Mock()
    df.columns = ["a"]
    with pytest.raises(
        InvalidCheckError,
        match=rf"require Databricks Runtime >= .*but the current version is {re.escape(dbr_version)}",
    ):
        engine.apply_checks(df, [DQRowRule(check_func=required_check, column="a")])


@pytest.mark.parametrize("dbr_version", ["17.x-photon-scala2.13", "18.x-photon-scala2.13", "17.x"])
def test_validate_dbr_version_bare_major_serverless_satisfies_same_major(dbr_version):
    engine = _engine_returning_dbr_version(dbr_version)
    engine._validate_dbr_version_requirements([DQRowRule(check_func=_check_requires_dbr_17_1, column="a")])
