"""Unit tests for DQMetricsObserver class."""

import pytest
from pyspark.sql import Observation
from pyspark.sql.connect.observation import Observation as SparkConnectObservation
from databricks.labs.dqx.errors import DQXError
from databricks.labs.dqx.metrics_observer import DQMetricsObserver, _sanitize_metric_alias
from databricks.labs.dqx.reporting_columns import DefaultColumnNames


def test_dq_observer_default_initialization():
    """Test DQMetricsObserver default initialization."""
    observer = DQMetricsObserver()
    assert observer.name == "dqx"
    assert observer.custom_metrics is None

    expected_default_metrics = [
        "count(1) as input_row_count",
        "count(case when _errors is not null then 1 end) as error_row_count",
        "count(case when _warnings is not null then 1 end) as warning_row_count",
        "count(case when _errors is null and _warnings is null then 1 end) as valid_row_count",
    ]
    assert observer.metrics == expected_default_metrics


def test_dq_observer_with_custom_metrics():
    """Test DQMetricsObserver with custom metrics."""
    custom_metrics = ["avg(age) as avg_age", "count(case when age > 65 then 1 end) as senior_count"]

    observer = DQMetricsObserver(name="custom_observer", custom_metrics=custom_metrics)
    assert observer.name == "custom_observer"
    assert observer.custom_metrics == custom_metrics

    expected_metrics = [
        "count(1) as input_row_count",
        "count(case when _errors is not null then 1 end) as error_row_count",
        "count(case when _warnings is not null then 1 end) as warning_row_count",
        "count(case when _errors is null and _warnings is null then 1 end) as valid_row_count",
        "avg(age) as avg_age",
        "count(case when age > 65 then 1 end) as senior_count",
    ]
    assert observer.metrics == expected_metrics


def test_dq_observer_empty_custom_metrics():
    """Test DQMetricsObserver with empty custom metrics list."""
    observer = DQMetricsObserver(custom_metrics=[])

    expected_default_metrics = [
        "count(1) as input_row_count",
        "count(case when _errors is not null then 1 end) as error_row_count",
        "count(case when _warnings is not null then 1 end) as warning_row_count",
        "count(case when _errors is null and _warnings is null then 1 end) as valid_row_count",
    ]
    assert observer.metrics == expected_default_metrics


def test_dq_observer_run_id_uniqueness():
    observer1 = DQMetricsObserver()
    observer2 = DQMetricsObserver()
    assert observer1.id != observer2.id


def test_dq_observer_id_overwrite():
    run_id_overwrite = "1"
    observer = DQMetricsObserver(id_overwrite=run_id_overwrite)
    assert observer.id == run_id_overwrite


def test_dq_observer_default_column_names():
    """Test that DQMetricsObserver uses correct default column names."""
    observer = DQMetricsObserver()
    errors_column = DefaultColumnNames.ERRORS.value
    warnings_column = DefaultColumnNames.WARNINGS.value

    assert f"count(case when {errors_column} is not null then 1 end) as error_row_count" in observer.metrics
    assert f"count(case when {warnings_column} is not null then 1 end) as warning_row_count" in observer.metrics
    assert (
        f"count(case when {errors_column} is null and {warnings_column} is null then 1 end) as valid_row_count"
        in observer.metrics
    )


def test_dq_observer_custom_column_names():
    """Test that DQMetricsObserver uses custom column names in metrics."""
    observer = DQMetricsObserver()
    errors_column = "my_errors"
    warnings_column = "my_warnings"
    observer.set_column_names(error_column_name=errors_column, warning_column_name=warnings_column)

    assert f"count(case when {errors_column} is not null then 1 end) as error_row_count" in observer.metrics
    assert f"count(case when {warnings_column} is not null then 1 end) as warning_row_count" in observer.metrics
    assert (
        f"count(case when {errors_column} is null and {warnings_column} is null then 1 end) as valid_row_count"
        in observer.metrics
    )


def test_dq_observer_observation_property():
    """Test that the observation property creates a Spark Observation."""
    observer = DQMetricsObserver(name="test_obs")
    observation = observer.observation
    assert observation is not None
    assert isinstance(observation, Observation | SparkConnectObservation)


def test_dq_observer_per_check_metrics_without_check_names():
    """Test that no per-check metrics are generated when no check names are set."""
    observer = DQMetricsObserver(track_extended_metrics=True)
    expected_default_metrics = [
        "count(1) as input_row_count",
        "count(case when _errors is not null then 1 end) as error_row_count",
        "count(case when _warnings is not null then 1 end) as warning_row_count",
        "count(case when _errors is null and _warnings is null then 1 end) as valid_row_count",
    ]
    assert observer.metrics == expected_default_metrics


def test_dq_observer_per_check_metrics_with_check_names():
    """Test that per-check metrics are generated after setting check names."""
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_check_names(["id_is_not_null", "name_is_not_empty"])

    metrics = observer.metrics
    assert "count(1) as input_row_count" in metrics

    assert (
        "count(case when exists(_errors, x -> x.name = 'id_is_not_null') then 1 end) " "as id_is_not_null_error_count"
    ) in metrics
    assert (
        "count(case when exists(_warnings, x -> x.name = 'id_is_not_null') then 1 end) "
        "as id_is_not_null_warning_count"
    ) in metrics
    assert (
        "count(case when exists(_errors, x -> x.name = 'name_is_not_empty') then 1 end) "
        "as name_is_not_empty_error_count"
    ) in metrics
    assert (
        "count(case when exists(_warnings, x -> x.name = 'name_is_not_empty') then 1 end) "
        "as name_is_not_empty_warning_count"
    ) in metrics


def test_dq_observer_per_check_metrics_ordering():
    """Test that per-check metrics appear between default and custom metrics."""
    custom_metrics = ["avg(age) as avg_age"]
    observer = DQMetricsObserver(custom_metrics=custom_metrics, track_extended_metrics=True)
    observer.set_check_names(["my_check"])

    metrics = observer.metrics
    # Default metrics come first (4 items), then per-check (2 per check), then custom
    assert metrics[0] == "count(1) as input_row_count"
    assert "my_check_error_count" in metrics[4]
    assert "my_check_warning_count" in metrics[5]
    assert metrics[6] == "avg(age) as avg_age"


def test_dq_observer_per_check_metrics_with_custom_column_names():
    """Test that per-check metrics use custom error/warning column names."""
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_column_names(error_column_name="dq_errors", warning_column_name="dq_warnings")
    observer.set_check_names(["my_check"])

    metrics = observer.metrics
    assert (
        "count(case when exists(dq_errors, x -> x.name = 'my_check') then 1 end) " "as my_check_error_count"
    ) in metrics
    assert (
        "count(case when exists(dq_warnings, x -> x.name = 'my_check') then 1 end) " "as my_check_warning_count"
    ) in metrics


def test_dq_observer_per_check_metrics_sanitizes_special_characters():
    """Test that check names with special characters are sanitized in metric aliases."""
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_check_names(["check-with-dashes", "check.with.dots"])

    metrics = observer.metrics
    # Aliases should be sanitized: dashes and dots become underscores
    assert any("check_with_dashes_error_count" in m for m in metrics)
    assert any("check_with_dots_error_count" in m for m in metrics)


def test_dq_observer_set_check_names_replaces_previous():
    """Test that calling set_check_names replaces previously set names."""
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_check_names(["first_check"])
    assert any("first_check_error_count" in m for m in observer.metrics)

    observer.set_check_names(["second_check"])
    assert not any("first_check_error_count" in m for m in observer.metrics)
    assert any("second_check_error_count" in m for m in observer.metrics)


def test_sanitize_metric_alias_empty_raises_error():
    """Test that _sanitize_metric_alias raises DQXError for names that produce empty aliases."""
    with pytest.raises(DQXError, match="Sanitizing check '!!!' produces an empty alias"):
        _sanitize_metric_alias("!!!")


def test_dq_observer_duplicate_alias_collision_raises_error():
    """Test that check names sanitizing to the same alias raise DQXError."""
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_check_names(["check-1", "check.1"])
    with pytest.raises(DQXError, match="produces alias 'check_1' which collides"):
        observer.metrics
