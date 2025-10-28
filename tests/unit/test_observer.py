"""Unit tests for DQMetricsObserver class."""

from pyspark.sql import Observation
from pyspark.sql.connect.observation import Observation as SparkConnectObservation
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import DefaultColumnNames


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


def test_dq_observer_observation_id_uniqueness():
    observer1 = DQMetricsObserver()
    observer2 = DQMetricsObserver()
    assert observer1.observation_id != observer2.observation_id


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
    """Test that DQMetricsObserver uses correct default column names."""
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
