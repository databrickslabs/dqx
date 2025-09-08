"""Unit tests for DQObserver class."""

from pyspark.sql import Observation
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import DefaultColumnNames


def test_dq_observer_default_initialization():
    """Test DQObserver default initialization."""
    observer = DQMetricsObserver()
    assert observer.name == "dqx"
    assert observer.custom_metrics is None
    assert observer.result_columns == {}

    expected_default_metrics = [
        "count(1) as input_count",
        "count(case when _errors is not null then 1 end) as error_count",
        "count(case when _warnings is not null then 1 end) as warning_count",
        "count(case when _errors is null and _warnings is null then 1 end) as valid_count",
    ]
    assert observer.metrics == expected_default_metrics


def test_dq_observer_with_custom_metrics():
    """Test DQObserver with custom metrics."""
    custom_metrics = ["avg(age) as avg_age", "count(case when age > 65 then 1 end) as senior_count"]

    observer = DQMetricsObserver(name="custom_observer", custom_metrics=custom_metrics)
    assert observer.name == "custom_observer"
    assert observer.custom_metrics == custom_metrics

    expected_metrics = [
        "count(1) as input_count",
        "count(case when _errors is not null then 1 end) as error_count",
        "count(case when _warnings is not null then 1 end) as warning_count",
        "count(case when _errors is null and _warnings is null then 1 end) as valid_count",
        "avg(age) as avg_age",
        "count(case when age > 65 then 1 end) as senior_count",
    ]
    assert observer.metrics == expected_metrics


def test_dq_observer_with_custom_result_columns():
    """Test DQObserver with custom result column names."""
    custom_columns = {"errors_column": "custom_errors", "warnings_column": "custom_warnings"}
    observer = DQMetricsObserver(result_columns=custom_columns)

    expected_default_metrics = [
        "count(1) as input_count",
        "count(case when custom_errors is not null then 1 end) as error_count",
        "count(case when custom_warnings is not null then 1 end) as warning_count",
        "count(case when custom_errors is null and custom_warnings is null then 1 end) as valid_count",
    ]
    assert observer.metrics == expected_default_metrics


def test_dq_observer_with_custom_metrics_and_columns():
    """Test DQObserver with both custom metrics and custom result columns."""
    custom_metrics = ["max(salary) as max_salary"]
    custom_columns = {"errors_column": "issues", "warnings_column": "alerts"}

    observer = DQMetricsObserver(custom_metrics=custom_metrics, result_columns=custom_columns)

    expected_metrics = [
        "count(1) as input_count",
        "count(case when issues is not null then 1 end) as error_count",
        "count(case when alerts is not null then 1 end) as warning_count",
        "count(case when issues is null and alerts is null then 1 end) as valid_count",
        "max(salary) as max_salary",
    ]
    assert observer.metrics == expected_metrics


def test_dq_observer_empty_custom_metrics():
    """Test DQObserver with empty custom metrics list."""
    observer = DQMetricsObserver(custom_metrics=[])

    expected_default_metrics = [
        "count(1) as input_count",
        "count(case when _errors is not null then 1 end) as error_count",
        "count(case when _warnings is not null then 1 end) as warning_count",
        "count(case when _errors is null and _warnings is null then 1 end) as valid_count",
    ]
    assert observer.metrics == expected_default_metrics


def test_dq_observer_default_column_names():
    """Test that DQObserver uses correct default column names."""
    observer = DQMetricsObserver()
    errors_column = DefaultColumnNames.ERRORS.value
    warnings_column = DefaultColumnNames.WARNINGS.value

    assert f"count(case when {errors_column} is not null then 1 end) as error_count" in observer.metrics
    assert f"count(case when {warnings_column} is not null then 1 end) as warning_count" in observer.metrics
    assert (
        f"count(case when {errors_column} is null and {warnings_column} is null then 1 end) as valid_count"
        in observer.metrics
    )


def test_dq_observer_observation_property():
    """Test that the observation property creates a Spark Observation."""
    observer = DQMetricsObserver(name="test_obs")
    observation = observer.observation
    assert isinstance(observation, Observation)
    assert observation is not None
