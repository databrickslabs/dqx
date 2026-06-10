"""Unit tests for DQMetricsObserver class."""

from pyspark.sql import Observation
from pyspark.sql.connect.observation import Observation as SparkConnectObservation
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.reporting_columns import DefaultColumnNames


def test_dq_observer_default_initialization():
    observer = DQMetricsObserver()
    assert observer.name == "dqx"
    assert observer.custom_metrics is None
    assert observer.get_metrics() == _default_metrics()


def test_dq_observer_with_custom_metrics():
    custom = ["avg(age) as avg_age", "count(case when age > 65 then 1 end) as senior_count"]
    observer = DQMetricsObserver(name="custom_observer", custom_metrics=custom)
    assert observer.name == "custom_observer"
    assert observer.get_metrics() == _default_metrics() + custom


def test_dq_observer_empty_custom_metrics():
    observer = DQMetricsObserver(custom_metrics=[])
    assert observer.get_metrics() == _default_metrics()


def test_dq_observer_run_id_uniqueness():
    assert DQMetricsObserver().id != DQMetricsObserver().id


def test_dq_observer_id_overwrite():
    observer = DQMetricsObserver(id_overwrite="1")
    assert observer.id == "1"


def test_dq_observer_default_column_names():
    observer = DQMetricsObserver()
    err = DefaultColumnNames.ERRORS.value
    warn = DefaultColumnNames.WARNINGS.value
    assert observer.get_metrics() == _default_metrics(err, warn)


def test_dq_observer_custom_column_names():
    observer = DQMetricsObserver()
    observer.set_column_names(error_column_name="my_errors", warning_column_name="my_warnings")
    assert observer.get_metrics() == _default_metrics("my_errors", "my_warnings")


def test_dq_observer_observation_property():
    observation = DQMetricsObserver(name="test_obs").observation
    assert isinstance(observation, Observation | SparkConnectObservation)


def test_get_metrics_without_check_names():
    observer = DQMetricsObserver()
    assert observer.get_metrics() == _default_metrics()


def test_get_metrics_with_checks_single():
    observer = DQMetricsObserver()
    metrics = observer.get_metrics(["id_is_not_null"])
    expected = _default_metrics() + [_check_metrics_expr(["id_is_not_null"])]
    assert metrics == expected


def test_get_metrics_with_checks_multiple():
    checks = ["id_is_not_null", "name_is_not_empty", "age_in_range"]
    observer = DQMetricsObserver()
    metrics = observer.get_metrics(checks)
    expected = _default_metrics() + [_check_metrics_expr(checks)]
    assert metrics == expected


def test_get_metrics_with_checks_ordering_with_custom():
    custom = ["avg(age) as avg_age"]
    observer = DQMetricsObserver(custom_metrics=custom)
    metrics = observer.get_metrics(["my_check"])
    expected = _default_metrics() + [_check_metrics_expr(["my_check"])] + custom
    assert metrics == expected


def test_get_metrics_with_checks_uses_custom_column_names():
    observer = DQMetricsObserver()
    observer.set_column_names(error_column_name="dq_errors", warning_column_name="dq_warnings")
    metrics = observer.get_metrics(["my_check"])
    expected = _default_metrics("dq_errors", "dq_warnings") + [
        _check_metrics_expr(["my_check"], "dq_errors", "dq_warnings")
    ]
    assert metrics == expected


def test_get_metrics_with_checks_escapes_single_quotes():
    observer = DQMetricsObserver()
    metrics = observer.get_metrics(["it's_valid"])
    expected = _default_metrics() + [_check_metrics_expr(["it's_valid"])]
    assert metrics == expected


def test_get_metrics_with_checks_empty_list():
    observer = DQMetricsObserver()
    metrics = observer.get_metrics([])
    assert metrics == _default_metrics()


def test_get_metrics_idempotent():
    """Verifies that repeated calls with the same args return equal results."""
    observer = DQMetricsObserver()
    assert observer.get_metrics() == observer.get_metrics()
    assert observer.get_metrics(["a"]) == observer.get_metrics(["a"])


def _default_metrics(err="_errors", warn="_warnings"):
    return [
        "count(1) as input_row_count",
        f"count(case when {err} is not null then 1 end) as error_row_count",
        f"count(case when {warn} is not null then 1 end) as warning_row_count",
        f"count(case when {err} is null and {warn} is null then 1 end) as valid_row_count",
    ]


def _check_metrics_expr(check_names, err="_errors", warn="_warnings"):
    # Note: this helper derives expected values from production code, so it validates structural
    # properties (ordering, column-name propagation) but not SQL correctness. The integration
    # tests in test_summary_metrics.py run the generated SQL against real Spark to cover that.
    observer = DQMetricsObserver()
    observer.set_column_names(error_column_name=err, warning_column_name=warn)
    return observer.get_metrics(check_names)[len(_default_metrics()) :][-1]
