"""Unit tests for DQMetricsObserver class."""

from pyspark.sql import Observation
from pyspark.sql.connect.observation import Observation as SparkConnectObservation
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.reporting_columns import DefaultColumnNames


def test_dq_observer_default_initialization():
    observer = DQMetricsObserver()
    assert observer.name == "dqx"
    assert observer.custom_metrics is None
    assert observer.metrics == _default_metrics()


def test_dq_observer_with_custom_metrics():
    custom = ["avg(age) as avg_age", "count(case when age > 65 then 1 end) as senior_count"]
    observer = DQMetricsObserver(name="custom_observer", custom_metrics=custom)
    assert observer.name == "custom_observer"
    assert observer.metrics == _default_metrics() + custom


def test_dq_observer_empty_custom_metrics():
    observer = DQMetricsObserver(custom_metrics=[])
    assert observer.metrics == _default_metrics()


def test_dq_observer_run_id_uniqueness():
    assert DQMetricsObserver().id != DQMetricsObserver().id


def test_dq_observer_id_overwrite():
    observer = DQMetricsObserver(id_overwrite="1")
    assert observer.id == "1"


def test_dq_observer_default_column_names():
    observer = DQMetricsObserver()
    err = DefaultColumnNames.ERRORS.value
    warn = DefaultColumnNames.WARNINGS.value
    assert observer.metrics == _default_metrics(err, warn)


def test_dq_observer_custom_column_names():
    observer = DQMetricsObserver()
    observer.set_column_names(error_column_name="my_errors", warning_column_name="my_warnings")
    assert observer.metrics == _default_metrics("my_errors", "my_warnings")


def test_dq_observer_observation_property():
    observation = DQMetricsObserver(name="test_obs").observation
    assert isinstance(observation, Observation | SparkConnectObservation)


def test_check_metrics_not_included_when_disabled():
    observer = DQMetricsObserver()
    observer.set_check_names(["my_check"])
    assert observer.metrics == _default_metrics()


def test_check_metrics_not_included_without_check_names():
    observer = DQMetricsObserver(track_extended_metrics=True)
    assert observer.metrics == _default_metrics()


def test_check_metrics_single_check():
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_check_names(["id_is_not_null"])
    expected = _default_metrics() + [_check_metrics_expr(["id_is_not_null"])]
    assert observer.metrics == expected


def test_check_metrics_multiple_checks():
    checks = ["id_is_not_null", "name_is_not_empty", "age_in_range"]
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_check_names(checks)
    expected = _default_metrics() + [_check_metrics_expr(checks)]
    assert observer.metrics == expected


def test_check_metrics_ordering_with_custom():
    custom = ["avg(age) as avg_age"]
    observer = DQMetricsObserver(custom_metrics=custom, track_extended_metrics=True)
    observer.set_check_names(["my_check"])
    expected = _default_metrics() + [_check_metrics_expr(["my_check"])] + custom
    assert observer.metrics == expected


def test_check_metrics_uses_custom_column_names():
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_column_names(error_column_name="dq_errors", warning_column_name="dq_warnings")
    observer.set_check_names(["my_check"])
    expected = _default_metrics("dq_errors", "dq_warnings") + [
        _check_metrics_expr(["my_check"], "dq_errors", "dq_warnings")
    ]
    assert observer.metrics == expected


def test_check_metrics_escapes_single_quotes():
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_check_names(["it's_valid"])
    expected = _default_metrics() + [_check_metrics_expr(["it's_valid"])]
    assert observer.metrics == expected


def test_check_metrics_replaced_on_second_set():
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_check_names(["first_check"])
    assert observer.metrics == _default_metrics() + [_check_metrics_expr(["first_check"])]

    observer.set_check_names(["second_check"])
    assert observer.metrics == _default_metrics() + [_check_metrics_expr(["second_check"])]


def test_check_metrics_repeated_use_different_checks():
    observer = DQMetricsObserver(track_extended_metrics=True)

    observer.set_check_names(["check_a", "check_b"])
    assert observer.metrics == _default_metrics() + [_check_metrics_expr(["check_a", "check_b"])]

    observer.set_check_names(["check_c"])
    assert observer.metrics == _default_metrics() + [_check_metrics_expr(["check_c"])]


def test_check_metrics_cleared_with_empty_names():
    observer = DQMetricsObserver(track_extended_metrics=True)
    observer.set_check_names(["my_check"])
    assert len(observer.metrics) == 5

    observer.set_check_names([])
    assert observer.metrics == _default_metrics()


def _default_metrics(err="_errors", warn="_warnings"):
    return [
        "count(1) as input_row_count",
        f"count(case when {err} is not null then 1 end) as error_row_count",
        f"count(case when {warn} is not null then 1 end) as warning_row_count",
        f"count(case when {err} is null and {warn} is null then 1 end) as valid_row_count",
    ]


def _check_metrics_expr(check_names, err="_errors", warn="_warnings"):
    elements = []
    for name in check_names:
        escaped = name.replace("'", "''")
        elements.append(
            f"named_struct("
            f"'check_name', '{escaped}', "
            f"'error_count', count(case when exists({err}, x -> x.name = '{escaped}') then 1 end), "
            f"'warning_count', count(case when exists({warn}, x -> x.name = '{escaped}') then 1 end)"
            f")"
        )
    return f"to_json(array({', '.join(elements)})) as check_metrics"
