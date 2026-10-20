"""Unit tests for DQMetricsObserver class."""

from unittest.mock import create_autospec

from pyspark.sql import DataFrame, Observation
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.connect.observation import Observation as SparkConnectObservation
from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule
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
    checks = [_rule("id_is_not_null")]
    metrics = observer.get_metrics(checks)
    expected = _default_metrics() + [_check_metrics_expr(checks)]
    assert metrics == expected


def test_get_metrics_with_checks_multiple():
    checks = [_rule("id_is_not_null"), _rule("name_is_not_empty", column="name"), _rule("age_in_range", column="age")]
    observer = DQMetricsObserver()
    metrics = observer.get_metrics(checks)
    expected = _default_metrics() + [_check_metrics_expr(checks)]
    assert metrics == expected


def test_get_metrics_with_checks_ordering_with_custom():
    custom = ["avg(age) as avg_age"]
    checks = [_rule("my_check")]
    observer = DQMetricsObserver(custom_metrics=custom)
    metrics = observer.get_metrics(checks)
    expected = _default_metrics() + [_check_metrics_expr(checks)] + custom
    assert metrics == expected


def test_get_metrics_with_checks_uses_custom_column_names():
    checks = [_rule("my_check")]
    observer = DQMetricsObserver()
    observer.set_column_names(error_column_name="dq_errors", warning_column_name="dq_warnings")
    metrics = observer.get_metrics(checks)
    expected = _default_metrics("dq_errors", "dq_warnings") + [_check_metrics_expr(checks, "dq_errors", "dq_warnings")]
    assert metrics == expected
    assert "exists(dq_errors, x -> x.rule_fingerprint" in metrics[-1]
    assert "exists(dq_warnings, x -> x.rule_fingerprint" in metrics[-1]


def test_get_metrics_with_checks_escapes_single_quotes():
    checks = [_rule("it's_valid")]
    observer = DQMetricsObserver()
    metrics = observer.get_metrics(checks)
    expected = _default_metrics() + [_check_metrics_expr(checks)]
    assert metrics == expected


def test_check_metrics_expr_escapes_single_quotes_with_backslash():
    """Single quotes must be escaped as \\' — ANSI '' doubling is not honoured by Spark.

    Spark's parser runs with *spark.sql.parser.escapedStringLiterals* false, where a doubled ''
    pair is dropped outright rather than unescaped: a check named "it's_valid" was reported as
    "its_valid". The round-trip is covered by an integration test; this pins the emitted SQL.

    ``test_get_metrics_with_checks_escapes_single_quotes`` above cannot catch this: it builds its
    expectation from the same production helper, so it holds whatever the escaping does.
    """
    expr = DQMetricsObserver().get_metrics([_rule("it's_valid")])[-1]

    assert "it\\'s_valid" in expr
    assert "it''s_valid" not in expr
    assert "''" not in expr


def test_check_metrics_expr_escapes_backslashes():
    """Backslashes must be doubled, or the parser consumes them.

    Without this, the backslash JSON-encoding adds for an embedded double quote is eaten and the
    emitted value is malformed JSON (``{"check_name":"he said "hi""}``).
    """
    expr = DQMetricsObserver().get_metrics([_rule('he said "hi"')])[-1]

    assert '\\\\"hi\\\\"' in expr


def test_check_metrics_expr_includes_rule_fingerprint():
    check = _rule("id_is_not_null")
    expr = DQMetricsObserver().get_metrics([check])[-1]

    assert f'"rule_fingerprint":"{check.rule_fingerprint}"' in expr
    assert expr.count(f"x.rule_fingerprint = '{check.rule_fingerprint}'") == 2


def test_check_metrics_expr_distinguishes_duplicate_check_names():
    error_check = _rule("dup_check")
    warn_check = _rule("dup_check", criticality="warn")
    expr = DQMetricsObserver().get_metrics([error_check, warn_check])[-1]

    assert error_check.rule_fingerprint != warn_check.rule_fingerprint
    assert expr.count('"check_name":"dup_check"') == 2
    assert expr.count(f"x.rule_fingerprint = '{error_check.rule_fingerprint}'") == 2
    assert expr.count(f"x.rule_fingerprint = '{warn_check.rule_fingerprint}'") == 2


def test_check_metrics_expr_checked_count_without_filter():
    expr = DQMetricsObserver().get_metrics([_rule("id_is_not_null")])[-1]

    assert "',\"checked_count\":', cast(count(1) as string)" in expr


def test_check_metrics_expr_checked_count_with_filter():
    expr = DQMetricsObserver().get_metrics([_rule("id_is_not_null", filter="age > 30")])[-1]

    assert "cast(count(case when (\nage > 30\n) then 1 end) as string)" in expr


def test_check_metrics_expr_checked_count_with_dataset_rule_filter():
    check = DQDatasetRule(check_func=check_funcs.is_unique, columns=["id"], filter="age > 30")
    expr = DQMetricsObserver().get_metrics([check])[-1]

    assert '"check_name":"id_is_not_unique"' in expr
    assert "cast(count(case when (\nage > 30\n) then 1 end) as string)" in expr


def test_check_metrics_expr_checked_count_with_unsafe_filter():
    expr = DQMetricsObserver().get_metrics([_rule("id_is_not_null", filter="1 = 1; DROP TABLE t")])[-1]

    assert "cast(0 as string)" in expr
    assert "DROP" not in expr


def test_check_metrics_expr_checked_count_for_skipped_check():
    check = _rule("id_is_not_null", filter="age > 30")
    expr = DQMetricsObserver().get_metrics([check], skipped_checks={check.rule_fingerprint})[-1]

    assert "cast(0 as string)" in expr
    assert "age > 30" not in expr


def test_check_metrics_expr_checked_count_with_row_filter_kwarg():
    check = DQDatasetRule(
        check_func=check_funcs.is_unique, columns=["id"], check_func_kwargs={"row_filter": "age > 30"}
    )
    expr = DQMetricsObserver().get_metrics([check])[-1]

    assert "cast(count(case when (\nage > 30\n) then 1 end) as string)" in expr


def test_check_metrics_expr_checked_count_with_positional_row_filter():
    check = DQDatasetRule(check_func=check_funcs.is_unique, columns=["id"], check_func_args=[True, "age > 30"])
    expr = DQMetricsObserver().get_metrics([check])[-1]

    assert "cast(count(case when (\nage > 30\n) then 1 end) as string)" in expr


def test_check_metrics_expr_matches_on_rule_fingerprint_when_present_in_results():
    check = _rule("id_is_not_null")
    expr = DQMetricsObserver().get_metrics([check], _df_with_result_fields(["name", "rule_fingerprint"]))[-1]

    assert expr.count(f"x.rule_fingerprint = '{check.rule_fingerprint}'") == 2
    assert "x.name" not in expr


def test_check_metrics_expr_matches_on_name_when_results_lack_rule_fingerprint():
    check = _rule("id_is_not_null")
    expr = DQMetricsObserver().get_metrics([check], _df_with_result_fields(["name"]))[-1]

    assert expr.count("x.name = 'id_is_not_null'") == 2
    assert "x.rule_fingerprint" not in expr
    assert f'"rule_fingerprint":"{check.rule_fingerprint}"' in expr


def test_get_metrics_with_check_names_produces_name_only_entries():
    expr = DQMetricsObserver().get_metrics(["id_is_not_null"])[-1]

    assert '{"check_name":"id_is_not_null","error_count":' in expr
    assert expr.count("x.name = 'id_is_not_null'") == 2
    assert "rule_fingerprint" not in expr
    assert "checked_count" not in expr


def test_check_metrics_expr_includes_user_metadata_when_set():
    check = _rule("id_is_not_null", user_metadata={"owner": "data-eng", "confidence": 0.95, "flag": True, "n": None})
    expr = DQMetricsObserver().get_metrics([check])[-1]

    assert (
        "',\"user_metadata\":', to_json(map("
        "cast('owner' as string), cast('data-eng' as string), "
        "cast('confidence' as string), cast(cast('0.95' as double) as string), "
        "cast('flag' as string), cast(true as string), "
        "cast('n' as string), cast(null as string))), '}')"
    ) in expr


def test_check_metrics_expr_escapes_user_metadata_literals():
    check = _rule("id_is_not_null", user_metadata={"it's \"k\"": "back\\slash\nnew\tline"})
    expr = DQMetricsObserver().get_metrics([check])[-1]

    assert "cast('it\\'s \"k\"' as string), cast('back\\\\slash\nnew\tline' as string)" in expr


def test_check_metrics_expr_omits_user_metadata_when_not_set():
    expr = DQMetricsObserver().get_metrics([_rule("id_is_not_null")])[-1]

    assert "user_metadata" not in expr


def test_get_metrics_with_checks_empty_list():
    observer = DQMetricsObserver()
    metrics = observer.get_metrics([])
    assert metrics == _default_metrics()


def test_get_metrics_idempotent():
    """Verifies that repeated calls with the same args return equal results."""
    observer = DQMetricsObserver()
    checks = [_rule("a")]
    assert observer.get_metrics() == observer.get_metrics()
    assert observer.get_metrics(checks) == observer.get_metrics(checks)


def _df_with_result_fields(field_names):
    entry = StructType([StructField(name, StringType()) for name in field_names])
    df = create_autospec(DataFrame)
    df.schema = StructType([StructField(column, ArrayType(entry)) for column in ("_errors", "_warnings")])
    return df


def _rule(name, column="id", **kwargs):
    return DQRowRule(name=name, check_func=check_funcs.is_not_null, column=column, **kwargs)


def _default_metrics(err="_errors", warn="_warnings"):
    return [
        "count(1) as input_row_count",
        f"count(case when {err} is not null then 1 end) as error_row_count",
        f"count(case when {warn} is not null then 1 end) as warning_row_count",
        f"count(case when {err} is null and {warn} is null then 1 end) as valid_row_count",
    ]


def _check_metrics_expr(checks, err="_errors", warn="_warnings"):
    # Note: this helper derives expected values from production code, so it validates structural
    # properties (ordering, column-name propagation) but not SQL correctness. The integration
    # tests in test_summary_metrics.py run the generated SQL against real Spark to cover that.
    observer = DQMetricsObserver()
    observer.set_column_names(error_column_name=err, warning_column_name=warn)
    return observer.get_metrics(checks)[len(_default_metrics()) :][-1]
