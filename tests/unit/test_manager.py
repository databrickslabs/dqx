import re
from pathlib import Path
from unittest.mock import create_autospec, PropertyMock

import pytest
import yaml
import pyspark.sql.functions as F
from pyspark.errors import AnalysisException, PySparkRuntimeError
from pyspark.sql import Column, DataFrame, SparkSession

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.checks_serializer import deserialize_checks
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.executor import DQCheckResult
from databricks.labs.dqx.manager import DQRuleManager
from databricks.labs.dqx.rule import (
    CHECK_FUNC_COLUMN_NAME_RESOLUTION_ATTRIBUTE,
    DQDatasetRule,
    DQRowRule,
    register_rule,
)
from databricks.labs.dqx.utils import get_column_name_or_alias


def _make_manager_with_missing_column(df: DataFrame, spark: SparkSession, *, suppress_skipped: bool) -> DQRuleManager:
    return DQRuleManager(
        check=DQRowRule(check_func=check_funcs.is_not_null, column="missing_col"),
        df=df,
        spark=spark,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=suppress_skipped,
    )


def test_rule_manager_suppress_skipped_returns_null_condition_for_invalid_column():
    """When suppress_skipped=True and a column cannot be resolved, process() returns a null-cast Column
    so the check produces no entry in _errors/_warnings."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    type(df_mock.select.return_value).schema = PropertyMock(
        side_effect=AnalysisException("Column 'missing_col' not found")
    )

    manager = _make_manager_with_missing_column(df_mock, spark_mock, suppress_skipped=True)
    result = manager.process()

    assert isinstance(result, DQCheckResult)
    assert result.condition is not None
    assert "CAST(NULL AS STRUCT" in str(result.condition)
    assert result.check_df is df_mock


def test_rule_manager_suppress_skipped_false_returns_struct_for_invalid_column():
    """When suppress_skipped=False and a column cannot be resolved, process() returns a result struct
    (not null) so the skipped check is recorded in _errors/_warnings."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    type(df_mock.select.return_value).schema = PropertyMock(
        side_effect=AnalysisException("Column 'missing_col' not found")
    )

    manager = _make_manager_with_missing_column(df_mock, spark_mock, suppress_skipped=False)
    result = manager.process()

    assert isinstance(result, DQCheckResult)
    assert result.condition is not None
    assert "skipped" in str(result.condition).lower()
    assert result.check_df is df_mock


def _make_manager_with_filter(df: DataFrame, spark: SparkSession, check: DQRowRule | DQDatasetRule) -> DQRuleManager:
    return DQRuleManager(
        check=check,
        df=df,
        spark=spark,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=False,
    )


def test_rule_manager_skips_check_with_destructive_filter():
    """A check whose filter contains a destructive statement (e.g. DROP) is skipped (not evaluated) rather
    than aborting the whole rule set; the skip message identifies the unsafe filter."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = _make_manager_with_filter(
        df_mock,
        spark_mock,
        DQRowRule(check_func=check_funcs.is_not_null, column="col1", filter="id = 1 OR DROP TABLE users"),
    )

    assert manager.has_unsafe_filter is True
    # process() returns a skipped result struct instead of raising or running the executor
    result = manager.process()
    assert isinstance(result, DQCheckResult)
    condition_str = str(result.condition)
    assert "skipped" in condition_str.lower()
    assert "unsafe check filter" in condition_str


def test_rule_manager_suppress_skipped_removes_check_with_destructive_filter():
    """With suppress_skipped=True, a check with an unsafe filter produces no _errors/_warnings entry."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = DQRuleManager(
        check=DQRowRule(check_func=check_funcs.is_not_null, column="col1", filter="id = 1 OR DROP TABLE users"),
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=True,
    )
    result = manager.process()
    assert "CAST(NULL AS STRUCT" in str(result.condition)


def test_rule_manager_allows_safe_filter():
    """A normal predicate filter passes the guard (does not raise)."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = DQRuleManager(
        check=DQRowRule(
            check_func=check_funcs.is_not_null,
            column="col1",
            filter="country = 'US'",
        ),
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=False,
    )
    # accessing filter_condition compiles the filter; a safe one must not raise
    assert manager.filter_condition is not None


def test_rule_manager_allows_select_filter():
    """A filter with a SELECT subquery is allowed (referential filters are a valid feature)."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = DQRuleManager(
        check=DQRowRule(
            check_func=check_funcs.is_not_null,
            column="email",
            filter="customer_id IN (SELECT customer_id FROM main.ref.active_customers)",
        ),
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
        suppress_skipped=False,
    )
    # a SELECT/subquery filter must not raise — compiling filter_condition is enough to prove it
    assert manager.filter_condition is not None


def test_rule_manager_skips_dataset_check_with_destructive_filter():
    """A dataset check's filter is pushed down to row_filter; a destructive one causes the check to be
    skipped (not evaluated) rather than aborting the whole rule set."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = _make_manager_with_filter(
        df_mock,
        spark_mock,
        DQDatasetRule(check_func=check_funcs.is_unique, columns=["col1"], filter="id = 1 OR DROP TABLE users"),
    )

    assert manager.has_unsafe_filter is True
    result = manager.process()
    assert "skipped" in str(result.condition).lower()


def test_rule_manager_skips_check_with_destructive_row_filter_in_kwargs():
    """An unsafe filter supplied directly as a row_filter kwarg (not the check-level filter) is also skipped."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    manager = _make_manager_with_filter(
        df_mock,
        spark_mock,
        DQDatasetRule(
            check_func=check_funcs.is_unique,
            columns=["col1"],
            check_func_kwargs={"row_filter": "id = 1 OR DROP TABLE users"},
        ),
    )

    assert manager.has_unsafe_filter is True
    result = manager.process()
    assert "skipped" in str(result.condition).lower()


def test_rule_manager_skips_check_with_destructive_row_filter_positional():
    """An unsafe row_filter supplied positionally via check_func_args is also detected and skipped, so it
    cannot slip past the manager gate and hard-raise in the executor."""
    df_mock = create_autospec(DataFrame)
    spark_mock = create_autospec(SparkSession)
    # has_no_outliers(column, row_filter=None): pass row_filter positionally through check_func_args
    manager = _make_manager_with_filter(
        df_mock,
        spark_mock,
        DQDatasetRule(
            check_func=check_funcs.has_no_outliers,
            column="col1",
            check_func_args=["id = 1 OR DROP TABLE users"],
        ),
    )

    assert manager.has_unsafe_filter is True
    result = manager.process()
    assert "skipped" in str(result.condition).lower()


@register_rule("row")
def custom_check_using_column_name(column: str) -> Column:
    """A custom check that is not registered for column name resolution and requires the column as a string."""
    return check_funcs.make_condition(F.col(column).isNull(), f"{column} is null", f"{column}_custom_is_null")


def _make_manager_for_columns(check: DQRowRule | DQDatasetRule, df_columns: list[str]) -> DQRuleManager:
    df_mock = create_autospec(DataFrame)
    df_mock.columns = df_columns
    spark_mock = create_autospec(SparkSession)
    return DQRuleManager(
        check=check,
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
    )


@pytest.mark.parametrize("column_name", ["Long Name", "Col with $pecial character", "Ääkkönen", "order-id"])
def test_rule_manager_passes_column_name_requiring_escaping_as_column_reference(column_name):
    """A column name that Spark SQL cannot parse as a single column is passed to the check as a column reference,
    so it is not parsed as SQL (e.g. "Long Name" as column "Long" aliased to "Name")."""
    check = DQRowRule(check_func=check_funcs.is_not_null, column=column_name)
    manager = _make_manager_for_columns(check, ["id", column_name])

    resolved_column = manager.resolved_check.column

    assert not isinstance(resolved_column, str)
    assert get_column_name_or_alias(resolved_column) == column_name
    assert manager.invalid_columns == []
    assert manager.check.column == column_name  # reported check details keep the column as defined


def test_rule_manager_back_quotes_column_name_with_dot():
    """A dot in a column name would otherwise be read as a nested field access."""
    check = DQRowRule(check_func=check_funcs.is_not_null, column="order.amount")
    manager = _make_manager_for_columns(check, ["id", "order.amount"])

    assert get_column_name_or_alias(manager.resolved_check.column) == "`order.amount`"


def test_rule_manager_keeps_simple_column_name_unchanged():
    check = DQRowRule(check_func=check_funcs.is_not_null, column="col1")
    manager = _make_manager_for_columns(check, ["col1", "col2"])

    assert manager.resolved_check is check


def test_rule_manager_keeps_sql_expression_unchanged():
    check = DQRowRule(check_func=check_funcs.is_not_null, column="col1 + col2")
    manager = _make_manager_for_columns(check, ["col1", "col2"])

    assert manager.resolved_check is check
    assert manager.invalid_columns == []


def test_rule_manager_prefers_exact_column_name_over_sql_expression():
    """The existing column named "Long Name" is checked, although Spark SQL also parses it as column "Long" aliased
    to "Name"."""
    check = DQRowRule(check_func=check_funcs.is_not_null, column="Long Name")
    manager = _make_manager_for_columns(check, ["Long", "Long Name"])

    resolved_column = manager.resolved_check.column

    assert not isinstance(resolved_column, str)
    assert get_column_name_or_alias(resolved_column) == "Long Name"


def test_rule_manager_resolves_each_column_in_columns_list():
    check = DQDatasetRule(
        check_func=check_funcs.foreign_key,
        columns=["Long Name", "id"],
        check_func_kwargs={"ref_columns": ["Long Name", "id"], "ref_df_name": "ref"},
    )
    manager = _make_manager_for_columns(check, ["id", "Long Name"])

    resolved_columns = manager.resolved_check.columns

    assert resolved_columns is not None
    assert not isinstance(resolved_columns[0], str)
    assert get_column_name_or_alias(resolved_columns[0]) == "Long Name"
    assert resolved_columns[1] == "id"


def test_rule_manager_keeps_columns_for_custom_check_not_registered_for_column_name_resolution():
    """Custom checks receive the columns exactly as defined, so a check requiring a string keeps working."""
    check = DQRowRule(check_func=custom_check_using_column_name, column="Long Name")
    manager = _make_manager_for_columns(check, ["id", "Long Name"])

    assert manager.resolved_check is check
    assert isinstance(manager.process(), DQCheckResult)


def test_rule_manager_keeps_columns_for_compare_datasets():
    """compare_datasets uses its columns as names and rejects column references to names with spaces."""
    check = DQDatasetRule(
        check_func=check_funcs.compare_datasets,
        columns=["Long Name"],
        check_func_kwargs={"ref_columns": ["Long Name"], "ref_df_name": "ref"},
    )
    manager = _make_manager_for_columns(check, ["id", "Long Name"])

    assert manager.resolved_check is check


def test_rule_manager_skips_missing_column_name_requiring_escaping():
    df_mock = create_autospec(DataFrame)
    df_mock.columns = ["id"]
    type(df_mock.select.return_value).schema = PropertyMock(
        side_effect=AnalysisException("Column 'Missing Column' not found")
    )
    spark_mock = create_autospec(SparkSession)
    check = DQRowRule(check_func=check_funcs.is_not_null, column="Missing Column")
    manager = DQRuleManager(
        check=check,
        df=df_mock,
        spark=spark_mock,
        engine_user_metadata={},
        run_time_overwrite=None,
        run_id="test-run",
    )

    assert manager.invalid_columns == ["`Missing Column`"]
    assert "skipped" in str(manager.process().condition).lower()


# Resources defining every built-in check, used to verify column name resolution for all checks
ALL_CHECKS_RESOURCES = [
    "all_row_checks.yaml",
    "all_dataset_checks.yaml",
    "all_row_geo_checks.yaml",
    "all_dateset_geo_checks.yaml",
]
# Plain column names (e.g. "col1") in the resources are renamed to require escaping; SQL expressions are kept
PLAIN_COLUMN_NAME = re.compile(r"[A-Za-z_]\w*")


def _all_checks_metadata() -> list:
    resources = Path(__file__).parent.parent / "resources"
    params = []
    for resource in ALL_CHECKS_RESOURCES:
        checks = yaml.safe_load((resources / resource).read_text(encoding="utf-8"))
        for index, check in enumerate(checks):
            params.append(pytest.param(check, id=f"{resource}-{index}-{check['check']['function']}"))
    return params


def _rename_plain_column_names(value: object, renamed: set[str]) -> object:
    """Adds a space to plain column names, leaving SQL expressions, '*' and non-string values unchanged."""
    if isinstance(value, str) and PLAIN_COLUMN_NAME.fullmatch(value):
        renamed.add(f"{value} x")
        return f"{value} x"
    if isinstance(value, list):
        return [_rename_plain_column_names(item, renamed) for item in value]
    return value


def _rename_check_column_names(check_metadata: dict) -> tuple[dict, set[str]]:
    """Returns the check metadata with plain names in column, columns and for_each_column renamed to contain a space,
    together with the renamed column names."""
    renamed: set[str] = set()
    check = dict(check_metadata["check"])
    arguments = dict(check.get("arguments") or {})
    for argument in ("column", "columns"):
        if argument in arguments:
            arguments[argument] = _rename_plain_column_names(arguments[argument], renamed)
    check["arguments"] = arguments
    if "for_each_column" in check:
        check["for_each_column"] = _rename_plain_column_names(check["for_each_column"], renamed)
    return {**check_metadata, "check": check}, renamed


def _check_columns(rule: DQRowRule | DQDatasetRule) -> list:
    return [rule.column] if rule.column is not None else list(rule.columns or [])


@pytest.mark.parametrize("check_metadata", _all_checks_metadata())
def test_rule_manager_column_name_resolution_supports_all_checks(check_metadata):
    """Every built-in check either accepts column references for names requiring escaping (when registered for
    column name resolution), or receives its columns exactly as defined."""
    renamed_check_metadata, renamed = _rename_check_column_names(check_metadata)
    try:
        rules = deserialize_checks([renamed_check_metadata])
    except (PySparkRuntimeError, InvalidParameterError) as e:
        if "NO_ACTIVE_OR_DEFAULT_SESSION" not in str(e):
            raise
        pytest.skip("building this check requires a Spark session, covered by integration tests")

    for rule in rules:
        resolved_check = _make_manager_for_columns(rule, sorted(renamed)).resolved_check

        if not getattr(rule.check_func, CHECK_FUNC_COLUMN_NAME_RESOLUTION_ATTRIBUTE, False):
            assert resolved_check is rule
            continue

        renamed_columns = [
            (column, resolved_column)
            for column, resolved_column in zip(_check_columns(rule), _check_columns(resolved_check))
            if isinstance(column, str) and column in renamed
        ]
        assert all(
            not isinstance(resolved_column, str) and get_column_name_or_alias(resolved_column) == column
            for column, resolved_column in renamed_columns
        )
