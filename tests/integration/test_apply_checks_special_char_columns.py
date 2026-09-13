import pyspark.sql.functions as F
import yaml
from pyspark.sql import Column

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQDatasetRule, DQRowRule, register_rule
from tests.integration.conftest import EXTRA_PARAMS


@register_rule("row")
def custom_is_null_by_column_name(column: str) -> Column:
    """A custom check that is not registered for column name resolution and requires the column as a string."""
    return make_condition(F.col(column).isNull(), f"Column '{column}' value is null", f"{column}_custom_is_null")


def _error_names(row) -> list[str]:
    return sorted(error["name"] for error in row["_errors"] or [])


def test_apply_checks_by_metadata_with_unquoted_special_character_column_names(ws, spark):
    """Column names with spaces and special characters can be used without back-quoting (issue #1202)."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    schema = "id int, `Long Name` string, `Col with $pecial character` string"
    test_df = spark.createDataFrame([[1, "tt", "nnnn"], [2, None, None]], schema)

    checks = yaml.safe_load(
        """
        - criticality: warn
          check:
            function: has_valid_schema
            arguments:
              expected_schema: "id int, `Long Name` string, `Col with $pecial character` string"
              strict: true
        - criticality: error
          check:
            function: is_not_null
            arguments:
              column: Long Name
        - criticality: error
          check:
            function: is_not_null
            arguments:
              column: Col with $pecial character
        """
    )

    rows = {row["id"]: row for row in dq_engine.apply_checks_by_metadata(test_df, checks).collect()}

    assert rows[1]["_errors"] is None
    assert rows[1]["_warnings"] is None
    assert sorted(error["message"] for error in rows[2]["_errors"]) == [
        "Column 'Col with $pecial character' value is null",
        "Column 'Long Name' value is null",
    ]
    assert all(error["skipped"] is None for error in rows[2]["_errors"])
    assert rows[2]["_warnings"] is None


def test_apply_checks_with_special_character_column_names_using_classes(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame(
        [[1, "Alice", 10], [2, "Bob", 200]], "id int, `Long Name` string, `order.amount` int"
    )

    checks = [
        DQRowRule(
            name="name_is_allowed",
            criticality="error",
            check_func=check_funcs.is_in_list,
            column="Long Name",
            check_func_kwargs={"allowed": ["'Alice'"]},
        ),
        DQRowRule(
            name="amount_is_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="order.amount",
        ),
        DQDatasetRule(
            name="max_amount_is_not_greater_than_100",
            criticality="warn",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="order.amount",
            check_func_kwargs={"limit": 100, "aggr_type": "max"},
        ),
    ]

    rows = {row["id"]: row for row in dq_engine.apply_checks(test_df, checks).collect()}

    assert rows[1]["_errors"] is None
    assert _error_names(rows[2]) == ["name_is_allowed"]
    for row in rows.values():
        assert [warning["name"] for warning in row["_warnings"]] == ["max_amount_is_not_greater_than_100"]
        assert row["_warnings"][0]["skipped"] is None


def test_apply_checks_dataset_checks_with_special_character_column_names(ws, spark):
    """compare_datasets uses its columns as names, foreign_key is registered for column name resolution.

    Column name resolution applies to the column / columns arguments only, so the foreign_key ref_columns are
    back-quoted."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    schema = "id int, `Long Name` string, amount int"
    test_df = spark.createDataFrame([[1, "Alice", 10], [2, "Bob", 20]], schema)
    ref_df = spark.createDataFrame([[1, "Alice", 10], [2, "Bob", 99]], schema)

    checks = [
        DQDatasetRule(
            name="matches_reference",
            criticality="error",
            check_func=check_funcs.compare_datasets,
            columns=["Long Name"],
            check_func_kwargs={"ref_columns": ["Long Name"], "ref_df_name": "ref"},
        ),
        DQDatasetRule(
            name="name_exists_in_reference",
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=["Long Name"],
            check_func_kwargs={"ref_columns": ["`Long Name`"], "ref_df_name": "ref"},
        ),
    ]

    checked_df = dq_engine.apply_checks(test_df, checks, ref_dfs={"ref": ref_df})
    rows = {row["Long Name"]: row for row in checked_df.collect()}

    assert rows["Alice"]["_errors"] is None
    assert _error_names(rows["Bob"]) == ["matches_reference"]
    assert rows["Bob"]["_errors"][0]["skipped"] is None
    assert '"amount":{"df":"20","ref":"99"}' in rows["Bob"]["_errors"][0]["message"]
    assert all(row["_warnings"] is None for row in rows.values())


def test_apply_checks_custom_check_receives_special_character_column_name_as_defined(ws, spark):
    """Custom checks that are not registered for column name resolution keep receiving the column as a string."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, "tt"], [2, None]], "id int, `Long Name` string")

    checks = [
        DQRowRule(
            name="custom_long_name_is_null",
            criticality="error",
            check_func=custom_is_null_by_column_name,
            column="Long Name",
        ),
    ]

    rows = {row["id"]: row for row in dq_engine.apply_checks(test_df, checks).collect()}

    assert rows[1]["_errors"] is None
    assert [error["message"] for error in rows[2]["_errors"]] == ["Column 'Long Name' value is null"]


def test_apply_checks_prefers_exact_column_name_over_sql_expression(ws, spark):
    """The existing column named "Long Name" is checked, although Spark SQL also parses it as column "Long" aliased
    to "Name"."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, "not null", None]], "id int, Long string, `Long Name` string")

    checks = [
        DQRowRule(
            name="long_name_is_not_null", criticality="error", check_func=check_funcs.is_not_null, column="Long Name"
        ),
    ]

    row = dq_engine.apply_checks(test_df, checks).collect()[0]

    assert [error["message"] for error in row["_errors"]] == ["Column 'Long Name' value is null"]
