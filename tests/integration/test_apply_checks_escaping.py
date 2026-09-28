"""Integration tests for applying checks to columns whose names require SQL identifier escaping.

Split out of test_apply_checks.py to keep that module under pylint's max-module-lines limit.
"""

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs
from tests.integration.conftest import (
    REPORTING_COLUMNS,
    RUN_TIME,
    EXTRA_PARAMS,
    RUN_ID,
    assert_df_equality_ignore_fingerprints as assert_df_equality,
)


def test_apply_checks_by_metadata_on_column_requiring_escaping_runs(ws, spark):
    """Ensures row-level checks execute on a column whose name requires SQL identifier escaping for checks defined using YAML."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    schema = "id int, `Päivämäärä` int, `Customer Name` string"
    test_df = spark.createDataFrame([[1, None, None], [2, 20, "test_customer"]], schema)

    checks = [
        {
            "name": "Päivämäärä_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "Päivämäärä"}},
        },
        {
            "name": "customer_name_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "Customer Name"}},
        },
    ]
    actual = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [
                1,
                None,
                None,
                [
                    {
                        "name": "Päivämäärä_not_null",
                        "message": "Column 'Päivämäärä' value is null",
                        "columns": ["Päivämäärä"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                    {
                        "name": "customer_name_not_null",
                        "message": "Column 'Customer Name' value is null",
                        "columns": ["Customer Name"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [2, 20, "test_customer", None, None],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(actual.sort("id"), expected.sort("id"), ignore_nullable=True)


def test_apply_checks_on_column_requiring_escaping_runs(ws, spark):
    """Ensures row-level checks execute on a column whose name requires SQL identifier escaping for checks defined using DQX classes."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    schema = "id int, `Päivämäärä` int, `Customer Name` string"
    test_df = spark.createDataFrame([[1, None, None], [2, 20, "test_customer"]], schema)

    checks = [
        DQRowRule(
            name="Päivämäärä_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="Päivämäärä",
        ),
        DQRowRule(
            name="customer_name_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="Customer Name",
        ),
    ]
    actual = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [
                1,
                None,
                None,
                [
                    {
                        "name": "Päivämäärä_not_null",
                        "message": "Column 'Päivämäärä' value is null",
                        "columns": ["Päivämäärä"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                    {
                        "name": "customer_name_not_null",
                        "message": "Column 'Customer Name' value is null",
                        "columns": ["Customer Name"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [2, 20, "test_customer", None, None],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(actual.sort("id"), expected.sort("id"), ignore_nullable=True)


def test_apply_checks_on_unresolvable_column_is_skipped_and_run_completes(ws, spark):
    """A check on a column that cannot be resolved (even after escaping) is skipped rather than aborting the
    run, and other checks in the same set still evaluate. This is the behaviour #1481 regressed: validation
    and execution now agree, so an unresolvable name is reported as skipped instead of crashing later."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    schema = "id int, `Customer Name` string"
    test_df = spark.createDataFrame([[1, None], [2, "test_customer"]], schema)

    checks = [
        {
            "name": "missing_col_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "Does Not Exist"}},
        },
        {
            "name": "customer_name_not_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "Customer Name"}},
        },
    ]
    checked = dq_engine.apply_checks_by_metadata(test_df, checks)
    errors_by_row = {row["id"]: row["_errors"] for row in checked.select("id", "_errors").collect()}

    # The unresolvable column can't be evaluated, so it's reported as skipped on every row rather than
    # aborting the run.
    for row_id in (1, 2):
        skipped = [e for e in (errors_by_row[row_id] or []) if e["name"] == "missing_col_not_null"]
        assert len(skipped) == 1, f"row {row_id}: {errors_by_row[row_id]}"
        assert skipped[0]["skipped"] is True

    # The escaping-required column still runs end-to-end: it flags the null for row id=1 and passes id=2.
    resolved_row1 = [e for e in (errors_by_row[1] or []) if e["name"] == "customer_name_not_null"]
    assert len(resolved_row1) == 1
    assert resolved_row1[0]["message"] == "Column 'Customer Name' value is null"
    resolved_row2 = [e for e in (errors_by_row[2] or []) if e["name"] == "customer_name_not_null"]
    assert len(resolved_row2) == 0
