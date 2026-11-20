"""Integration tests for validating check SQL expressions (filters, custom messages, *sql_expression*) against
the input DataFrame before the checks are applied.

Split out of test_apply_checks.py to keep that module under pylint's max-module-lines limit.
"""

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs
from tests.integration.conftest import EXTRA_PARAMS


def test_apply_checks_filter_cannot_resolve_against_another_check_expression(ws, spark):
    """Check expressions are validated together in a single schema analysis. A filter referring to a missing
    column must still be skipped as invalid, even when that column name could resolve against another expression
    of the same analysis (lateral column alias)."""
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None], [2, "ok"]], "id int, b string")

    checks = [
        DQRowRule(
            name="b_is_null_valid_filter",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="b",
            filter="id > 0",
        ),
        DQRowRule(
            name="b_is_null_missing_column_filter",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="b",
            filter="expr_0 IS NOT NULL",
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)
    errors_by_row = {row["id"]: row["_errors"] for row in checked.select("id", "_errors").collect()}

    for row_id in (1, 2):
        skipped = [e for e in (errors_by_row[row_id] or []) if e["name"] == "b_is_null_missing_column_filter"]
        assert len(skipped) == 1, f"row {row_id}: {errors_by_row[row_id]}"
        assert skipped[0]["skipped"] is True
        assert skipped[0]["message"] == "Check evaluation skipped due to invalid check filter: 'expr_0 IS NOT NULL'"

    valid_row1 = [e for e in (errors_by_row[1] or []) if e["name"] == "b_is_null_valid_filter"]
    assert len(valid_row1) == 1
    assert valid_row1[0]["skipped"] is None
    assert not [e for e in (errors_by_row[2] or []) if e["name"] == "b_is_null_valid_filter"]
