from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from tests.integration.conftest import (
    EXTRA_PARAMS,
    RUN_TIME,
    RUN_ID,
)

SCHEMA = "a: int, b: int, c: int"


def test_apply_checks_suppress_skipped_suppresses_skipped_entries(ws, spark):
    extra_params = ExtraParams(run_time_overwrite=RUN_TIME.isoformat(), run_id_overwrite=RUN_ID, suppress_skipped=True)
    dq_engine = DQEngine(workspace_client=ws, extra_params=extra_params)
    test_df = spark.createDataFrame([[1, None, 3]], SCHEMA)

    checks = [
        DQRowRule(name="b_is_null", criticality="error", check_func=check_funcs.is_not_null, column="b"),
        DQRowRule(
            name="missing_col_is_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="missing_col",
        ),
    ]

    good_df, bad_df = dq_engine.apply_checks_and_split(test_df, checks)

    # row is in bad_df only because of the real failure (b is null), not the skipped check
    assert bad_df.count() == 1
    errors = bad_df.select("_errors").first()["_errors"]
    assert len(errors) == 1
    assert errors[0]["name"] == "b_is_null"

    # good_df is empty since the real check failed
    assert good_df.count() == 0


def test_apply_checks_skipped_flag_set_on_skipped_entries(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 2, 3]], SCHEMA)

    checks = [
        DQRowRule(
            name="missing_col_is_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="missing_col",
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    errors = checked.select("_errors").first()["_errors"]
    assert len(errors) == 1
    assert errors[0]["name"] == "missing_col_is_null"
    assert errors[0]["skipped"] is True
