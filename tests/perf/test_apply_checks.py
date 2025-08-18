from databricks.labs.dqx.engine import DQEngine
from datetime import datetime
from databricks.labs.dqx.rule import ExtraParams, DQRowRule
import pytest
from databricks.labs.dqx import check_funcs
from tests.perf.conftest import ROWS

RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0)
EXTRA_PARAMS = ExtraParams(run_time=RUN_TIME)
EXPECTED_ROWS = ROWS


def test_benchmark_apply_checks_all_row_checks(benchmark, ws, all_row_checks, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checked_df = benchmark(dq_engine.apply_checks_by_metadata, generated_df, all_row_checks)
    actual_count = checked_df.count()
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"])
def test_benchmark_is_null_or_empty(benchmark, ws, generated_df, column):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            name=f"{column}_is_null_or_empty",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column=column,
        ),
    ]
    checked = benchmark(dq_engine.apply_checks, generated_df, checks)
    actual_count = checked.count()
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"])
def test_benchmark_is_not_empty(benchmark, ws, generated_df, column):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            name=f"{column}_is_not_empty",
            criticality="warn",
            check_func=check_funcs.is_not_empty,
            column=column,
        ),
    ]
    checked = benchmark(dq_engine.apply_checks, generated_df, checks)
    actual_count = checked.count()
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"])
def test_benchmark_is_not_null(benchmark, ws, generated_df, column):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            name=f"{column}_is_not_null",
            criticality="warn",
            check_func=check_funcs.is_not_null,
            column=column,
        ),
    ]
    checked = benchmark(dq_engine.apply_checks, generated_df, checks)
    actual_count = checked.count()
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"])
def test_benchmark_is_not_null_and_is_in_list(benchmark, ws, generated_df, column):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            name=f"{column}_is_not_null_and_is_in_list",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_is_in_list,
            column=column,
        ),
    ]
    checked = benchmark(dq_engine.apply_checks, generated_df, checks)
    actual_count = checked.count()
    assert actual_count == EXPECTED_ROWS
