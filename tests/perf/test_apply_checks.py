from databricks.labs.dqx.engine import DQEngine
from datetime import datetime
from databricks.labs.dqx.rule import ExtraParams, DQRowRule, DQDatasetRule
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
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
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
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
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
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col9"])
def test_benchmark_is_not_null_and_is_in_list(benchmark, ws, generated_df, column):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            name=f"{column}_is_not_null_and_is_in_list",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_is_in_list,
            check_func_kwargs={"allowed": [1, 2, 3, 5, 6, 7, 8, 9, 10]},
            column=column,
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col9"])
def test_benchmark_is_in_list(benchmark, ws, generated_df, column):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            name=f"{column}_is_in_list",
            criticality="warn",
            check_func=check_funcs.is_in_list,
            check_func_kwargs={"allowed": [1, 2, 3, 5, 6, 7, 8, 9, 10]},
            column=column,
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col5", "col6", "col9"])
def test_benchmark_sql_expression(benchmark, ws, generated_df, column):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            name=f"{column}_sql_expression",
            criticality="warn",
            check_func=check_funcs.sql_expression,
            check_func_kwargs={"expression": f"{column} not like \"val%\""},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_older_than_col2_for_n_days(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_older_than_col2_for_n_days,
            check_func_kwargs={"column1": "col5", "column2": "col5", "days": 1},
            user_metadata={"tag1": "value4"},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_older_than_n_days(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_older_than_n_days,
            column="col5",
            check_func_kwargs={"days": 1},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_not_in_future(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_in_future,
            column="col6",
            check_func_kwargs={"offset": 10000},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_not_in_near_future(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_in_near_future,
            column="col6",
            check_func_kwargs={"offset": 10000},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_not_less_than(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_less_than,
            column="col6",
            check_func_kwargs={"limit": datetime(2025, 1, 1, 1, 0, 0)},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_not_greater_than(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_greater_than,
            column="col5",
            check_func_kwargs={"limit": datetime(2025, 8, 19).date()},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_in_range(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_in_range,
            column="col1",
            check_func_kwargs={"min_limit": 5, "max_limit": 100_000},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_not_in_range(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_in_range,
            column="col2",
            check_func_kwargs={"min_limit": 1_000_000, "max_limit": 1_000_000},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_regex_match(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.regex_match,
            column="col2",
            check_func_kwargs={"regex": "[0-9]+", "negate": False},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_not_null_and_not_empty_array(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty_array,
            column="col4",
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_valid_date(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_date,
            column="col5",
            check_func_kwargs={"date_format": "yyyy-MM-dd"},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_valid_timestamp(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_timestamp,
            column="col6",
            check_func_kwargs={"timestamp_format": "yyyy-MM-dd HH:mm:ss"},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_valid_ipv4_address(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_ipv4_address,
            column="col9",
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_ipv4_address_in_cidr(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_ipv4_address_in_cidr,
            column="col9",
            check_func_kwargs={"cidr_block": "255.255.255.255/32"},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_data_fresh(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_data_fresh,
            column="col5",
            check_func_kwargs={"max_age_minutes": 1440},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


# Dataset checks


def test_benchmark_is_unique(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQDatasetRule(
            criticality="warn",
            filter="col2 > 1 or col2 is null",
            check_func=check_funcs.is_unique,
            columns=["col1", "col2"],
            check_func_kwargs={"nulls_distinct": False},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_foreign_key(benchmark, ws, generated_df, make_ref_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=["col1", "col2"],
            check_func_kwargs={
                "ref_columns": ["ref_col1", "ref_col2"],
                "ref_df_name": "ref_df",
            },
        ),
    ]
    refs_df = {"ref_df": make_ref_df}
    checked = benchmark(dq_engine.apply_checks, generated_df, checks, refs_df)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_sql_query(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    query = "SELECT col2, SUM(col1) > 1 AS condition FROM {{input_view}} GROUP BY col2"

    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.sql_query,
            check_func_kwargs={
                "query": query,
                "merge_columns": ["col2"],
                "condition_column": "condition",
                "negate": True,
            },
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_aggr_not_greater_than(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="col1",
            check_func_kwargs={"aggr_type": "count", "limit": 10},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_aggr_not_less_than(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_less_than,
            column="col2",
            check_func_kwargs={"aggr_type": "count", "limit": 1},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_aggr_equal(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_equal,
            column="col2",
            check_func_kwargs={"aggr_type": "avg", "limit": 10.0},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_aggr_not_equal(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_equal,
            column="col2",
            check_func_kwargs={"aggr_type": "avg", "limit": 10.0},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_compare_datasets(benchmark, ws, generated_df, make_ref_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.compare_datasets,
            columns=["col1", "col2"],
            check_func_kwargs={
                "ref_columns": ["ref_col1", "ref_col2"],
                "ref_df_name": "ref_df",
            },
        ),
    ]
    refs_df = {"ref_df": make_ref_df}

    checked = benchmark(dq_engine.apply_checks, generated_df, checks, refs_df)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_is_data_fresh_per_time_window(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_data_fresh_per_time_window,
            column="col6",
            check_func_kwargs={"window_minutes": 1, "min_records_per_window": 1, "lookback_windows": 3},
        ),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS
