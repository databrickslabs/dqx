from databricks.labs.dqx.engine import DQEngine
from datetime import datetime, timezone
from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule, DQForEachColRule
from databricks.labs.dqx.config import ExtraParams
import pytest
from databricks.labs.dqx import check_funcs
from tests.perf.conftest import DEFAULT_ROWS

RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
EXTRA_PARAMS = ExtraParams(run_time=RUN_TIME.isoformat())
EXPECTED_ROWS = DEFAULT_ROWS


def test_benchmark_apply_checks_all_row_checks(benchmark, ws, all_row_checks, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checked_df = dq_engine.apply_checks_by_metadata(generated_df, all_row_checks)
    actual_count = benchmark(lambda: checked_df.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_apply_checks_all_dataset_checks(benchmark, ws, all_dataset_checks, generated_df, make_ref_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    refs_df = {"ref_df_key": make_ref_df}
    checked_df = dq_engine.apply_checks_by_metadata(generated_df, all_dataset_checks, ref_dfs=refs_df)
    actual_count = benchmark(lambda: checked_df.count())
    assert actual_count == EXPECTED_ROWS

def test_benchmark_row_check_foreach_col(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            columns=["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"],
            check_func=check_funcs.is_not_null,
            criticality="error",
        ).get_rules(),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


def test_benchmark_dataset_check_foreach_col(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    checks = [
        *DQForEachColRule(
            check_func=check_funcs.is_unique,
            columns=[["col1"], ["col2"], ["col1", "col2"]],
            filter="col2 > 1 or col2 is null",
            criticality="error",
            check_func_kwargs={"nulls_distinct": False},
        ).get_rules(),
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"])
@pytest.mark.benchmark(group="test_benchmark_is_null_or_empty")
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
    benchmark.group += f" {column}"
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize(
    "generated_string_df",
    [
        {"n_rows": DEFAULT_ROWS, "n_columns": 5}
    ],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_null_or_empty")
def test_benchmark_foreach_is_null_or_empty(benchmark, ws, generated_string_df):
    columns, df, n_rows = (
        generated_string_df
    )
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            check_func=check_funcs.is_not_null_and_not_empty,
            columns=columns,
            criticality="error",
        ).get_rules()
    ]

    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"])
@pytest.mark.benchmark(group="test_benchmark_is_not_empty")
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
    benchmark.group += f" {column}"
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize(
    "generated_string_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_not_empty")
def test_benchmark_foreach_is_not_empty(benchmark, ws, generated_string_df):
    columns, df, n_rows = generated_string_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            check_func=check_funcs.is_not_empty,
            columns=columns,
            criticality="error",
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS

@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9"])
@pytest.mark.benchmark(group="test_benchmark_is_not_null")
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
    benchmark.group += f" {column}"
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize(
    "generated_string_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5, "percentNulls": 0.20}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_not_null")
def test_benchmark_foreach_is_not_null(benchmark, ws, generated_string_df):
    columns, df, n_rows = generated_string_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            check_func=check_funcs.is_not_null,
            columns=columns,
            criticality="error",
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col9"])
@pytest.mark.benchmark(group="test_benchmark_is_not_null_and_is_in_list")
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
    benchmark.group += f" {column}"
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize(
    "generated_integer_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_not_null_and_is_in_list")
def test_benchmark_foreach_is_not_null_and_is_in_list(benchmark, ws, generated_integer_df):
    columns, df, n_rows = generated_integer_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            check_func=check_funcs.is_not_null_and_is_in_list,
            columns=columns,
            criticality="warn",
            check_func_kwargs={"allowed": [1, 2, 3, 5, 6, 7, 8, 9, 10]},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col9"])
@pytest.mark.benchmark(group="test_benchmark_is_in_list")
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
    benchmark.group += f" {column}"
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize(
    "generated_integer_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_in_list")
def test_benchmark_foreach_is_in_list(benchmark, ws, generated_integer_df):
    columns, df, n_rows = generated_integer_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            check_func=check_funcs.is_in_list,
            columns=columns,
            criticality="warn",
            check_func_kwargs={"allowed": [1, 2, 3, 5, 6, 7, 8, 9, 10]},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


@pytest.mark.parametrize("column", ["col1", "col2", "col3", "col5", "col6", "col9"])
@pytest.mark.benchmark(group="test_benchmark_sql_expression")
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
    benchmark.group += f" {column}"
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize(
    "generated_string_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_sql_expression")
def test_benchmark_foreach_sql_expression(benchmark, ws, generated_string_df):
    columns, df, n_rows = generated_string_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = []
    for col in columns:
        checks.extend(
            DQForEachColRule(
                check_func=check_funcs.sql_expression,
                columns=[col],
                criticality="warn",
                check_func_kwargs={"expression": f"{col} NOT LIKE 'val%'"},
            ).get_rules()
        )
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_date_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 2}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_older_than_col2_for_n_days")
def test_benchmark_foreach_is_older_than_col2_for_n_days(benchmark, ws, generated_date_df):
    columns, df, n_rows = generated_date_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_older_than_col2_for_n_days,
            columns=columns,
            check_func_kwargs={"column1": "col1", "column2": "col2", "days": 1},
            user_metadata={"tag2": "value5"},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_date_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 1}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_older_than_n_days")
def test_benchmark_foreach_is_older_than_n_days(benchmark, ws, generated_date_df):
    columns, df, n_rows = generated_date_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_older_than_n_days,
            columns=columns,
            check_func_kwargs={"days": 1},
            user_metadata={"tag2": "value5"},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_timestamp_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 1}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_not_in_future")
def test_benchmark_foreach_is_not_in_future(benchmark, ws, generated_timestamp_df):
    columns, df, n_rows = generated_timestamp_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_not_in_future,
            columns=columns,
            check_func_kwargs={"offset": 10000},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_timestamp_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 1}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_not_in_near_future")
def test_benchmark_foreach_is_not_in_near_future(benchmark, ws, generated_timestamp_df):
    columns, df, n_rows = generated_timestamp_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_not_in_near_future,
            columns=columns,
            check_func_kwargs={"offset": 10000},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


def test_benchmark_is_equal_to(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_equal_to,
            column="col1",
            check_func_kwargs={"value": 1},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize(
    "generated_integer_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_equal_to")
def test_benchmark_foreach_is_equal_to(benchmark, ws, generated_integer_df):
    columns, df, n_rows = generated_string_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_equal_to,
            columns=columns,
            check_func_kwargs={"value": 1},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


def test_benchmark_is_not_equal_to(benchmark, ws, generated_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_equal_to,
            column="col1",
            check_func_kwargs={"value": 1},
        )
    ]
    checked = dq_engine.apply_checks(generated_df, checks)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS


@pytest.mark.parametrize(
    "generated_integer_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_not_equal_to")
def test_benchmark_foreach_is_not_equal_to(benchmark, ws, generated_string_df):
    columns, df, n_rows = generated_string_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_not_equal_to,
            columns=columns,
            check_func_kwargs={"value": 1},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_integer_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_not_less_than")
def test_benchmark_foreach_is_not_less_than(benchmark, ws, generated_integer_df):
    columns, df, n_rows = generated_integer_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_not_less_than,
            columns=columns,
            check_func_kwargs={"limit": 1000},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_integer_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_not_greater_than")
def test_benchmark_foreach_is_not_greater_than(benchmark, ws, generated_integer_df):
    columns, df, n_rows = generated_integer_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_not_greater_than,
            columns=columns,
            check_func_kwargs={"limit": 1000},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_integer_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_in_range")
def test_benchmark_foreach_is_in_range(benchmark, ws, generated_integer_df):
    columns, df, n_rows = generated_integer_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_in_range,
            columns=columns,
            check_func_kwargs={"min_limit": 5, "max_limit": 100_000},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_integer_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_not_in_range")
def test_benchmark_foreach_is_not_in_range(benchmark, ws, generated_integer_df):
    columns, df, n_rows = generated_integer_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_not_in_range,
            columns=columns,
            check_func_kwargs={"min_limit": 1_000_000, "max_limit": 1_000_000},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_string_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_regex_match")
def test_benchmark_foreach_regex_match(benchmark, ws, generated_string_df):
    columns, df, n_rows = generated_string_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.regex_match,
            columns=columns,
            check_func_kwargs={"regex": "[0-9]+", "negate": False},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_array_string_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5, "array_length": 2}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}_array_length_{param['array_length']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_not_null_and_not_empty_array")
def test_benchmark_foreach_is_not_null_and_not_empty_array(benchmark, ws, generated_array_string_df):
    columns, df, n_rows = generated_array_string_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty_array,
            columns=columns,
            check_func_kwargs={},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_date_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_valid_date")
def test_benchmark_foreach_is_valid_date(benchmark, ws, generated_date_df):
    columns, df, n_rows = generated_date_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_valid_date,
            columns=columns,
            check_func_kwargs={"date_format": "yyyy-MM-dd"},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_timestamp_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_valid_timestamp")
def test_benchmark_foreach_is_valid_timestamp(benchmark, ws, generated_timestamp_df):
    columns, df, n_rows = generated_timestamp_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_valid_timestamp,
            columns=columns,
            check_func_kwargs={"timestamp_format": "yyyy-MM-dd HH:mm:ss"},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"

    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_string_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5, "template": r"\n.\n.\n.\n"}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_valid_ipv4_address")
def test_benchmark_foreach_is_valid_ipv4_address(benchmark, ws, generated_string_df):
    columns, df, n_rows = generated_string_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_valid_ipv4_address,
            columns=columns,
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_string_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5, "template": r"\n.\n.\n.\n"}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_ipv4_address_in_cidr")
def test_benchmark_foreach_is_ipv4_address_in_cidr(benchmark, ws, generated_string_df):
    columns, df, n_rows = generated_string_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_ipv4_address_in_cidr,
            columns=columns,
            check_func_kwargs={"cidr_block": "255.255.255.255/32"},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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


@pytest.mark.parametrize(
    "generated_string_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_data_fresh")
def test_benchmark_foreach_is_data_fresh(benchmark, ws, generated_string_df):
    columns, df, n_rows = generated_string_df
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checks = [
        *DQForEachColRule(
            criticality="error",
            check_func=check_funcs.is_data_fresh,
            columns=columns,
            check_func_kwargs={"max_age_minutes": 1440},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    result = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
    assert result == EXPECTED_ROWS


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

@pytest.mark.parametrize(
    "generated_string_df",
    [{"n_rows": DEFAULT_ROWS, "n_columns": 5}],
    indirect=True,
    ids=lambda param: f"n_rows_{param['n_rows']}_n_columns_{param['n_columns']}",
)
@pytest.mark.benchmark(group="test_benchmark_foreach_is_unique")
def test_benchmark_is_unique(benchmark, ws, generated_string_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    columns, df, n_rows = generated_string_df
    checks = [
        *DQForEachColRule(
            check_func=check_funcs.is_unique,
            criticality="warn",
            columns=[columns],
            check_func_kwargs={"nulls_distinct": False},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    actual_count = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
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
    checked = dq_engine.apply_checks(generated_df, checks, refs_df)
    actual_count = benchmark(lambda: checked.count())
    assert actual_count == EXPECTED_ROWS

@pytest.mark.benchmark(group="test_benchmark_foreach_foreign_key")
def test_benchmark_foreign_key(benchmark, ws, generated_string_df):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    columns, df, n_rows = generated_string_df
    checks = [
        *DQForEachColRule(
            check_func=check_funcs.foreign_key,
            criticality="warn",
            columns=[columns],
            check_func_kwargs={"nulls_distinct": False},
        ).get_rules()
    ]
    benchmark.group += f"_{n_rows}_rows_{len(columns)}_columns"
    actual_count = benchmark(lambda: dq_engine.apply_checks(df, checks).count())
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

    checked = dq_engine.apply_checks(generated_df, checks, refs_df)
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
