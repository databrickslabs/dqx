from datetime import datetime
from pathlib import Path
from collections.abc import Callable
import yaml
import pyspark.sql.functions as F
import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import (
    ExtraParams,
    DQForEachColRule,
    ColumnArguments,
    register_rule,
    DQRowRule,
    DQDatasetRule,
)
from databricks.labs.dqx.schema import dq_result_schema
from databricks.labs.dqx import check_funcs


SCHEMA = "a: int, b: int, c: int"
REPORTING_COLUMNS = f", _errors: {dq_result_schema.simpleString()}, _warnings: {dq_result_schema.simpleString()}"
EXPECTED_SCHEMA = SCHEMA + REPORTING_COLUMNS
EXPECTED_SCHEMA_WITH_CUSTOM_NAMES = (
    SCHEMA + f", dq_errors: {dq_result_schema.simpleString()}, dq_warnings: {dq_result_schema.simpleString()}"
)

RUN_TIME = datetime(2025, 1, 1, 0, 0, 0, 0)
EXTRA_PARAMS = ExtraParams(run_time=RUN_TIME)


def test_apply_checks_on_empty_checks(ws, spark):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, None], [2, 4, None]], SCHEMA)

    good = dq_engine.apply_checks(test_df, [])

    expected_df = spark.createDataFrame([[1, 3, None, None, None], [2, 4, None, None, None]], EXPECTED_SCHEMA)
    assert_df_equality(good, expected_df)


def test_apply_checks_and_split_on_empty_checks(ws, spark):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, None], [2, 4, None]], SCHEMA)

    good, bad = dq_engine.apply_checks_and_split(test_df, [])

    expected_df = spark.createDataFrame([], EXPECTED_SCHEMA)

    assert_df_equality(good, test_df)
    assert_df_equality(bad, expected_df)


def test_apply_checks_passed(ws, spark):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3]], SCHEMA)

    checks = [
        DQRowRule(
            name="a_is_null_or_empty",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="a",
        ),
        DQRowRule(
            name="b_is_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column=F.col("b"),
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame([[1, 3, 3, None, None]], EXPECTED_SCHEMA)
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_foreign_key_check(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    src_df = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [4, 5, 6],
            [6, 6, 7],
            [None, None, None],
        ],
        SCHEMA,
    )

    ref_df = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [5, 6, 7],
            [None, None, None],
        ],
        SCHEMA,
    )
    ref_column = "a"

    checks = [
        DQDatasetRule(
            name="a_has_no_foreign_key",
            criticality="warn",
            check_func=check_funcs.foreign_key,
            column="a",
            check_func_kwargs={
                "ref_column": ref_column,
                "ref_df_name": "ref_df",
            },
            user_metadata={"tag1": "value1", "tag2": "value2"},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.foreign_key,
            column=F.col("a"),
            filter="a > 4",
            check_func_kwargs={
                "ref_column": F.col(ref_column),
                "ref_df_name": "ref_df",
            },
        ),
    ]

    refs_df = {"ref_df": ref_df}

    checked = dq_engine.apply_checks(src_df, checks, refs_df)
    good_df, bad_df = dq_engine.apply_checks_and_split(src_df, checks, refs_df)

    expected = spark.createDataFrame(
        [
            [1, 2, 3, None, None],
            [1, 2, 3, None, None],
            [
                4,
                5,
                6,
                None,
                [
                    {
                        "name": "a_has_no_foreign_key",
                        "message": "FK violation: Value '4' in column 'a' not found in reference column 'a'",
                        "columns": ["a"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "value2"},
                    }
                ],
            ],
            [
                6,
                6,
                7,
                [
                    {
                        "name": "a_a_foreign_key_violation",
                        "message": "FK violation: Value '6' in column 'a' not found in reference column 'a'",
                        "columns": ["a"],
                        "filter": "a > 4",
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_has_no_foreign_key",
                        "message": "FK violation: Value '6' in column 'a' not found in reference column 'a'",
                        "columns": ["a"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "value2"},
                    }
                ],
            ],
            [None, None, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)
    assert_df_equality(
        bad_df, expected.where(F.col("_errors").isNotNull() | F.col("_warnings").isNotNull()), ignore_nullable=True
    )
    assert_df_equality(good_df, expected.where(F.col("_errors").isNull()).select("a", "b", "c"), ignore_nullable=True)


def test_foreign_key_check_yaml(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    src_df = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [4, 5, 6],
            [6, 6, 7],
            [None, None, None],
        ],
        SCHEMA,
    )

    ref_df = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [5, 6, 7],
            [None, None, None],
        ],
        SCHEMA,
    )
    ref_column = "a"

    checks = yaml.safe_load(
        f"""
        - name: a_has_no_foreign_key
          criticality: warn
          check:
            function: foreign_key
            arguments:
              column: a
              ref_column: {ref_column}
              ref_df_name: ref_df
          user_metadata:
            tag1: value1
            tag2: value2
        - criticality: error
          filter: a > 4
          check:
            function: foreign_key
            arguments:
              column: a
              ref_column: {ref_column}
              ref_df_name: ref_df
        """
    )

    ref_dfs = {"ref_df": ref_df}

    checked = dq_engine.apply_checks_by_metadata(src_df, checks, ref_dfs=ref_dfs)
    good_df, bad_df = dq_engine.apply_checks_by_metadata_and_split(src_df, checks, ref_dfs=ref_dfs)

    expected = spark.createDataFrame(
        [
            [1, 2, 3, None, None],
            [1, 2, 3, None, None],
            [
                4,
                5,
                6,
                None,
                [
                    {
                        "name": "a_has_no_foreign_key",
                        "message": "FK violation: Value '4' in column 'a' not found in reference column 'a'",
                        "columns": ["a"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "value2"},
                    }
                ],
            ],
            [
                6,
                6,
                7,
                [
                    {
                        "name": "a_a_foreign_key_violation",
                        "message": "FK violation: Value '6' in column 'a' not found in reference column 'a'",
                        "columns": ["a"],
                        "filter": "a > 4",
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_has_no_foreign_key",
                        "message": "FK violation: Value '6' in column 'a' not found in reference column 'a'",
                        "columns": ["a"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "value2"},
                    }
                ],
            ],
            [None, None, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)
    assert_df_equality(
        bad_df, expected.where(F.col("_errors").isNotNull() | F.col("_warnings").isNotNull()), ignore_nullable=True
    )
    assert_df_equality(good_df, expected.where(F.col("_errors").isNull()).select("a", "b", "c"), ignore_nullable=True)


def test_foreign_key_check_using_ref_table(ws, spark, make_schema, make_random):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    src_df = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [4, 5, 6],
            [6, 6, 7],
            [None, None, None],
        ],
        SCHEMA,
    )

    ref_df = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [5, 6, 7],
            [None, None, None],
        ],
        SCHEMA,
    )
    ref_column = "a"

    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    ref_table = f"{catalog_name}.{schema.name}.{make_random(6).lower()}"
    ref_df.write.saveAsTable(ref_table)

    checks = [
        DQDatasetRule(
            name="a_has_no_foreign_key",
            criticality="warn",
            check_func=check_funcs.foreign_key,
            column="a",
            check_func_kwargs={
                "ref_column": ref_column,
                "ref_table": ref_table,
            },
            user_metadata={"tag1": "value1", "tag2": "value2"},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.foreign_key,
            column=F.col("a"),
            filter="a > 4",
            check_func_kwargs={
                "ref_column": F.col(ref_column),
                "ref_table": ref_table,
            },
        ),
    ]

    checked = dq_engine.apply_checks(src_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 2, 3, None, None],
            [1, 2, 3, None, None],
            [
                4,
                5,
                6,
                None,
                [
                    {
                        "name": "a_has_no_foreign_key",
                        "message": "FK violation: Value '4' in column 'a' not found in reference column 'a'",
                        "columns": ["a"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "value2"},
                    }
                ],
            ],
            [
                6,
                6,
                7,
                [
                    {
                        "name": "a_a_foreign_key_violation",
                        "message": "FK violation: Value '6' in column 'a' not found in reference column 'a'",
                        "columns": ["a"],
                        "filter": "a > 4",
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_has_no_foreign_key",
                        "message": "FK violation: Value '6' in column 'a' not found in reference column 'a'",
                        "columns": ["a"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "value2"},
                    }
                ],
            ],
            [None, None, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_foreign_key_check_missing_ref_df(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    src_df = spark.createDataFrame(
        [
            [1, 2, 3],
        ],
        SCHEMA,
    )

    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            column="a",
            check_func_kwargs={
                "ref_column": "a",
                "ref_df_name": "ref_df",
            },
        ),
    ]

    refs_df = {}
    with pytest.raises(ValueError, match="Reference DataFrame 'ref_df' not found in provided reference DataFrames"):
        dq_engine.apply_checks(src_df, checks, refs_df)


def test_foreign_key_check_missing_provided_both_ref_df_and_table(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    src_df = spark.createDataFrame(
        [
            [1, 2, 3],
        ],
        SCHEMA,
    )

    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            column="a",
            check_func_kwargs={
                "ref_column": "a",
                "ref_df_name": "ref_df",
                "ref_table": "table",
            },
        ),
    ]

    refs_df = {}
    with pytest.raises(ValueError, match="Both 'ref_df_name' and 'ref_table' are provided"):
        dq_engine.apply_checks(src_df, checks, refs_df)


def test_foreign_key_check_missing_missing_ref_df_and_table(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    src_df = spark.createDataFrame(
        [
            [1, 2, 3],
        ],
        SCHEMA,
    )

    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.foreign_key,
            column="a",
            check_func_kwargs={
                "ref_column": "a",
            },
        ),
    ]

    refs_df = {}
    with pytest.raises(ValueError, match="Either 'ref_df_name' or 'ref_table' must be provided"):
        dq_engine.apply_checks(src_df, checks, refs_df)


def test_apply_is_unique(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame(
        [
            [1, 2, None],
            [1, 1, None],
            [1, 1, None],
            [1, None, None],
            [1, None, None],
            [1, None, None],
            [None, None, None],
        ],
        SCHEMA,
    )

    checks = [
        DQDatasetRule(
            criticality="error",
            filter="b = 1 or b is null",
            check_func=check_funcs.is_unique,
            columns=["a", "b"],
            check_func_kwargs={"nulls_distinct": True},
        ),
        DQDatasetRule(
            criticality="warn",
            filter="b > 1 or b is null",
            check_func=check_funcs.is_unique,
            columns=["a", "b"],
            check_func_kwargs={"nulls_distinct": False},
        ),
        DQDatasetRule(
            name="must_filter_as_part_of_check",
            criticality="error",
            filter="b = 2",
            check_func=check_funcs.is_unique,
            columns=["a"],
            check_func_kwargs={"nulls_distinct": True},
        ),
    ]
    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [
                1,
                1,
                None,
                [
                    {
                        "name": "struct_a_b_is_not_unique",
                        "message": "Value '{1, 1}' in column 'struct(a, b)' is not unique, found 2 duplicates",
                        "columns": ["a", "b"],
                        "filter": "b = 1 or b is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                1,
                1,
                None,
                [
                    {
                        "name": "struct_a_b_is_not_unique",
                        "message": "Value '{1, 1}' in column 'struct(a, b)' is not unique, found 2 duplicates",
                        "columns": ["a", "b"],
                        "filter": "b = 1 or b is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [None, None, None, None, None],
            [
                1,
                None,
                None,
                None,
                [
                    {
                        "name": "struct_a_b_is_not_unique",
                        "message": "Value '{1, null}' in column 'struct(a, b)' is not unique, found 3 duplicates",
                        "columns": ["a", "b"],
                        "filter": "b > 1 or b is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                1,
                None,
                None,
                None,
                [
                    {
                        "name": "struct_a_b_is_not_unique",
                        "message": "Value '{1, null}' in column 'struct(a, b)' is not unique, found 3 duplicates",
                        "columns": ["a", "b"],
                        "filter": "b > 1 or b is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                1,
                None,
                None,
                None,
                [
                    {
                        "name": "struct_a_b_is_not_unique",
                        "message": "Value '{1, null}' in column 'struct(a, b)' is not unique, found 3 duplicates",
                        "columns": ["a", "b"],
                        "filter": "b > 1 or b is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [1, 2, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRowRule(
            name="a_is_null_or_empty",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="a",
            user_metadata={"tag1": "value11", "tag2": "value21"},
        ),
        DQRowRule(
            name="b_is_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="b",
            user_metadata={"tag1": "value12", "tag2": "value22"},
        ),
        DQRowRule(
            name="c_is_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="c",
            user_metadata={"tag1": "value13", "tag2": "value23"},
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value12", "tag2": "value22"},
                    }
                ],
                None,
            ],
            [
                None,
                4,
                None,
                [
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value13", "tag2": "value23"},
                    }
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value11", "tag2": "value21"},
                    }
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value12", "tag2": "value22"},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value13", "tag2": "value23"},
                    },
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value11", "tag2": "value21"},
                    }
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_using_yaml_invalid_criticality(ws, spark):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3]], SCHEMA)

    checks = yaml.safe_load(
        """
    - criticality: invalid
      check:
        function: is_not_null_and_not_empty
        arguments:
          column: col1
    """
    )

    with pytest.raises(ValueError, match="Invalid 'criticality' value"):
        dq_engine.apply_checks_by_metadata(test_df, checks)


def test_apply_checks_using_classes_invalid_criticality(ws, spark):
    dq_engine = DQEngine(ws)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRowRule(
            name="c_is_null_or_empty",
            criticality="invalid",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="c",
        ),
    ]

    with pytest.raises(ValueError, match="Invalid 'criticality' value"):
        dq_engine.apply_checks(test_df, checks)


def test_apply_checks_from_yaml_missing_criticality(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    schema = "col1: int, col2: int, col3: int"
    test_df = spark.createDataFrame([[1, 2, 3], [None, None, None]], schema)

    checks = yaml.safe_load(
        """
    - check:
        function: is_not_null
        arguments:
          column: col1
    - check:
        function: is_not_null
        arguments:
          column: col2
        criticality: warn
    - check:
        function: is_not_null
        for_each_column:
          - col3
        criticality: warn
    """
    )

    actual = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 2, 3, None, None],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "col1_is_null",
                        "message": "Column 'col1' value is null",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col2_is_null",
                        "message": "Column 'col2' value is null",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col3_is_null",
                        "message": "Column 'col3' value is null",
                        "columns": ["col3"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_apply_checks_from_class_missing_criticality(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    schema = "col1: int, col2: int, col3: int"
    test_df = spark.createDataFrame([[1, 2, 3], [None, None, None]], schema)

    checks = [
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null, column="col1"),
        DQRowRule(
            # missing criticality, default to "error"
            check_func=check_funcs.is_not_null,
            column="col2",
        ),
    ] + DQForEachColRule(
        # missing criticality, default to "error"
        check_func=check_funcs.is_not_null,
        columns=["col3"],
    ).get_rules()

    actual = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 2, 3, None, None],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "col1_is_null",
                        "message": "Column 'col1' value is null",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col2_is_null",
                        "message": "Column 'col2' value is null",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col3_is_null",
                        "message": "Column 'col3' value is null",
                        "columns": ["col3"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_apply_checks_with_autogenerated_columns(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRowRule(criticality="warn", check_func=check_funcs.is_not_null_and_not_empty, column="a"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null_and_not_empty, column="b"),
        DQRowRule(criticality="error", check_func=check_funcs.is_not_null_and_not_empty, column="c"),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                None,
                4,
                None,
                [
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_and_split(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRowRule(
            name="a_is_null_or_empty",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="a",
        ),
        DQRowRule(
            name="b_is_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="b",
        ),
        DQRowRule(
            name="c_is_null_or_empty",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="c",
        ),
    ]

    good, bad = dq_engine.apply_checks_and_split(test_df, checks)

    expected_good = spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA)
    assert_df_equality(good, expected_good)

    expected_bad = spark.createDataFrame(
        [
            [
                2,
                None,
                4,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(bad, expected_bad, ignore_nullable=True)


def test_apply_checks_and_split_by_metadata(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {
            "name": "a_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "a"}},
        },
        {
            "name": "b_is_null_or_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
        },
        {
            "name": "c_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "c"}},
        },
        {
            "name": "a_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "a", "allowed": [1, 3, 4]}},
        },
        {
            "name": "c_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "c", "allowed": [1, 3, 4]}},
        },
    ]

    good, bad = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)

    expected_good = spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA)
    assert_df_equality(good, expected_good)

    expected_bad = spark.createDataFrame(
        [
            [
                2,
                None,
                4,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_not_in_the_list",
                        "message": "Value '2' in Column 'a' is not in the allowed list: [1, 3, 4]",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_in_list",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(bad, expected_bad, ignore_nullable=True)


def test_apply_checks_and_split_by_metadata_with_autogenerated_columns(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "for_each_column": ["a", "c"], "arguments": {}},
        },
        {
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "for_each_column": ["a", "c"], "arguments": {"allowed": [1, 3, 4]}},
        },
    ]

    good, bad = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)

    expected_good = spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA)
    assert_df_equality(good, expected_good)

    expected_bad = spark.createDataFrame(
        [
            [
                2,
                None,
                4,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_not_in_the_list",
                        "message": "Value '2' in Column 'a' is not in the allowed list: [1, 3, 4]",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_in_list",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(bad, expected_bad, ignore_nullable=True)


def test_apply_checks_by_metadata(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "for_each_column": ["a", "c"]},
        },
        {
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
        },
        {
            "criticality": "warn",
            "check": {"function": "is_in_list", "for_each_column": ["a", "c"], "arguments": {"allowed": [1, 3, 4]}},
        },
    ]

    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_not_in_the_list",
                        "message": "Value '2' in Column 'a' is not in the allowed list: [1, 3, 4]",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_in_list",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                None,
                None,
                None,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_with_filter(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame(
        [[1, 3, 3], [2, None, 4], [3, 4, None], [4, None, None], [None, None, None]], SCHEMA
    )

    checks = DQForEachColRule(
        check_func=check_funcs.is_not_null_and_not_empty, criticality="warn", filter="b>3", columns=["a", "c"]
    ).get_rules() + [
        DQRowRule(
            name="b_is_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="b",
            filter="a<3",
        )
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": "a<3",
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                3,
                4,
                None,
                None,
                [
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": "b>3",
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [4, None, None, None, None],
            [None, None, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_with_multiple_cols_and_common_name(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, None, None], [None, 2, None]], SCHEMA)

    checks = (
        DQForEachColRule(
            name="common_name", check_func=check_funcs.is_not_null, criticality="warn", columns=["a", "b"]
        ).get_rules()
        + DQForEachColRule(
            name="common_name2",
            check_func=check_funcs.is_unique,
            criticality="warn",
            columns=[["a", "b"], ["c"], ["c"]],
            check_func_kwargs={"nulls_distinct": False},
        ).get_rules()
        + DQForEachColRule(
            name="common_name2",
            check_func=check_funcs.is_aggr_not_less_than,
            criticality="warn",
            columns=["a", "a"],
            check_func_kwargs={"limit": 0},
        ).get_rules()
        + DQForEachColRule(
            name="common_name2",
            check_func=check_funcs.is_aggr_not_greater_than,
            criticality="warn",
            columns=["a", "a"],
            check_func_kwargs={"limit": 10},
        ).get_rules()
        + DQForEachColRule(
            name="common_name2",
            check_func=check_funcs.foreign_key,
            criticality="warn",
            columns=["a", "a"],
            check_func_kwargs={"ref_column": "ref_a", "ref_df_name": "ref_df"},
        ).get_rules()
    )

    ref_df = spark.createDataFrame([[1]], "ref_a: int")
    ref_dfs = {"ref_df": ref_df}

    checked = dq_engine.apply_checks(test_df, checks, ref_dfs)

    expected = spark.createDataFrame(
        [
            [
                None,
                2,
                None,
                None,
                [
                    {
                        "name": "common_name",
                        "message": "Column 'a' value is null",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "common_name2",
                        "message": "Value '{null}' in column 'struct(c)' is not unique, found 2 duplicates",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "common_name2",
                        "message": "Value '{null}' in column 'struct(c)' is not unique, found 2 duplicates",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                1,
                None,
                None,
                None,
                [
                    {
                        "name": "common_name",
                        "message": "Column 'b' value is null",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "common_name2",
                        "message": "Value '{null}' in column 'struct(c)' is not unique, found 2 duplicates",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "common_name2",
                        "message": "Value '{null}' in column 'struct(c)' is not unique, found 2 duplicates",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_by_metadata_with_filter(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame(
        [[1, 3, 3], [2, None, 4], [3, 4, None], [4, None, None], [None, None, None]], SCHEMA
    )

    checks = [
        {
            "criticality": "warn",
            "filter": "b>3",
            "check": {"function": "is_not_null_and_not_empty", "for_each_column": ["b", "c"], "arguments": {}},
        },
        {
            "criticality": "error",
            "filter": "a<3",
            "check": {"function": "is_not_null_and_not_empty", "for_each_column": None, "arguments": {"column": "b"}},
        },
    ]

    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [
                2,
                None,
                4,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": "a<3",
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                3,
                4,
                None,
                None,
                [
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": "b>3",
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [4, None, None, None, None],
            [None, None, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_from_json_file_by_metadata(ws, spark, make_local_check_file_as_json):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    schema = "col1: int, col2: int, col3: int, col4 int"
    test_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

    check_file = make_local_check_file_as_json
    checks = DQEngine.load_checks_from_local_file(check_file)

    actual = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, 1, None, None],
            [
                2,
                None,
                4,
                1,
                [
                    {
                        "name": "col2_is_null",
                        "message": "Column 'col2' value is null",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_apply_checks_from_yaml_file_by_metadata(ws, spark, make_local_check_file_as_yaml):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    schema = "col1: int, col2: int, col3: int, col4 int"
    test_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

    check_file = make_local_check_file_as_yaml
    checks = DQEngine.load_checks_from_local_file(check_file)

    actual = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, 1, None, None],
            [
                2,
                None,
                4,
                1,
                [
                    {
                        "name": "col2_is_null",
                        "message": "Column 'col2' value is null",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(actual, expected, ignore_nullable=True)


def custom_row_check_func_global(column: str) -> Column:
    col_expr = F.col(column)
    return check_funcs.make_condition(col_expr.isNull(), "custom check failed", f"{column}_is_null_custom")


def custom_row_check_func_custom_args(column_custom_arg: str) -> Column:
    col_expr = F.col(column_custom_arg)
    return check_funcs.make_condition(
        col_expr.isNull(), "custom check with custom args failed", f"{column_custom_arg}_is_null_custom_args"
    )


def custom_row_check_func_global_a_column_no_args() -> Column:
    col_expr = F.col("a")
    return check_funcs.make_condition(col_expr.isNull(), "custom check without args failed", "a_is_null_custom")


@register_rule("row")
def custom_row_check_func_global_registered(column: str) -> Column:
    col_expr = F.col(column)
    return check_funcs.make_condition(col_expr.isNull(), "custom check registered failed", f"{column}_is_null_custom")


def custom_dataset_check_func(column: str) -> tuple[Column, Callable]:
    col_expr = F.col(column)
    condition_col = "condition"

    def closure(df: DataFrame, _ref_dfs: dict[str, DataFrame] | None = None) -> DataFrame:
        check_df = df.withColumn(condition_col, col_expr.isNull())
        return check_df

    return (
        check_funcs.make_condition(
            condition=F.col(condition_col) == F.lit(True),  # check condition returns true
            message="dataset check failed",
            alias=f"{column}_custom_dataset_check",
        ),
        closure,
    )


@register_rule("dataset")
def custom_dataset_check_func_registered_with_ref_dfs(column: str) -> tuple[Column, Callable]:
    col_expr = F.col(column)
    condition_col = "condition"

    def closure(df: DataFrame, _ref_dfs: dict[str, DataFrame] | None = None) -> DataFrame:
        check_df = df.withColumn(condition_col, col_expr.isNull())
        return check_df

    return (
        check_funcs.make_condition(
            condition=F.col(condition_col) == F.lit(True),  # check condition returns true
            message="dataset check registered failed",
            alias=f"{column}_custom_dataset_check",
        ),
        closure,
    )


@register_rule("dataset")
def custom_dataset_check_func_registered_with_spark_sql(column: str) -> tuple[Column, Callable]:
    condition_col = f"{column}_is_null_check"

    def closure(df: DataFrame, spark: SparkSession) -> DataFrame:
        df.createOrReplaceTempView("temp_df")

        sql_query = f"""
                SELECT *,
                       CASE WHEN {column} IS NULL THEN TRUE ELSE FALSE END AS {condition_col}
                FROM temp_df
            """

        return spark.sql(sql_query)

    return (
        check_funcs.make_condition(
            condition=F.col(condition_col) == F.lit(True),  # check condition returns true
            message="dataset check registered failed",
            alias=f"{column}_custom_dataset_check",
        ),
        closure,
    )


def test_apply_checks_with_custom_check(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRowRule(criticality="warn", check_func=check_funcs.is_not_null_and_not_empty, column="a"),
        DQRowRule(criticality="warn", check_func=custom_row_check_func_global, column="a"),
        DQRowRule(criticality="warn", check_func=custom_row_check_func_global, check_func_kwargs={"column": "a"}),
        DQRowRule(criticality="warn", check_func=custom_row_check_func_global_registered, column="a"),
        DQRowRule(
            criticality="warn", check_func=custom_row_check_func_global_registered, check_func_kwargs={"column": "a"}
        ),
        DQRowRule(criticality="warn", check_func=custom_row_check_func_global_a_column_no_args),
        DQRowRule(
            criticality="warn",
            check_func=custom_row_check_func_custom_args,
            check_func_kwargs={"column_custom_arg": "a"},
        ),
        DQDatasetRule(criticality="warn", check_func=custom_dataset_check_func, column="a"),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [2, None, 4, None, None],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_row_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check failed",
                        "columns": None,
                        "filter": None,
                        "function": "custom_row_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check registered failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_row_check_func_global_registered",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check registered failed",
                        "columns": None,
                        "filter": None,
                        "function": "custom_row_check_func_global_registered",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check without args failed",
                        "columns": None,
                        "filter": None,
                        "function": "custom_row_check_func_global_a_column_no_args",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom_args",
                        "message": "custom check with custom args failed",
                        "columns": None,
                        "filter": None,
                        "function": "custom_row_check_func_custom_args",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_custom_dataset_check",
                        "message": "dataset check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_dataset_check_func",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                None,
                None,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_row_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check failed",
                        "columns": None,
                        "filter": None,
                        "function": "custom_row_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check registered failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_row_check_func_global_registered",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check registered failed",
                        "columns": None,
                        "filter": None,
                        "function": "custom_row_check_func_global_registered",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check without args failed",
                        "columns": None,
                        "filter": None,
                        "function": "custom_row_check_func_global_a_column_no_args",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom_args",
                        "message": "custom check with custom args failed",
                        "columns": None,
                        "filter": None,
                        "function": "custom_row_check_func_custom_args",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_custom_dataset_check",
                        "message": "dataset check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_dataset_check_func",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_for_each_col_with_custom_check(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[None, None, None]], SCHEMA)

    checks = (
        DQForEachColRule(criticality="warn", check_func=custom_row_check_func_global, columns=["a", "b"]).get_rules()
        + DQForEachColRule(
            # check func must be registered as dataset check to use in DQForEachColRule
            criticality="warn",
            check_func=custom_dataset_check_func_registered_with_ref_dfs,
            columns=["a", "b"],
        ).get_rules()
        + DQForEachColRule(
            criticality="warn", check_func=custom_dataset_check_func_registered_with_spark_sql, columns=["a", "b"]
        ).get_rules()
    )

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [
                None,
                None,
                None,
                None,
                [
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_row_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "b_is_null_custom",
                        "message": "custom check failed",
                        "columns": ["b"],
                        "filter": None,
                        "function": "custom_row_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_custom_dataset_check",
                        "message": "dataset check registered failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_dataset_check_func_registered_with_ref_dfs",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "b_custom_dataset_check",
                        "message": "dataset check registered failed",
                        "columns": ["b"],
                        "filter": None,
                        "function": "custom_dataset_check_func_registered_with_ref_dfs",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_custom_dataset_check",
                        "message": "dataset check registered failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_dataset_check_func_registered_with_spark_sql",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "b_custom_dataset_check",
                        "message": "dataset check registered failed",
                        "columns": ["b"],
                        "filter": None,
                        "function": "custom_dataset_check_func_registered_with_spark_sql",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_by_metadata_with_custom_check(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {"criticality": "warn", "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "a"}}},
        {"criticality": "warn", "check": {"function": "custom_row_check_func_global", "arguments": {"column": "a"}}},
        {
            "criticality": "warn",
            "check": {"function": "custom_row_check_func_global_registered", "arguments": {"column": "a"}},
        },
        {
            "criticality": "warn",
            "check": {"function": "custom_row_check_func_custom_args", "arguments": {"column_custom_arg": "a"}},
        },
    ]

    checked = dq_engine.apply_checks_by_metadata(
        test_df,
        checks,
        custom_check_functions={
            "custom_row_check_func_global": custom_row_check_func_global,
            "custom_row_check_func_global_registered": custom_row_check_func_global_registered,
            "custom_row_check_func_custom_args": custom_row_check_func_custom_args,
        },
    )
    # or for simplicity use globals
    checked2 = dq_engine.apply_checks_by_metadata(test_df, checks, custom_check_functions=globals())

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [2, None, 4, None, None],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_row_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check registered failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_row_check_func_global_registered",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom_args",
                        "message": "custom check with custom args failed",
                        "columns": None,
                        "filter": None,
                        "function": "custom_row_check_func_custom_args",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                None,
                None,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_row_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom",
                        "message": "custom check registered failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_row_check_func_global_registered",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_is_null_custom_args",
                        "message": "custom check with custom args failed",
                        "columns": None,
                        "filter": None,
                        "function": "custom_row_check_func_custom_args",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)
    assert_df_equality(checked2, expected, ignore_nullable=True)


def test_get_valid_records(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    test_df = spark.createDataFrame(
        [
            [1, 1, 1, None, None],
            [
                None,
                2,
                2,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_null_or_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                None,
                2,
                2,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "check failed",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_null_or_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        EXPECTED_SCHEMA,
    )

    valid_df = dq_engine.get_valid(test_df)

    expected_valid_df = spark.createDataFrame(
        [
            [1, 1, 1],
            [None, 2, 2],
        ],
        SCHEMA,
    )

    assert_df_equality(valid_df, expected_valid_df)


def test_get_invalid_records(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    test_df = spark.createDataFrame(
        [
            [1, 1, 1, None, None],
            [
                None,
                2,
                2,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "a_is_null_or_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                None,
                2,
                2,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "check failed",
                        "columns": ["b"],
                        "filter": None,
                        "function": "a_is_null_or_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        EXPECTED_SCHEMA,
    )

    invalid_df = dq_engine.get_invalid(test_df)

    expected_invalid_df = spark.createDataFrame(
        [
            [
                None,
                2,
                2,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "a_is_null_or_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                None,
                2,
                2,
                [
                    {
                        "name": "b_is_null_or_empty",
                        "message": "check failed",
                        "columns": ["b"],
                        "filter": None,
                        "function": "a_is_null_or_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(invalid_df, expected_invalid_df)


def test_apply_checks_with_custom_column_naming(ws, spark):
    dq_engine = DQEngine(
        ws,
        extra_params=ExtraParams(
            reporting_column_names={
                ColumnArguments.ERRORS.value: "dq_errors",
                ColumnArguments.WARNINGS.value: "dq_warnings",
            },
            run_time=RUN_TIME,
        ),
    )
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [{"criticality": "warn", "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "a"}}}]
    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected = spark.createDataFrame(
        [
            [1, 3, 3, None, None],
            [2, None, 4, None, None],
            [
                None,
                4,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                None,
                None,
                None,
                None,
                [
                    {
                        "name": "a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        EXPECTED_SCHEMA_WITH_CUSTOM_NAMES,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_by_metadata_with_custom_column_naming(ws, spark):
    dq_engine = DQEngine(
        ws,
        extra_params=ExtraParams(
            reporting_column_names={
                ColumnArguments.ERRORS.value: "dq_errors",
                ColumnArguments.WARNINGS.value: "dq_warnings",
            },
            run_time=RUN_TIME,
        ),
    )
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {"criticality": "warn", "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "a"}}},
        {"criticality": "error", "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}}},
    ]
    good, bad = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)

    assert_df_equality(good, spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA), ignore_nullable=True)

    assert_df_equality(
        bad,
        spark.createDataFrame(
            [
                [
                    2,
                    None,
                    4,
                    [
                        {
                            "name": "b_is_null_or_empty",
                            "message": "Column 'b' value is null or empty",
                            "columns": ["b"],
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": RUN_TIME,
                            "user_metadata": {},
                        }
                    ],
                    None,
                ],
                [
                    None,
                    4,
                    None,
                    None,
                    [
                        {
                            "name": "a_is_null_or_empty",
                            "message": "Column 'a' value is null or empty",
                            "columns": ["a"],
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": RUN_TIME,
                            "user_metadata": {},
                        }
                    ],
                ],
                [
                    None,
                    None,
                    None,
                    [
                        {
                            "name": "b_is_null_or_empty",
                            "message": "Column 'b' value is null or empty",
                            "columns": ["b"],
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": RUN_TIME,
                            "user_metadata": {},
                        }
                    ],
                    [
                        {
                            "name": "a_is_null_or_empty",
                            "message": "Column 'a' value is null or empty",
                            "columns": ["a"],
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": RUN_TIME,
                            "user_metadata": {},
                        }
                    ],
                ],
            ],
            EXPECTED_SCHEMA_WITH_CUSTOM_NAMES,
        ),
    )


def test_apply_checks_by_metadata_with_custom_column_naming_fallback_to_default(ws, spark):
    dq_engine = DQEngine(
        ws,
        extra_params=ExtraParams(
            reporting_column_names={"errors_invalid": "dq_errors", "warnings_invalid": "dq_warnings"}, run_time=RUN_TIME
        ),
    )
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {"criticality": "warn", "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "a"}}},
        {"criticality": "error", "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}}},
    ]
    good, bad = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)

    assert_df_equality(good, spark.createDataFrame([[1, 3, 3], [None, 4, None]], SCHEMA), ignore_nullable=True)

    assert_df_equality(
        bad,
        spark.createDataFrame(
            [
                [
                    2,
                    None,
                    4,
                    [
                        {
                            "name": "b_is_null_or_empty",
                            "message": "Column 'b' value is null or empty",
                            "columns": ["b"],
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": RUN_TIME,
                            "user_metadata": {},
                        }
                    ],
                    None,
                ],
                [
                    None,
                    4,
                    None,
                    None,
                    [
                        {
                            "name": "a_is_null_or_empty",
                            "message": "Column 'a' value is null or empty",
                            "columns": ["a"],
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": RUN_TIME,
                            "user_metadata": {},
                        }
                    ],
                ],
                [
                    None,
                    None,
                    None,
                    [
                        {
                            "name": "b_is_null_or_empty",
                            "message": "Column 'b' value is null or empty",
                            "columns": ["b"],
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": RUN_TIME,
                            "user_metadata": {},
                        }
                    ],
                    [
                        {
                            "name": "a_is_null_or_empty",
                            "message": "Column 'a' value is null or empty",
                            "columns": ["a"],
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": RUN_TIME,
                            "user_metadata": {},
                        }
                    ],
                ],
            ],
            EXPECTED_SCHEMA,
        ),
    )


def test_apply_checks_with_sql_expression(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    schema = "col1: string, col2: string"
    test_df = spark.createDataFrame([["str1", "str2"], ["val1", "val2"]], schema)

    checks = [
        {
            "criticality": "error",
            "check": {"function": "sql_expression", "arguments": {"expression": "col1 not like \"val%\""}},
        },
        {
            "criticality": "error",
            "check": {
                "function": "sql_expression",
                "column": "col1",  # should be skipped
                "arguments": {"expression": "col2 not like 'val%'"},
            },
        },
    ]

    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            ["str1", "str2", None, None],
            [
                "val1",
                "val2",
                [
                    {
                        "name": "col1_not_like_val",
                        "message": "Value is not matching expression: col1 not like \"val%\"",
                        "columns": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col2_not_like_val",
                        "message": "Value is not matching expression: col2 not like 'val%'",
                        "columns": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_with_is_unique(ws, spark, set_utc_timezone):
    schema = "col1: int, col2: timestamp, col3: string"
    test_df = spark.createDataFrame(
        [
            [1, datetime(2025, 1, 1), "a"],
            [1, datetime(2025, 1, 2), "a"],
            [2, None, "b"],
            [2, None, "c"],
            [None, None, ""],
            [None, None, ""],
        ],
        schema,
    )

    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_unique", "arguments": {"columns": ["col1"]}},
        },
        {
            "criticality": "warn",
            "name": "col1_is_not_unique_filter_col2_null",
            "filter": "col2 is null",
            "check": {"function": "is_unique", "arguments": {"columns": ["col1"]}},
        },
        {
            "criticality": "error",
            "name": "composite_key_col1_and_col2_is_not_unique",
            "check": {
                "function": "is_unique",
                "arguments": {"columns": ["col1", "col2"]},
            },
        },
        {
            "criticality": "error",
            "name": "composite_key_col1_and_col3_is_not_unique",
            "check": {
                "function": "is_unique",
                "arguments": {"columns": ["col1", "col3"]},
            },
        },
    ]

    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                1,
                datetime(2025, 1, 1),
                "a",
                [
                    {
                        "name": "struct_col1_is_not_unique",
                        "message": "Value '{1}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in column 'struct(col1, col3)' is not unique, found 2 duplicates",
                        "columns": ["col1", "col3"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                1,
                datetime(2025, 1, 2),
                "a",
                [
                    {
                        "name": "struct_col1_is_not_unique",
                        "message": "Value '{1}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in column 'struct(col1, col3)' is not unique, found 2 duplicates",
                        "columns": ["col1", "col3"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                None,
                None,
                "",
                None,
                None,
            ],
            [
                None,
                None,
                "",
                None,
                None,
            ],
            [
                2,
                None,
                "b",
                [
                    {
                        "name": "struct_col1_is_not_unique",
                        "message": "Value '{2}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "col1_is_not_unique_filter_col2_null",
                        "message": "Value '{2}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": "col2 is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                2,
                None,
                "c",
                [
                    {
                        "name": "struct_col1_is_not_unique",
                        "message": "Value '{2}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "col1_is_not_unique_filter_col2_null",
                        "message": "Value '{2}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": "col2 is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_with_is_unique_nulls_not_distinct(ws, spark, set_utc_timezone):
    schema = "col1: int, col2: timestamp, col3: string"
    test_df = spark.createDataFrame(
        [
            [1, datetime(2025, 1, 1), "a"],
            [1, datetime(2025, 1, 2), "a"],
            [2, None, "b"],
            [2, None, "c"],
            [None, None, ""],
            [None, None, ""],
        ],
        schema,
    )

    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_unique", "arguments": {"columns": ["col1"], "nulls_distinct": False}},
        },
        {
            "criticality": "warn",
            "name": "col1_is_not_unique_filter_col2_null",
            "filter": "col2 is null",
            "check": {"function": "is_unique", "arguments": {"columns": ["col1"], "nulls_distinct": False}},
        },
        {
            "criticality": "error",
            "name": "composite_key_col1_and_col2_is_not_unique",
            "check": {
                "function": "is_unique",
                "arguments": {"columns": ["col1", "col2"], "nulls_distinct": False},
            },
        },
        {
            "criticality": "error",
            "name": "composite_key_col1_and_col3_is_not_unique",
            "check": {
                "function": "is_unique",
                "arguments": {"columns": ["col1", "col3"], "nulls_distinct": False},
            },
        },
    ]

    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                1,
                datetime(2025, 1, 1),
                "a",
                [
                    {
                        "name": "struct_col1_is_not_unique",
                        "message": "Value '{1}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in column 'struct(col1, col3)' is not unique, found 2 duplicates",
                        "columns": ["col1", "col3"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                1,
                datetime(2025, 1, 2),
                "a",
                [
                    {
                        "name": "struct_col1_is_not_unique",
                        "message": "Value '{1}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in column 'struct(col1, col3)' is not unique, found 2 duplicates",
                        "columns": ["col1", "col3"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                None,
                None,
                "",
                [
                    {
                        "name": "struct_col1_is_not_unique",
                        "message": "Value '{null}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col2_is_not_unique",
                        "message": "Value '{null, null}' in column 'struct(col1, col2)' is not unique, found 2 duplicates",
                        "columns": ["col1", "col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{null, }' in column 'struct(col1, col3)' is not unique, found 2 duplicates",
                        "columns": ["col1", "col3"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "col1_is_not_unique_filter_col2_null",
                        "message": "Value '{null}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": "col2 is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                None,
                None,
                "",
                [
                    {
                        "name": "struct_col1_is_not_unique",
                        "message": "Value '{null}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col2_is_not_unique",
                        "message": "Value '{null, null}' in column 'struct(col1, col2)' is not unique, found 2 duplicates",
                        "columns": ["col1", "col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{null, }' in column 'struct(col1, col3)' is not unique, found 2 duplicates",
                        "columns": ["col1", "col3"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "col1_is_not_unique_filter_col2_null",
                        "message": "Value '{null}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": "col2 is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                2,
                None,
                "b",
                [
                    {
                        "name": "struct_col1_is_not_unique",
                        "message": "Value '{2}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col2_is_not_unique",
                        "message": "Value '{2, null}' in column 'struct(col1, col2)' is not unique, found 2 duplicates",
                        "columns": ["col1", "col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "col1_is_not_unique_filter_col2_null",
                        "message": "Value '{2}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": "col2 is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                2,
                None,
                "c",
                [
                    {
                        "name": "struct_col1_is_not_unique",
                        "message": "Value '{2}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col2_is_not_unique",
                        "message": "Value '{2, null}' in column 'struct(col1, col2)' is not unique, found 2 duplicates",
                        "columns": ["col1", "col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "col1_is_not_unique_filter_col2_null",
                        "message": "Value '{2}' in column 'struct(col1)' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": "col2 is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_all_row_checks_as_yaml_with_streaming(ws, make_schema, make_random, make_volume, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    input_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema_name)

    file_path = Path(__file__).parent.parent / "resources" / "all_row_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        checks = yaml.safe_load(f)

    dq_engine = DQEngine(ws)
    assert not dq_engine.validate_checks(checks).has_errors

    schema = (
        "col1: string, col2: int, col3: int, col4 array<int>, col5: date, col6: timestamp, "
        "col7: map<string, int>, col8: struct<field1: int>"
    )
    test_df = spark.createDataFrame(
        [
            [
                "val1",
                1,
                1,
                [1],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 1, 0, 0),
                {"key1": 1},
                {"field1": 1},
            ],
            [
                "val2",
                2,
                2,
                [2],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 2, 0, 0),
                {"key1": 1},
                {"field1": 1},
            ],
            [
                "val3",
                3,
                3,
                [3],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 3, 0, 0),
                {"key1": 1},
                {"field1": 1},
            ],
        ],
        schema,
    )
    test_df.write.saveAsTable(input_table_name)
    streaming_test_df = spark.readStream.table(input_table_name)

    streaming_checked_df = dq_engine.apply_checks_by_metadata(streaming_test_df, checks)

    dq_engine.save_results_in_table(
        output_df=streaming_checked_df,
        output_table=output_table_name,
        output_table_options={
            "checkpointLocation": f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{make_random(6).lower()}"
        },
        trigger={"availableNow": True},
        output_table_mode="overwrite",
    )

    checked_df = spark.table(output_table_name)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                "val1",
                1,
                1,
                [1],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 1, 0, 0),
                {"key1": 1},
                {"field1": 1},
                None,
                None,
            ],
            [
                "val2",
                2,
                2,
                [2],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 2, 0, 0),
                {"key1": 1},
                {"field1": 1},
                None,
                None,
            ],
            [
                "val3",
                3,
                3,
                [3],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 3, 0, 0),
                {"key1": 1},
                {"field1": 1},
                None,
                None,
            ],
        ],
        expected_schema,
    )

    assert_df_equality(checked_df, expected, ignore_nullable=True)


def test_apply_checks_all_checks_as_yaml(ws, spark):
    """Test applying all checks from a yaml file.

    The checks used in the test are also showcased in the docs under /docs/reference/quality_rules.mdx
    The checks should be kept up to date with the docs to make sure the documentation examples are validated.
    """
    file_path = Path(__file__).parent.parent / "resources" / "all_dataset_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        checks = yaml.safe_load(f)

    file_path = Path(__file__).parent.parent / "resources" / "all_row_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        checks.extend(yaml.safe_load(f))

    dq_engine = DQEngine(ws)
    status = dq_engine.validate_checks(checks)
    assert not status.has_errors

    schema = (
        "col1: string, col2: int, col3: int, col4 array<int>, col5: date, col6: timestamp, "
        "col7: map<string, int>, col8: struct<field1: int>"
    )
    test_df = spark.createDataFrame(
        [
            [
                "val1",
                1,
                1,
                [1],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 1, 0, 0),
                {"key1": 1},
                {"field1": 1},
            ],
            [
                "val2",
                2,
                2,
                [2],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 2, 0, 0),
                {"key1": 1},
                {"field1": 1},
            ],
            [
                "val3",
                3,
                3,
                [3],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 3, 0, 0),
                {"key1": 1},
                {"field1": 1},
            ],
        ],
        schema,
    )

    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                "val1",
                1,
                1,
                [1],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 1, 0, 0),
                {"key1": 1},
                {"field1": 1},
                None,
                None,
            ],
            [
                "val2",
                2,
                2,
                [2],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 2, 0, 0),
                {"key1": 1},
                {"field1": 1},
                None,
                None,
            ],
            [
                "val3",
                3,
                3,
                [3],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 3, 0, 0),
                {"key1": 1},
                {"field1": 1},
                None,
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_all_checks_using_classes(ws, spark):
    """Test applying all checks using DQX classes.

    The checks used in the test are also showcased in the docs under /docs/reference/quality_rules.mdx
    The checks should be kept up to date with the docs to make sure the documentation examples are validated.
    """
    checks = [
        # is_not_null check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="col1",
            user_metadata={"tag1": "value1", "tag2": "001"},
        ),
        # is_not_empty check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_empty,
            column="col1",
            user_metadata={"tag1": "value1", "tag2": "002"},
        ),
        # is_not_null_and_not_empty check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="col1",
            check_func_kwargs={"trim_strings": True},
            user_metadata={"tag1": "value1", "tag2": "003"},
        ),
        # is_in_list check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_in_list,
            column="col2",
            check_func_kwargs={"allowed": [1, 2, 3]},
            user_metadata={"tag1": "value1", "tag2": "004"},
        ),
        # is_not_null_and_is_in_list check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null_and_is_in_list,
            column="col2",
            check_func_kwargs={"allowed": [1, 2, 3]},
            user_metadata={"tag1": "value1", "tag2": "005"},
        ),
        # is_not_null_and_not_empty_array check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty_array,
            column="col4",
            user_metadata={"tag1": "value1", "tag2": "006"},
        ),
        # is_not_null_and_not_empty check, use args to pass arguments
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="col1",
            check_func_kwargs={"trim_strings": True},
            user_metadata={"tag1": "value1", "tag2": "007"},
        ),
        # is_in_range check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_in_range,
            column="col2",
            check_func_kwargs={"min_limit": 1, "max_limit": 10},
            user_metadata={"tag1": "value1", "tag2": "008"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_in_range,
            column="col5",
            check_func_kwargs={"min_limit": datetime(2025, 1, 1).date(), "max_limit": datetime(2025, 2, 24).date()},
            user_metadata={"tag1": "value1", "tag2": "009"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_in_range,
            column="col6",
            check_func_kwargs={"min_limit": datetime(2025, 1, 1, 0, 0, 0), "max_limit": datetime(2025, 2, 24, 1, 0, 0)},
            user_metadata={"tag1": "value1", "tag2": "010"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_in_range,
            column="col3",
            check_func_kwargs={"min_limit": "col2", "max_limit": "col2 * 2"},
            user_metadata={"tag1": "value2", "tag2": "011"},
        ),
        # is_not_in_range check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_in_range,
            column="col2",
            check_func_kwargs={"min_limit": 11, "max_limit": 20},
            user_metadata={"tag1": "value2", "tag2": "012"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_in_range,
            column="col5",
            check_func_kwargs={"min_limit": datetime(2025, 2, 25).date(), "max_limit": datetime(2025, 2, 26).date()},
            user_metadata={"tag1": "value2", "tag2": "013"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_in_range,
            column="col6",
            check_func_kwargs={
                "min_limit": datetime(2025, 2, 25, 0, 0, 0),
                "max_limit": datetime(2025, 2, 26, 1, 0, 0),
            },
            user_metadata={"tag1": "value2", "tag2": "014"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_in_range,
            column="col3",
            check_func_kwargs={"min_limit": "col2 + 10", "max_limit": "col2 * 10"},
            user_metadata={"tag1": "value2", "tag2": "015"},
        ),
        # is_not_less_than check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_less_than,
            column="col2",
            check_func_kwargs={"limit": 0},
            user_metadata={"tag1": "value2", "tag2": "016"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_less_than,
            column="col5",
            check_func_kwargs={"limit": datetime(2025, 1, 1).date()},
            user_metadata={"tag1": "value2", "tag2": "017"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_less_than,
            column="col6",
            check_func_kwargs={"limit": datetime(2025, 1, 1, 1, 0, 0)},
            user_metadata={"tag1": "value2", "tag2": "018"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_less_than,
            column="col3",
            check_func_kwargs={"limit": "col2 - 10"},
            user_metadata={"tag1": "value2", "tag2": "019"},
        ),
        # is_not_greater_than check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_greater_than,
            column="col2",
            check_func_kwargs={"limit": 10},
            user_metadata={"tag1": "value3", "tag2": "020"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_greater_than,
            column="col5",
            check_func_kwargs={"limit": datetime(2025, 3, 1).date()},
            user_metadata={"tag1": "value3", "tag2": "021"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_greater_than,
            column="col6",
            check_func_kwargs={"limit": datetime(2025, 3, 24, 1, 0, 0)},
            user_metadata={"tag1": "value3", "tag2": "022"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_greater_than,
            column="col3",
            check_func_kwargs={"limit": "col2 + 10"},
            user_metadata={"tag1": "value3", "tag2": "023"},
        ),
        # is_valid_date check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_date,
            column="col5",
            user_metadata={"tag1": "value3", "tag2": "024"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_date,
            column="col5",
            check_func_kwargs={"date_format": "yyyy-MM-dd"},
            name="col5_is_not_valid_date2",
            user_metadata={"tag1": "value3", "tag2": "025"},
        ),
        # is_valid_timestamp check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_timestamp,
            column="col6",
            user_metadata={"tag1": "value3", "tag2": "026"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_timestamp,
            column="col6",
            check_func_kwargs={"timestamp_format": "yyyy-MM-dd HH:mm:ss"},
            name="col6_is_not_valid_timestamp2",
            user_metadata={"tag1": "value3", "tag2": "027"},
        ),
        # is_not_in_future check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_in_future,
            column="col6",
            check_func_kwargs={"offset": 86400},
            user_metadata={"tag1": "value3", "tag2": "028"},
        ),
        # is_not_in_near_future check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_in_near_future,
            column="col6",
            check_func_kwargs={"offset": 36400},
            user_metadata={"tag1": "value3", "tag2": "029"},
        ),
        # is_older_than_n_days check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_older_than_n_days,
            column="col5",
            check_func_kwargs={"days": 10},
            user_metadata={"tag1": "value4"},
        ),
        # is_older_than_col2_for_n_days check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_older_than_col2_for_n_days,
            check_func_kwargs={"column1": "col5", "column2": "col6", "days": 2},
            user_metadata={"tag1": "value4"},
        ),
        # is_unique check
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_unique,
            columns=["col1"],  # this check require list of columns
            user_metadata={"tag1": "value4"},
        ),
        # is_unique check defined using list of columns
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_unique,
            columns=[F.col("col1")],  # this check require list of columns
            user_metadata={"tag1": "value4"},
        ),
        # is_unique on multiple columns (composite key), nulls are distinct (default behavior)
        # eg. (1, NULL) not equals (1, NULL) and (NULL, NULL) not equals (NULL, NULL)
        DQDatasetRule(
            criticality="error",
            name="composite_key_col1_and_col2_is_not_unique",
            check_func=check_funcs.is_unique,
            columns=["col1", "col2"],
            user_metadata={"tag1": "value4"},
        ),
        # is_unique on multiple columns (composite key), nulls are not distinct
        # eg. (1, NULL) equals (1, NULL) and (NULL, NULL) equals (NULL, NULL)
        DQDatasetRule(
            criticality="error",
            name="composite_key_col1_and_col2_is_not_unique_nulls_not_distinct",
            check_func=check_funcs.is_unique,
            columns=["col1", "col2"],
            check_func_kwargs={"nulls_distinct": False},
            user_metadata={"tag1": "value4"},
        ),
        # is_aggr_not_greater_than check with count aggregation over all rows
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            check_func_kwargs={"column": "*", "aggr_type": "count", "limit": 10},
        ),
        # is_aggr_not_greater_than check with aggregation over col2 (skip nulls)
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            check_func_kwargs={"column": "col2", "aggr_type": "count", "limit": 10},
        ),
        # is_aggr_not_greater_than check with aggregation over col2 grouped by col3 (skip nulls)
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            check_func_kwargs={"column": "col2", "aggr_type": "count", "group_by": ["col3"], "limit": 10},
        ),
        # is_aggr_not_less_than check with count aggregation over all rows
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_less_than,
            column="*",
            check_func_kwargs={"aggr_type": "count", "limit": 1},
        ),
        # is_aggr_not_less_than check with aggregation over col2 (skip nulls)
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_less_than,
            column="col2",
            check_func_kwargs={"aggr_type": "count", "limit": 1},
        ),
        # is_aggr_not_less_than check with aggregation over col2 grouped by col3 (skip nulls)
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_less_than,
            column="col2",
            check_func_kwargs={"aggr_type": "count", "group_by": ["col3"], "limit": 1},
        ),
        # regex_match check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.regex_match,
            column="col2",
            check_func_kwargs={"regex": "[0-9]+", "negate": False},
            user_metadata={"tag1": "value5"},
        ),
        # sql_expression check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.sql_expression,
            check_func_kwargs={
                "expression": "col3 >= col2 and col3 <= 10",
                "msg": "col3 is less than col2 and col3 is greater than 10",
                "name": "custom_output_name",
                "negate": False,
            },
            user_metadata={"tag1": "value5"},
        ),
    ]

    # apply check to multiple columns
    checks = (
        checks
        + DQForEachColRule(
            check_func=check_funcs.is_not_null,  # 'column' as first argument
            criticality="error",
            columns=["col3", "col5"],  # apply the check for each column in the list
            user_metadata={"tag1": "multi column"},
        ).get_rules()
        + DQForEachColRule(
            check_func=check_funcs.is_unique,  # 'columns' as first argument
            criticality="error",
            columns=[["col3", "col5"], ["col1"]],  # apply the check for each list of columns
            user_metadata={"tag1": "multi column"},
        ).get_rules()
    )

    checks = checks + [
        # is_not_null check applied to a struct column element (dot notation)
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="col8.field1",
        ),
        # is_not_null check applied to a map column element
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null,
            column=F.try_element_at("col7", F.lit("key1")),
        ),
        # is_not_null check applied to an array column element at the specified position
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_null,
            column=F.try_element_at("col4", F.lit(1)),
        ),
        # is_not_greater_than check applied to an array column
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_greater_than,
            column=F.array_max("col4"),
            check_func_kwargs={"limit": 10},
        ),
        # is_not_less_than check applied to an array column
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_less_than,
            column=F.array_min("col4"),
            check_func_kwargs={"limit": 1},
        ),
        # sql_expression check applied to a map column element
        DQRowRule(
            criticality="error",
            check_func=check_funcs.sql_expression,
            check_func_kwargs={
                "expression": "try_element_at(col7, 'key1') < 10",
                "msg": "col7 element 'key1' is less than 10",
                "name": "col7_element_key1_less_than_10",
                "negate": False,
            },
        ),
        # sql_expression check applied to an array of map column elements
        DQRowRule(
            criticality="error",
            check_func=check_funcs.sql_expression,
            check_func_kwargs={
                "expression": "not exists(col4, x -> x >= 10)",
                "msg": "array col4 has an element greater than 10",
                "name": "col4_all_elements_less_than_10",
                "negate": False,
            },
        ),
    ]

    # apply check to multiple columns (simple col, map and array)
    checks = (
        checks
        + DQForEachColRule(
            check_func=check_funcs.is_not_null,
            criticality="error",
            columns=[
                "col1",  # col as string
                F.col("col2"),  # col
                "col8.field1",  # struct col
                F.try_element_at("col7", F.lit("key1")),  # map col
                F.try_element_at("col4", F.lit(1)),  # array col
            ],
        ).get_rules()
    )

    dq_engine = DQEngine(ws)

    schema = (
        "col1: string, col2: int, col3: int, col4 array<int>, col5: date, col6: timestamp, "
        "col7: map<string, int>, col8: struct<field1: int>"
    )
    test_df = spark.createDataFrame(
        [
            [
                "val1",
                1,
                1,
                [1],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 1, 0, 0),
                {"key1": 1},
                {"field1": 1},
            ],
            [
                "val2",
                2,
                2,
                [2],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 2, 0, 0),
                {"key1": 1},
                {"field1": 1},
            ],
            [
                "val3",
                3,
                3,
                [3],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 3, 0, 0),
                {"key1": 1},
                {"field1": 1},
            ],
        ],
        schema,
    )

    checked = dq_engine.apply_checks(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                "val1",
                1,
                1,
                [1],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 1, 0, 0),
                {"key1": 1},
                {"field1": 1},
                None,
                None,
            ],
            [
                "val2",
                2,
                2,
                [2],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 2, 0, 0),
                {"key1": 1},
                {"field1": 1},
                None,
                None,
            ],
            [
                "val3",
                3,
                3,
                [3],
                datetime(2025, 1, 2).date(),
                datetime(2025, 1, 12, 3, 0, 0),
                {"key1": 1},
                {"field1": 1},
                None,
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_define_user_metadata_and_extract_dq_results(ws, spark):
    user_metadata = {"key1": "value1", "key2": "value2"}
    extra_params = ExtraParams(run_time=RUN_TIME, user_metadata=user_metadata)
    dq_engine = DQEngine(workspace_client=ws, extra_params=extra_params)
    test_df = spark.createDataFrame([[None, 1, 1]], SCHEMA)

    checks = [
        DQRowRule(
            name="a_is_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="a",
        ),
        DQRowRule(
            name="a_is_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="a",
            filter="b = 1",
        ),
        DQRowRule(
            name="a_is_null_or_empty",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="a",
        ),
        DQRowRule(
            name="a_is_null",
            criticality="warn",
            check_func=check_funcs.is_not_null,
            column="a",
            filter="b = 1",
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    result_errors = checked.select(F.explode(F.col("_errors")).alias("dq")).select(F.expr("dq.*"))
    result_warnings = checked.select(F.explode(F.col("_warnings")).alias("dq")).select(F.expr("dq.*"))

    expected = spark.createDataFrame(
        [
            [
                "a_is_null_or_empty",
                "Column 'a' value is null or empty",
                ["a"],
                None,
                "is_not_null_and_not_empty",
                RUN_TIME,
                user_metadata,
            ],
            [
                "a_is_null",
                "Column 'a' value is null",
                ["a"],
                "b = 1",
                "is_not_null",
                RUN_TIME,
                user_metadata,
            ],
        ],
        dq_result_schema.elementType,
    )

    assert_df_equality(result_errors, expected, ignore_nullable=True)
    assert_df_equality(result_warnings, expected, ignore_nullable=True)


def test_apply_checks_with_sql_expression_for_map_and_array(ws, spark):
    schema = "col1: map<string,int>, col2: array<map<string, int>>"
    test_df = spark.createDataFrame(
        [[{"key1": 10, "key2": 1}, [{"key1": 1, "key2": 2}, {"key1": 10, "key2": 20}]]], schema
    )

    checks = [
        {
            "criticality": "error",
            "name": "map_element_at_col1_key1_is_not_greater_than_10",
            "check": {"function": "sql_expression", "arguments": {"expression": "try_element_at(col1, 'key1') < 10"}},
        },
        {
            "criticality": "error",
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "not exists(col2, x -> x.key1 >= 10)"},
            },
        },
    ]

    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                {"key1": 10, "key2": 1},
                [{"key1": 1, "key2": 2}, {"key1": 10, "key2": 20}],
                [
                    {
                        "name": "map_element_at_col1_key1_is_not_greater_than_10",
                        "message": "Value is not matching expression: try_element_at(col1, 'key1') < 10",
                        "columns": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "not_exists_col2_x_x_key1_10",
                        "message": "Value is not matching expression: not exists(col2, x -> x.key1 >= 10)",
                        "columns": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_complex_types_by_metadata(ws, spark):
    schema = "col1: map<string,int>, col2: array<map<string, int>>"
    test_df = spark.createDataFrame(
        [
            [{"key1": 10, "key2": 1}, [{"key1": 1, "key2": 2}, {"key1": 10, "key2": 20}]],
            [{"key1": 1, "key2": 1}, [{"key1": 1, "key2": 2}, {"key1": 1, "key2": 20}]],
        ],
        schema,
    )

    checks = [
        {
            "criticality": "error",
            "name": "map_element_at_col1_key1_is_not_greater_than_5",
            "check": {
                "function": "is_not_greater_than",
                "arguments": {"column": "try_element_at(col1, 'key1')", "limit": 5},
            },
        },
        {
            "criticality": "error",
            "name": "array_element_at_position_2_key1_is_not_greater_than_5",
            "check": {
                "function": "is_not_greater_than",
                "arguments": {"column": "try_element_at(try_element_at(col2, 2), 'key1')", "limit": 5},
            },
        },
        {  # map key does not exist
            "criticality": "error",
            "name": "map_element_at_col1_not_exists_is_not_greater_than_5",
            "check": {
                "function": "is_not_greater_than",
                "arguments": {"column": "try_element_at(col1, 'not_exists')", "limit": 5},
            },
        },
        {  # element does not exist at the given position
            "criticality": "error",
            "name": "array_element_at_position_1000_key1_is_not_greater_than_5",
            "check": {
                "function": "is_not_greater_than",
                "arguments": {"column": "try_element_at(try_element_at(col2, 1000), 'key1')", "limit": 5},
            },
        },
    ]

    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                {"key1": 10, "key2": 1},
                [{"key1": 1, "key2": 2}, {"key1": 10, "key2": 20}],
                [
                    {
                        "name": "map_element_at_col1_key1_is_not_greater_than_5",
                        "message": "Value '10' in Column 'try_element_at(col1, 'key1')' is greater than limit: 5",
                        "columns": ["try_element_at(col1, 'key1')"],
                        "filter": None,
                        "function": "is_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "array_element_at_position_2_key1_is_not_greater_than_5",
                        "message": "Value '10' in Column 'try_element_at(try_element_at(col2, 2), 'key1')' is greater than limit: 5",
                        "columns": ["try_element_at(try_element_at(col2, 2), 'key1')"],
                        "filter": None,
                        "function": "is_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                {"key1": 1, "key2": 1},
                [{"key1": 1, "key2": 2}, {"key1": 1, "key2": 20}],
                None,
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_complex_types_using_classes(ws, spark):
    schema = "col1: map<string,int>, col2: array<map<string, int>>"
    test_df = spark.createDataFrame(
        [
            [{"key1": 10, "key2": 1}, [{"key1": 1, "key2": 2}, {"key1": 10, "key2": 20}]],
            [{"key1": 1, "key2": 1}, [{"key1": 1, "key2": 2}, {"key1": 1, "key2": 20}]],
        ],
        schema,
    )

    checks = [
        DQRowRule(
            criticality="error",
            name="map_element_at_col1_key1_is_not_greater_than_5",
            check_func=check_funcs.is_not_greater_than,
            column=F.try_element_at("col1", F.lit("key1")),
            check_func_kwargs={"limit": 5},
        ),
        DQRowRule(
            criticality="error",
            name="array_element_at_position_2_key1_is_not_greater_than_5",
            check_func=check_funcs.is_not_greater_than,
            column=F.try_element_at(F.try_element_at("col2", F.lit(2)), F.lit("key1")),
            check_func_kwargs={"limit": 5},
        ),
        DQRowRule(
            criticality="error",
            name="map_element_at_col1_not_exists_is_not_greater_than_5",
            check_func=check_funcs.is_not_greater_than,
            column=F.try_element_at("col1", F.lit("not_exists")),
            check_func_kwargs={"limit": 5},
        ),
        DQRowRule(
            criticality="error",
            name="array_element_at_position_1000_key1_is_not_greater_than_5",
            check_func=check_funcs.is_not_greater_than,
            column=F.try_element_at(F.try_element_at("col2", F.lit(1000)), F.lit("key1")),
            check_func_kwargs={"limit": 5},
        ),
    ]

    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    checked = dq_engine.apply_checks(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                {"key1": 10, "key2": 1},
                [{"key1": 1, "key2": 2}, {"key1": 10, "key2": 20}],
                [
                    {
                        "name": "map_element_at_col1_key1_is_not_greater_than_5",
                        "message": "Value '10' in Column 'try_element_at(col1, key1)' is greater than limit: 5",
                        "columns": ["try_element_at(col1, key1)"],
                        "filter": None,
                        "function": "is_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "array_element_at_position_2_key1_is_not_greater_than_5",
                        "message": "Value '10' in Column 'try_element_at(try_element_at(col2, 2), key1)' is greater than limit: 5",
                        "columns": ["try_element_at(try_element_at(col2, 2), key1)"],
                        "filter": None,
                        "function": "is_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                {"key1": 1, "key2": 1},
                [{"key1": 1, "key2": 2}, {"key1": 1, "key2": 20}],
                None,
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_with_check_and_engine_metadata_from_config(ws, spark):
    extra_params = ExtraParams(run_time=RUN_TIME, user_metadata={"tag2": "from_engine", "tag3": "from_engine"})
    dq_engine = DQEngine(workspace_client=ws, extra_params=extra_params)
    schema = "col1: string, col2: string"
    test_df = spark.createDataFrame([["str1", "str2"], [None, "val2"], ["val1", ""], [None, None]], schema)

    checks = [
        {
            "name": "col1_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
            "user_metadata": {"tag1": "value1", "tag2": "from_check"},
        },
        {
            "name": "col2_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "col2"}},
            "user_metadata": {"tag1": "value2"},
        },
    ]

    expected_schema = schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            ["str1", "str2", None, None],
            [
                None,
                "val2",
                [
                    {
                        "name": "col1_is_null",
                        "message": "Column 'col1' value is null",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "from_check", "tag3": "from_engine"},
                    }
                ],
                None,
            ],
            [
                "val1",
                "",
                [
                    {
                        "name": "col2_is_null",
                        "message": "Column 'col2' value is null or empty",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value2", "tag2": "from_engine", "tag3": "from_engine"},
                    }
                ],
                None,
            ],
            [
                None,
                None,
                [
                    {
                        "name": "col1_is_null",
                        "message": "Column 'col1' value is null",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "from_check", "tag3": "from_engine"},
                    },
                    {
                        "name": "col2_is_null",
                        "message": "Column 'col2' value is null or empty",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value2", "tag2": "from_engine", "tag3": "from_engine"},
                    },
                ],
                None,
            ],
        ],
        expected_schema,
    )

    actual_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    assert_df_equality(actual_df, expected_df)


def test_apply_checks_with_check_and_engine_metadata_from_classes(ws, spark):
    extra_params = ExtraParams(run_time=RUN_TIME, user_metadata={"tag2": "from_engine", "tag3": "from_engine"})
    dq_engine = DQEngine(workspace_client=ws, extra_params=extra_params)
    schema = "col1: string, col2: string"
    test_df = spark.createDataFrame([["str1", "str2"], [None, "val2"], ["val1", ""], [None, None]], schema)

    checks = [
        DQRowRule(
            name="col1_is_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="col1",
            user_metadata={"tag1": "value1", "tag2": "from_check"},
        ),
        DQRowRule(
            name="col2_is_null",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="col2",
            user_metadata={"tag1": "value2"},
        ),
    ]

    expected_schema = schema + REPORTING_COLUMNS
    expected_df = spark.createDataFrame(
        [
            ["str1", "str2", None, None],
            [
                None,
                "val2",
                [
                    {
                        "name": "col1_is_null",
                        "message": "Column 'col1' value is null",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "from_check", "tag3": "from_engine"},
                    }
                ],
                None,
            ],
            [
                "val1",
                "",
                [
                    {
                        "name": "col2_is_null",
                        "message": "Column 'col2' value is null or empty",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value2", "tag2": "from_engine", "tag3": "from_engine"},
                    }
                ],
                None,
            ],
            [
                None,
                None,
                [
                    {
                        "name": "col1_is_null",
                        "message": "Column 'col1' value is null",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "from_check", "tag3": "from_engine"},
                    },
                    {
                        "name": "col2_is_null",
                        "message": "Column 'col2' value is null or empty",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value2", "tag2": "from_engine", "tag3": "from_engine"},
                    },
                ],
                None,
            ],
        ],
        expected_schema,
    )

    actual_df = dq_engine.apply_checks(test_df, checks)
    assert_df_equality(actual_df, expected_df)


def test_apply_aggr_checks(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[None, None, None], [1, None, 5], [1, 2, 3]], SCHEMA)

    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="*",  # count all rows
            check_func_kwargs={"aggr_type": "count", "limit": 0},
        ),
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="a",  # count over column a, don't count rows where a is null
            check_func_kwargs={"aggr_type": "count", "limit": 0},
        ),
        DQDatasetRule(
            name="a_count_greater_than_limit_with_b_not_null",
            criticality="warn",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="a",
            filter="b is not null",  # apply filter
            check_func_kwargs={"aggr_type": "count", "limit": 0},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="a",
            check_func_kwargs={"group_by": ["a"], "limit": 0, "aggr_type": "count"},
        ),
        DQDatasetRule(
            name="a_count_group_by_a_greater_than_limit_with_b_not_null",
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="a",
            filter="b is not null",
            check_func_kwargs={"group_by": ["a"], "limit": 0, "aggr_type": "count"},
        ),
        DQDatasetRule(
            name="row_count_group_by_a_b_greater_than_limit",
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="a",
            check_func_kwargs={"group_by": ["a", "b"], "limit": 0, "aggr_type": "count"},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="c",
            check_func_kwargs={"limit": 0, "aggr_type": "avg"},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="c",
            check_func_kwargs={"group_by": ["a"], "limit": 0, "aggr_type": "avg"},
        ),
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.is_aggr_not_less_than,
            column="*",  # count all rows
            check_func_kwargs={"aggr_type": "count", "limit": 10},
        ),
        DQDatasetRule(
            name="a_count_group_by_a_less_than_limit_with_b_not_null",
            criticality="error",
            check_func=check_funcs.is_aggr_not_less_than,
            column="a",
            filter="b is not null",
            check_func_kwargs={"group_by": ["a"], "limit": 10, "aggr_type": "count"},
        ),
    ]

    all_df = dq_engine.apply_checks(test_df, checks)

    expected_df = spark.createDataFrame(
        [
            [
                None,
                None,
                None,
                [
                    {
                        "name": "c_avg_greater_than_limit",
                        "message": "Avg 4.0 in column 'c' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "count_greater_than_limit",
                        "message": "Count 3 in column '*' is greater than limit: 0",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_greater_than_limit",
                        "message": "Count 2 in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "count_less_than_limit",
                        "message": "Count 3 in column '*' is less than limit: 10",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_less_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                1,
                None,
                5,
                [
                    {
                        "name": "a_count_group_by_a_greater_than_limit",
                        "message": "Count 2 per group of columns 'a' in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "row_count_group_by_a_b_greater_than_limit",
                        "message": "Count 1 per group of columns 'a, b' in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_greater_than_limit",
                        "message": "Avg 4.0 in column 'c' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_group_by_a_greater_than_limit",
                        "message": "Avg 4.0 per group of columns 'a' in column 'c' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "count_greater_than_limit",
                        "message": "Count 3 in column '*' is greater than limit: 0",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_greater_than_limit",
                        "message": "Count 2 in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "count_less_than_limit",
                        "message": "Count 3 in column '*' is less than limit: 10",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_less_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                1,
                2,
                3,
                [
                    {
                        "name": "a_count_group_by_a_greater_than_limit",
                        "message": "Count 2 per group of columns 'a' in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_group_by_a_greater_than_limit_with_b_not_null",
                        "message": "Count 1 per group of columns 'a' in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": "b is not null",
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "row_count_group_by_a_b_greater_than_limit",
                        "message": "Count 1 per group of columns 'a, b' in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_greater_than_limit",
                        "message": "Avg 4.0 in column 'c' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_group_by_a_greater_than_limit",
                        "message": "Avg 4.0 per group of columns 'a' in column 'c' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_group_by_a_less_than_limit_with_b_not_null",
                        "message": "Count 1 per group of columns 'a' in column 'a' is less than limit: 10",
                        "columns": ["a"],
                        "filter": "b is not null",
                        "function": "is_aggr_not_less_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "count_greater_than_limit",
                        "message": "Count 3 in column '*' is greater than limit: 0",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_greater_than_limit",
                        "message": "Count 2 in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_greater_than_limit_with_b_not_null",
                        "message": "Count 1 in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": "b is not null",
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "count_less_than_limit",
                        "message": "Count 3 in column '*' is less than limit: 10",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_less_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(all_df, expected_df)


def test_apply_aggr_checks_by_metadata(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[None, None, None], [1, None, 5], [1, 2, 3]], SCHEMA)

    checks = [
        {
            "criticality": "warn",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"column": "*", "aggr_type": "count", "limit": 0},
            },
        },
        {
            "criticality": "warn",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"column": "a", "aggr_type": "count", "limit": 0},
            },
        },
        {
            "name": "a_count_greater_than_limit_with_b_not_null",
            "criticality": "warn",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"column": "a", "aggr_type": "count", "limit": 0},
            },
            "filter": "b is not null",
        },
        {
            "criticality": "error",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"column": "a", "group_by": ["a"], "limit": 0, "aggr_type": "count"},
            },
        },
        {
            "name": "a_count_group_by_a_greater_than_limit_with_b_not_null",
            "criticality": "error",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"column": "a", "group_by": ["a"], "limit": 0, "aggr_type": "count"},
            },
            "filter": "b is not null",
        },
        {
            "name": "row_count_group_by_a_b_greater_than_limit",
            "criticality": "error",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"column": "a", "group_by": ["a", "b"], "limit": 0, "aggr_type": "count"},
            },
        },
        {
            "criticality": "error",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"column": "c", "limit": 0, "aggr_type": "avg"},
            },
        },
        {
            "criticality": "error",
            "check": {
                "function": "is_aggr_not_greater_than",
                "arguments": {"column": "c", "group_by": ["a"], "limit": 0, "aggr_type": "avg"},
            },
        },
        {
            "criticality": "warn",
            "check": {
                "function": "is_aggr_not_less_than",
                "arguments": {"column": "*", "aggr_type": "count", "limit": 10},
            },
        },
        {
            "name": "a_count_group_by_a_less_than_limit_with_b_not_null",
            "criticality": "error",
            "check": {
                "function": "is_aggr_not_less_than",
                "arguments": {"column": "a", "group_by": ["a"], "limit": 10, "aggr_type": "count"},
            },
            "filter": "b is not null",
        },
    ]

    all_df = dq_engine.apply_checks_by_metadata(test_df, checks)

    expected_df = spark.createDataFrame(
        [
            [
                None,
                None,
                None,
                [
                    {
                        "name": "c_avg_greater_than_limit",
                        "message": "Avg 4.0 in column 'c' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "count_greater_than_limit",
                        "message": "Count 3 in column '*' is greater than limit: 0",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_greater_than_limit",
                        "message": "Count 2 in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "count_less_than_limit",
                        "message": "Count 3 in column '*' is less than limit: 10",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_less_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                1,
                None,
                5,
                [
                    {
                        "name": "a_count_group_by_a_greater_than_limit",
                        "message": "Count 2 per group of columns 'a' in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "row_count_group_by_a_b_greater_than_limit",
                        "message": "Count 1 per group of columns 'a, b' in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_greater_than_limit",
                        "message": "Avg 4.0 in column 'c' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_group_by_a_greater_than_limit",
                        "message": "Avg 4.0 per group of columns 'a' in column 'c' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "count_greater_than_limit",
                        "message": "Count 3 in column '*' is greater than limit: 0",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_greater_than_limit",
                        "message": "Count 2 in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "count_less_than_limit",
                        "message": "Count 3 in column '*' is less than limit: 10",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_less_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                1,
                2,
                3,
                [
                    {
                        "name": "a_count_group_by_a_greater_than_limit",
                        "message": "Count 2 per group of columns 'a' in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_group_by_a_greater_than_limit_with_b_not_null",
                        "message": "Count 1 per group of columns 'a' in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": "b is not null",
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "row_count_group_by_a_b_greater_than_limit",
                        "message": "Count 1 per group of columns 'a, b' in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_greater_than_limit",
                        "message": "Avg 4.0 in column 'c' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_group_by_a_greater_than_limit",
                        "message": "Avg 4.0 per group of columns 'a' in column 'c' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_group_by_a_less_than_limit_with_b_not_null",
                        "message": "Count 1 per group of columns 'a' in column 'a' is less than limit: 10",
                        "columns": ["a"],
                        "filter": "b is not null",
                        "function": "is_aggr_not_less_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "count_greater_than_limit",
                        "message": "Count 3 in column '*' is greater than limit: 0",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_greater_than_limit",
                        "message": "Count 2 in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_greater_than_limit_with_b_not_null",
                        "message": "Count 1 in column 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": "b is not null",
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "count_less_than_limit",
                        "message": "Count 3 in column '*' is less than limit: 10",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_less_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(all_df, expected_df)
