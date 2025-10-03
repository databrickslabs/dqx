import json
import uuid
from datetime import datetime
from pathlib import Path
from collections.abc import Callable
import yaml
import pyspark.sql.functions as F
import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from chispa.dataframe_comparer import assert_df_equality  # type: ignore

from databricks.labs.dqx.errors import MissingParameterError, InvalidCheckError, InvalidParameterError
from databricks.labs.dqx.check_funcs import sql_query
from databricks.labs.dqx.config import OutputConfig, FileChecksStorageConfig, ExtraParams, RunConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import (
    DQForEachColRule,
    ColumnArguments,
    register_rule,
    DQRowRule,
    DQDatasetRule,
)
from databricks.labs.dqx.schema import dq_result_schema
from databricks.labs.dqx import check_funcs
import databricks.labs.dqx.geo.check_funcs as geo_check_funcs
from tests.integration.conftest import REPORTING_COLUMNS, RUN_TIME, EXTRA_PARAMS


SCHEMA = "a: int, b: int, c: int"
EXPECTED_SCHEMA = SCHEMA + REPORTING_COLUMNS
EXPECTED_SCHEMA_WITH_CUSTOM_NAMES = (
    SCHEMA + f", dq_errors: {dq_result_schema.simpleString()}, dq_warnings: {dq_result_schema.simpleString()}"
)


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
            [1, 1, 3],
            [1, 1, 3],
            [5, 5, 7],
        ],
        SCHEMA,
    )
    ref_column = "b"

    checks = [
        DQDatasetRule(
            name="a_has_no_foreign_key",
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": [ref_column],
                "ref_df_name": "ref_df",
            },
            user_metadata={"tag1": "value1", "tag2": "value2"},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.foreign_key,
            columns=[F.col("a")],
            filter="a > 4",
            check_func_kwargs={
                "ref_columns": [F.col(ref_column)],
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
                        "message": "Value '4' in column 'a' not found in reference column 'b'",
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
                        "name": "a_not_exists_in_ref_b",
                        "message": "Value '6' in column 'a' not found in reference column 'b'",
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
                        "message": "Value '6' in column 'a' not found in reference column 'b'",
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


def test_foreign_key_check_negate(ws, spark):
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
            [1, 1, 3],
            [1, 1, 3],
            [5, 6, 7],
        ],
        SCHEMA,
    )
    ref_column = "b"

    checks = [
        DQDatasetRule(
            name="a_has_foreign_key",
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=["a"],
            check_func_kwargs={"ref_columns": [ref_column], "ref_df_name": "ref_df", "negate": True},
            user_metadata={"tag1": "value1", "tag2": "value2"},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.foreign_key,
            columns=[F.col("a")],
            filter="a > 4",
            check_func_kwargs={"ref_columns": [F.col(ref_column)], "ref_df_name": "ref_df", "negate": True},
        ),
    ]

    refs_df = {"ref_df": ref_df}

    checked = dq_engine.apply_checks(src_df, checks, refs_df)
    good_df, bad_df = dq_engine.apply_checks_and_split(src_df, checks, refs_df)

    expected = spark.createDataFrame(
        [
            [
                1,
                2,
                3,
                None,
                [
                    {
                        "name": "a_has_foreign_key",
                        "message": "Value '1' in column 'a' found in reference column 'b'",
                        "columns": ["a"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "value2"},
                    }
                ],
            ],
            [
                1,
                2,
                3,
                None,
                [
                    {
                        "name": "a_has_foreign_key",
                        "message": "Value '1' in column 'a' found in reference column 'b'",
                        "columns": ["a"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1", "tag2": "value2"},
                    }
                ],
            ],
            [
                4,
                5,
                6,
                None,
                None,
            ],
            [
                6,
                6,
                7,
                [
                    {
                        "name": "a_exists_in_ref_b",
                        "message": "Value '6' in column 'a' found in reference column 'b'",
                        "columns": ["a"],
                        "filter": "a > 4",
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_has_foreign_key",
                        "message": "Value '6' in column 'a' found in reference column 'b'",
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


def test_foreign_key_check_on_composite_keys(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    src_df = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [4, 5, 6],
            [6, None, 7],
            [None, None, None],
        ],
        SCHEMA,
    )

    ref_df = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [4, 5, 6],
            [6, 5, 7],
        ],
        "ref_a: int, ref_b: int, e: int",  # use different names than in the source intentionally
    )

    ref_df2 = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
        ],
        "ref_a: int, ref_b: int, e: int",  # use different names than in the source intentionally
    )

    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.foreign_key,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": [F.col("ref_a")],
                "ref_df_name": "ref_df",
            },
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.foreign_key,
            columns=[F.col("a"), F.col("b")],
            check_func_kwargs={
                "ref_columns": [F.col("ref_a"), F.col("ref_b")],
                "ref_df_name": "ref_df2",
            },
        ),
    ]

    checks_yaml = yaml.safe_load(
        """
        - criticality: error
          check:
            function: foreign_key
            arguments:
              columns:
              - a
              ref_columns:
              - ref_a
              ref_df_name: ref_df
        - criticality: error
          check:
            function: foreign_key
            arguments:
              columns:
              - a
              - b
              ref_columns:
              - ref_a
              - ref_b
              ref_df_name: ref_df2
        """
    )

    refs_df = {"ref_df": ref_df, "ref_df2": ref_df2}

    checked = dq_engine.apply_checks(src_df, checks, ref_dfs=refs_df)
    checked_yaml = dq_engine.apply_checks_by_metadata(src_df, checks_yaml, ref_dfs=refs_df)

    expected = spark.createDataFrame(
        [
            [1, 2, 3, None, None],
            [1, 2, 3, None, None],
            [
                4,
                5,
                6,
                [
                    {
                        "name": "struct_a_as_a_b_as_b_not_exists_in_ref_struct_ref_a_as_a_ref_b_as_b",
                        "message": "Value '{4, 5}' in column 'struct(a AS a, b AS b)' not found in reference column 'struct(ref_a AS a, ref_b AS b)'",
                        "columns": ["a", "b"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                6,
                None,
                7,
                None,
                None,
            ],
            [None, None, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)
    assert_df_equality(checked_yaml, expected, ignore_nullable=True)


def test_foreign_key_check_on_composite_keys_negate(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    src_df = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [4, 5, 6],
            [6, None, 7],
            [None, None, None],
        ],
        SCHEMA,
    )

    ref_df = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [4, 5, 6],
            [6, 5, 7],
        ],
        "ref_a: int, ref_b: int, e: int",  # use different names than in the source intentionally
    )

    ref_df2 = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
        ],
        "ref_a: int, ref_b: int, e: int",  # use different names than in the source intentionally
    )

    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.foreign_key,
            columns=[F.col("a"), F.col("b")],
            check_func_kwargs={
                "ref_columns": [F.col("ref_a"), F.col("ref_b")],
                "ref_df_name": "ref_df2",
                "negate": True,
            },
        ),
    ]

    checks_yaml = yaml.safe_load(
        """
        - criticality: error
          check:
            function: foreign_key
            arguments:
              columns:
              - a
              - b
              ref_columns:
              - ref_a
              - ref_b
              ref_df_name: ref_df2
              negate: true
        """
    )

    refs_df = {"ref_df": ref_df, "ref_df2": ref_df2}

    checked = dq_engine.apply_checks(src_df, checks, ref_dfs=refs_df)
    checked_yaml = dq_engine.apply_checks_by_metadata(src_df, checks_yaml, ref_dfs=refs_df)

    expected = spark.createDataFrame(
        [
            [
                1,
                2,
                3,
                [
                    {
                        "name": "struct_a_as_a_b_as_b_exists_in_ref_struct_ref_a_as_a_ref_b_as_b",
                        "message": "Value '{1, 2}' in column 'struct(a AS a, b AS b)' found in reference column 'struct(ref_a AS a, ref_b AS b)'",
                        "columns": ["a", "b"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                1,
                2,
                3,
                [
                    {
                        "name": "struct_a_as_a_b_as_b_exists_in_ref_struct_ref_a_as_a_ref_b_as_b",
                        "message": "Value '{1, 2}' in column 'struct(a AS a, b AS b)' found in reference column 'struct(ref_a AS a, ref_b AS b)'",
                        "columns": ["a", "b"],
                        "filter": None,
                        "function": "foreign_key",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [
                4,
                5,
                6,
                None,
                None,
            ],
            [
                6,
                None,
                7,
                None,
                None,
            ],
            [None, None, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)
    assert_df_equality(checked_yaml, expected, ignore_nullable=True)


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
              columns:
              - a
              ref_columns:
              - {ref_column}
              ref_df_name: ref_df
          user_metadata:
            tag1: value1
            tag2: value2
        - criticality: error
          filter: a > 4
          check:
            function: foreign_key
            arguments:
              columns:
              - a
              ref_columns:
              - {ref_column}
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
                        "message": "Value '4' in column 'a' not found in reference column 'a'",
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
                        "name": "a_not_exists_in_ref_a",
                        "message": "Value '6' in column 'a' not found in reference column 'a'",
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
                        "message": "Value '6' in column 'a' not found in reference column 'a'",
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


def test_foreign_key_check_on_tables(ws, spark, make_schema, make_random):
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

    catalog_name = "main"
    schema = make_schema(catalog_name=catalog_name)
    ref_table = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"
    ref_df.write.saveAsTable(ref_table)

    ref_df2 = spark.createDataFrame(
        [
            [1, 2, 3],
            [1, 2, 3],
            [4, 5, 6],
            [6, 6, 7],
        ],
        SCHEMA,
    )

    ref_table2 = f"{catalog_name}.{schema.name}.{make_random(10).lower()}"
    ref_df2.write.saveAsTable(ref_table2)

    checks = [
        DQDatasetRule(
            name="a_has_no_foreign_key",
            criticality="warn",
            check_func=check_funcs.foreign_key,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_table": ref_table,
            },
            user_metadata={"tag1": "value1", "tag2": "value2"},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.foreign_key,
            columns=[F.col("a")],
            filter="a > 4",
            check_func_kwargs={
                "ref_columns": [F.col("a")],
                "ref_table": ref_table,
            },
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.foreign_key,
            columns=["a", "b"],
            check_func_kwargs={
                "ref_columns": ["a", "b"],
                "ref_table": ref_table2,
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
                        "message": "Value '4' in column 'a' not found in reference column 'a'",
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
                        "name": "a_not_exists_in_ref_a",
                        "message": "Value '6' in column 'a' not found in reference column 'a'",
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
                        "message": "Value '6' in column 'a' not found in reference column 'a'",
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
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_df_name": "ref_df",
            },
        ),
    ]

    refs_df = {}
    with pytest.raises(
        MissingParameterError,
        match="Reference DataFrame with key 'ref_df' not found. Provide reference 'ref_df' DataFrame when applying the checks.",
    ):
        dq_engine.apply_checks(src_df, checks, refs_df)


def test_foreign_key_check_null_ref_df(ws, spark):
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
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_df_name": "ref_df",
            },
        ),
    ]

    with pytest.raises(MissingParameterError, match="Reference DataFrames dictionary not provided"):
        dq_engine.apply_checks(src_df, checks)


def test_foreign_key_check_missing_ref_df_key(ws, spark):
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
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_df_name": "ref_df_key",
            },
        ),
    ]

    ref_dfs = {"ref_df_different_key": src_df}

    with pytest.raises(MissingParameterError, match="Reference DataFrame with key 'ref_df_key' not found"):
        dq_engine.apply_checks(src_df, checks, ref_dfs=ref_dfs)


def test_compare_datasets_check_missing_ref_df(ws, spark):
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
            check_func=check_funcs.compare_datasets,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_df_name": "ref_df",
            },
        ),
    ]

    refs_df = {}
    with pytest.raises(MissingParameterError, match="Reference DataFrame with key 'ref_df' not found"):
        dq_engine.apply_checks(src_df, checks, refs_df)


def test_compare_datasets_check_null_ref_df(ws, spark):
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
            check_func=check_funcs.compare_datasets,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_df_name": "ref_df",
            },
        ),
    ]

    with pytest.raises(MissingParameterError, match="Reference DataFrames dictionary not provided"):
        dq_engine.apply_checks(src_df, checks)


def test_compare_datasets_check_missing_ref_df_key(ws, spark):
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
            check_func=check_funcs.compare_datasets,
            columns=["a"],
            check_func_kwargs={
                "ref_columns": ["a"],
                "ref_df_name": "ref_df_key",
            },
        ),
    ]

    ref_dfs = {"ref_df_different_key": src_df}

    with pytest.raises(MissingParameterError, match="Reference DataFrame with key 'ref_df_key' not found"):
        dq_engine.apply_checks(src_df, checks, ref_dfs=ref_dfs)


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
            # alternative way of defining columns
            check_func_kwargs={"columns": ["a"], "nulls_distinct": True},
        ),
    ]
    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
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
            [1, 2, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_compare_datasets_with_tolerance(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    schema = "id int, value double"
    # Source DataFrame: has values near, just within, and just outside tolerances
    src_df = spark.createDataFrame(
        [
            [1, 100.00],  # equal under zero tolerance
            [2, 100.99],  # equal under abs_tolerance=1 (diff = 0.99)
            [3, 101.01],  # not equal under abs_tolerance=1 (diff = 1.01)
            [4, 202.0],  # equal under rel_tolerance=0.01 (diff = 2, tolerance = 2.02)
            [5, 204.5],  # not equal under rel_tolerance=0.01 (diff = 4.5, tolerance = 2.0)
            [6, None],  # Null comparison
            [7, None],  # Null comparison
        ],
        schema,
    )

    # Reference DataFrame
    ref_df = spark.createDataFrame(
        [
            [1, 100.00],
            [2, 100.00],
            [3, 100.0],
            [4, 200.0],
            [5, 200.0],
            [6, 100.00],
            [7, None],
        ],
        schema,
    )

    pk_columns = ["id"]

    # Add check with both tolerances
    checks = [
        DQDatasetRule(
            name="id_compare_with_tolerance",
            criticality="error",
            check_func=check_funcs.compare_datasets,
            columns=pk_columns,
            check_func_kwargs={
                "ref_columns": pk_columns,
                "ref_df_name": "ref_df",
                "abs_tolerance": 1.0,  # absolute tolerance of 1
                "rel_tolerance": 0.01,  # relative tolerance of 1%
                "null_safe_column_value_matching": True,
            },
            user_metadata={"test": "tolerance"},
        ),
    ]

    refs_df = {"ref_df": ref_df}

    checked = dq_engine.apply_checks(src_df, checks, refs_df)

    # Build expected results: rows only get flagged when outside of both tolerances
    expected = spark.createDataFrame(
        [
            [1, 100.00, None, None],  # exact match, no error/warning
            [2, 100.99, None, None],  # diff = 0.99 <= abs_tolerance=1.0, so no error
            [3, 101.01, None, None],  # diff = 1.01 <= (1.0 + 0.01*100 = 2.0), so no error],
            [4, 202.00, None, None],  # diff = 2.0, rel_tolerance = 2.02, so within relative tolerance
            [
                5,
                204.50,
                [
                    {
                        "name": "id_compare_with_tolerance",
                        "message": '{"row_missing":false,"row_extra":false,"changed":{"value":{"df":"204.5","ref":"200.0"}}}',
                        "columns": pk_columns,
                        "filter": None,
                        "function": "compare_datasets",
                        "run_time": RUN_TIME,
                        "user_metadata": {"test": "tolerance"},
                    }
                ],
                None,
            ],
            [
                6,
                None,
                [
                    {
                        "name": "id_compare_with_tolerance",
                        "message": '{"row_missing":false,"row_extra":false,"changed":{"value":{"ref":"100.0"}}}',
                        "columns": pk_columns,
                        "filter": None,
                        "function": "compare_datasets",
                        "run_time": RUN_TIME,
                        "user_metadata": {"test": "tolerance"},
                    }
                ],
                None,
            ],
            [7, None, None, None],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(checked.sort(pk_columns), expected.sort(pk_columns), ignore_nullable=True)


def test_compare_datasets_with_tolerance_with_disabled_null_safe_column_value_matching(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    schema = "id int, value double"
    # Source DataFrame: has values near, just within, and just outside tolerances
    src_df = spark.createDataFrame(
        [
            [1, 100.00],  # equal under zero tolerance
            [2, 100.99],  # equal under abs_tolerance=1 (diff = 0.99)
            [3, 101.01],  # not equal under abs_tolerance=1 (diff = 1.01)
            [4, 202.0],  # equal under rel_tolerance=0.01 (diff = 2, tolerance = 2.02)
            [5, 204.5],  # not equal under rel_tolerance=0.01 (diff = 4.5, tolerance = 2.0)
            [6, None],  # Null comparison
            [7, None],  # Null comparison
        ],
        schema,
    )

    # Reference DataFrame
    ref_df = spark.createDataFrame(
        [
            [1, 100.00],
            [2, 100.00],
            [3, 100.0],
            [4, 200.0],
            [5, 200.0],
            [6, 100.00],
            [7, None],
        ],
        schema,
    )

    pk_columns = ["id"]

    # Add check with both tolerances
    checks = [
        DQDatasetRule(
            name="id_compare_with_tolerance",
            criticality="error",
            check_func=check_funcs.compare_datasets,
            columns=pk_columns,
            check_func_kwargs={
                "ref_columns": pk_columns,
                "ref_df_name": "ref_df",
                "abs_tolerance": 1.0,  # absolute tolerance of 1
                "rel_tolerance": 0.01,  # relative tolerance of 1%
                "null_safe_column_value_matching": False,
            },
            user_metadata={"test": "tolerance"},
        ),
    ]

    refs_df = {"ref_df": ref_df}

    checked = dq_engine.apply_checks(src_df, checks, refs_df)

    # Build expected results: rows only get flagged when outside of both tolerances
    expected = spark.createDataFrame(
        [
            [1, 100.00, None, None],  # exact match, no error/warning
            [2, 100.99, None, None],  # diff = 0.99 <= abs_tolerance=1.0, so no error
            [3, 101.01, None, None],  # diff = 1.01 <= (1.0 + 0.01*100 = 2.0), so no error],
            [4, 202.00, None, None],  # diff = 2.0, rel_tolerance = 2.02, so within relative tolerance
            [
                5,
                204.50,
                [
                    {
                        "name": "id_compare_with_tolerance",
                        "message": '{"row_missing":false,"row_extra":false,"changed":{"value":{"df":"204.5","ref":"200.0"}}}',
                        "columns": pk_columns,
                        "filter": None,
                        "function": "compare_datasets",
                        "run_time": RUN_TIME,
                        "user_metadata": {"test": "tolerance"},
                    }
                ],
                None,
            ],
            [6, None, None, None],  # Nulls, should be considered equal if null_safe is disabled
            [7, None, None, None],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(checked.sort(pk_columns), expected.sort(pk_columns), ignore_nullable=True)


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
            check_func_kwargs={"column": "c"},  # alternative way of defining column
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


def test_create_checks_using_yaml_invalid_criticality(ws, spark):
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

    with pytest.raises(InvalidCheckError, match="Invalid 'criticality' value"):
        dq_engine.apply_checks_by_metadata(test_df, checks)


def test_create_checks_using_classes_invalid_criticality():
    with pytest.raises(InvalidCheckError, match="Invalid 'criticality' value"):
        DQRowRule(
            name="c_is_null_or_empty",
            criticality="invalid",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="c",
        )


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
        *DQForEachColRule(
            # missing criticality, default to "error"
            check_func=check_funcs.is_not_null,
            columns=["col3"],
        ).get_rules(),
    ]

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

    checks = [
        *DQForEachColRule(
            check_func=check_funcs.is_not_null_and_not_empty, criticality="warn", filter="b>3", columns=["a", "c"]
        ).get_rules(),
        DQRowRule(
            name="b_is_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="b",
            filter="a<3",
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
            name="common_name3",
            check_func=check_funcs.is_aggr_not_less_than,
            criticality="warn",
            columns=["a", "a"],
            check_func_kwargs={"limit": 0},
        ).get_rules()
        + DQForEachColRule(
            name="common_name4",
            check_func=check_funcs.is_aggr_not_greater_than,
            criticality="warn",
            columns=["a", "a"],
            check_func_kwargs={"limit": 10},
        ).get_rules()
        + DQForEachColRule(
            name="foreign_key_check",
            check_func=check_funcs.foreign_key,
            criticality="warn",
            columns=[["a"], ["a"]],
            check_func_kwargs={"ref_columns": ["ref_a"], "ref_df_name": "ref_df"},
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
                        "message": "Value 'null' in column 'c' is not unique, found 2 duplicates",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "common_name2",
                        "message": "Value 'null' in column 'c' is not unique, found 2 duplicates",
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
                        "message": "Value 'null' in column 'c' is not unique, found 2 duplicates",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "common_name2",
                        "message": "Value 'null' in column 'c' is not unique, found 2 duplicates",
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
    checks = dq_engine.load_checks(config=FileChecksStorageConfig(location=check_file))

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
    checks = dq_engine.load_checks(config=FileChecksStorageConfig(location=check_file))

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


def custom_dataset_check_func(column: str, group_by: str) -> tuple[Column, Callable]:
    condition_col = "condition" + uuid.uuid4().hex

    def closure(df: DataFrame) -> DataFrame:
        # Aggregate per group
        aggr_df = df.groupBy(group_by).agg((F.sum(column) > 1).alias(condition_col))

        # Join back to input DataFrame
        result_df = df.join(aggr_df, on=group_by, how="left")

        return result_df

    return (
        check_funcs.make_condition(
            condition=F.col(condition_col),  # check condition returns true
            message="dataset check failed",
            alias=f"{column}_custom_dataset_check",
        ),
        closure,
    )


@register_rule("dataset")
def custom_dataset_check_func_with_ref_dfs(column: str) -> tuple[Column, Callable]:
    condition_col = "condition" + uuid.uuid4().hex

    def closure(df: DataFrame, spark: SparkSession, _ref_dfs: dict[str, DataFrame] | None = None) -> DataFrame:
        df.createOrReplaceTempView("input_view")

        aggr_sql = f"""
        WITH aggr AS (
            SELECT
                c,
                SUM({column}) > 1 AS {condition_col}
            FROM input_view
            GROUP BY c
        )
        SELECT
            input_view.*,
            aggr.{condition_col} AS {condition_col}
        FROM input_view
        LEFT JOIN aggr
          ON input_view.c = aggr.c
        """

        return spark.sql(aggr_sql)

    return (
        check_funcs.make_condition(
            condition=F.col(condition_col),  # check condition returns true
            message="dataset check ref failed",
            alias=f"{column}_custom_dataset_check",
        ),
        closure,
    )


def test_apply_checks_with_sql_query(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 3], [1, None, 4], [None, None, None]], SCHEMA)

    # fail the check if condition column evaluates to True
    query = "SELECT c, SUM(a) > 1 AS condition FROM {{input_view}} GROUP BY c"
    query_non_unique_merge_key = "SELECT a, b is not null AS condition FROM {{ input_view_non_default_name }}"

    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=sql_query,
            check_func_kwargs={
                "query": query,
                "merge_columns": ["c"],
                "condition_column": "condition",
                "msg": "sql aggregation check failed",
            },
        ),
        DQDatasetRule(
            criticality="error",
            check_func=sql_query,
            check_func_kwargs={
                "query": query,
                "merge_columns": ["c"],
                "condition_column": "condition",
                "msg": "sql aggregation check failed - negated",
                "name": "a_sql_aggregation_check_negated",
                "negate": True,  # fail if condition evaluates to False
            },
        ),
        DQDatasetRule(
            criticality="warn",
            check_func=sql_query,
            name="non_unique_merge_key",
            check_func_kwargs={
                "query": query_non_unique_merge_key,
                "merge_columns": ["a"],
                "condition_column": "condition",
                "input_placeholder": "input_view_non_default_name",
            },
        ),
        DQDatasetRule(
            criticality="error",
            check_func=sql_query,
            check_func_kwargs={
                "query": query,
                "merge_columns": ["c"],
                "condition_column": "condition",
                "negate": True,
            },
        ),
        DQDatasetRule(
            criticality="warn",
            check_func=sql_query,
            name="check_with_filter",
            # would result in quality issue if row filter was not pushed down to the query but only applied after
            filter="b is not null",
            check_func_kwargs={
                "query": query,
                "merge_columns": ["c"],
                "condition_column": "condition",
            },
        ),
        DQDatasetRule(
            criticality="warn",
            name="multiple_key_check_violation",  # overwrite name specified for the check function
            check_func=sql_query,
            check_func_kwargs={
                "query": "SELECT b, c, SUM(a) > 0 AS condition FROM {{input_view}} GROUP BY b, c",
                "merge_columns": ["b", "c"],
                "msg": "multiple key check failed",
                "condition_column": "condition",
                "name": "not_used",
            },
        ),
    ]
    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [
                1,
                3,
                3,
                None,
                [
                    {
                        "name": "c_query_condition_violation",
                        "message": "sql aggregation check failed",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "non_unique_merge_key",
                        "message": f"Value is not matching query: '{query_non_unique_merge_key}'",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "multiple_key_check_violation",
                        "message": "multiple key check failed",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                2,
                None,
                3,
                None,
                [
                    {
                        "name": "c_query_condition_violation",
                        "message": "sql aggregation check failed",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                1,
                None,
                4,
                [
                    {
                        "name": "a_sql_aggregation_check_negated",
                        "message": "sql aggregation check failed - negated",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_query_condition_violation",
                        "message": f"Value is matching query: '{query}'",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    # this is a false positive because the merge columns are not unique in the results of the user query
                    # the record is valid but the check fails for it since one of the records of the merge columns
                    # has failed
                    {
                        "name": "non_unique_merge_key",
                        "message": f"Value is not matching query: '{query_non_unique_merge_key}'",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [None, None, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_with_sql_query_and_ref_df(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    # sensor data
    sensor_schema = "measurement_id: int, sensor_id: int, reading_value: int"
    sensor_df = spark.createDataFrame([[1, 1, 4], [1, 2, 1], [2, 2, 110]], sensor_schema)

    # reference specs
    sensor_specs_df = spark.createDataFrame(
        [
            [1, 5],
            [2, 100],
        ],
        "sensor_id: int, min_threshold: int",
    )

    query = """
            WITH joined AS (
                SELECT
                    sensor.*,
                    COALESCE(specs.min_threshold, 100) AS effective_threshold
                FROM {{ sensor }} sensor
                LEFT JOIN {{ sensor_specs }} specs
                    ON sensor.sensor_id = specs.sensor_id
            )
            SELECT
                sensor_id,
                MAX(CASE WHEN reading_value > effective_threshold THEN 1 ELSE 0 END) = 1 AS condition
            FROM joined
            GROUP BY sensor_id
        """

    checks = [
        DQDatasetRule(
            criticality="error",
            check_func=sql_query,
            check_func_kwargs={
                "query": query,
                "merge_columns": ["sensor_id"],
                "condition_column": "condition",
                "msg": "one of the sensor reading is greater than limit",
                "name": "sensor_reading_check",
                "input_placeholder": "sensor",
            },
        ),
    ]

    ref_dfs = {"sensor_specs": sensor_specs_df}
    checked = dq_engine.apply_checks(sensor_df, checks, ref_dfs=ref_dfs)

    expected = spark.createDataFrame(
        [
            [1, 1, 4, None, None],
            [
                1,
                2,
                1,
                [
                    {
                        "name": "sensor_reading_check",
                        "message": "one of the sensor reading is greater than limit",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                2,
                2,
                110,
                [
                    {
                        "name": "sensor_reading_check",
                        "message": "one of the sensor reading is greater than limit",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
        ],
        sensor_schema + REPORTING_COLUMNS,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_with_sql_query_and_ref_table(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    # sensor data
    sensor_schema = "measurement_id: int, sensor_id: int, reading_value: int"
    sensor_df = spark.createDataFrame([[1, 1, 4], [1, 2, 1], [2, 2, 110]], sensor_schema)

    # reference specs
    sensor_specs_df = spark.createDataFrame(
        [
            [1, 5],
            [2, 100],
        ],
        "sensor_id: int, min_threshold: int",
    )

    checks = yaml.safe_load(
        """
        - criticality: error
          check:
            function: sql_query
            arguments:
              merge_columns:
                - sensor_id
              condition_column: condition
              msg: one of the sensor reading is greater than limit
              name: sensor_reading_check
              negate: false
              input_placeholder: sensor
              query: |
                WITH joined AS (
                    SELECT
                        sensor.*,
                        COALESCE(specs.min_threshold, 100) AS effective_threshold
                    FROM {{ sensor }} sensor
                    LEFT JOIN {{ sensor_specs }} specs
                        ON sensor.sensor_id = specs.sensor_id
                )
                SELECT
                    sensor_id,
                    MAX(CASE WHEN reading_value > effective_threshold THEN 1 ELSE 0 END) = 1 AS condition
                FROM joined
                GROUP BY sensor_id
        """
    )

    ref_dfs = {"sensor_specs": sensor_specs_df}

    checked = dq_engine.apply_checks_by_metadata(sensor_df, checks, ref_dfs=ref_dfs)

    expected = spark.createDataFrame(
        [
            [1, 1, 4, None, None],
            [
                1,
                2,
                1,
                [
                    {
                        "name": "sensor_reading_check",
                        "message": "one of the sensor reading is greater than limit",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                2,
                2,
                110,
                [
                    {
                        "name": "sensor_reading_check",
                        "message": "one of the sensor reading is greater than limit",
                        "columns": None,
                        "filter": None,
                        "function": "sql_query",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
        ],
        sensor_schema + REPORTING_COLUMNS,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_with_custom_check(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 3], [None, 4, None], [None, None, None]], SCHEMA)

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
        DQDatasetRule(
            criticality="warn", check_func=custom_dataset_check_func, column="a", check_func_kwargs={"group_by": "c"}
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [
                1,
                3,
                3,
                None,
                [
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
                2,
                None,
                3,
                None,
                [
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
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_for_each_col_with_custom_check(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[None, None, None], [1, 1, 1], [1, 1, 1]], SCHEMA)

    checks = (
        DQForEachColRule(criticality="warn", check_func=custom_row_check_func_global, columns=["a", "b"]).get_rules()
        + DQForEachColRule(
            # check func must be registered as dataset check to use in DQForEachColRule
            criticality="warn",
            check_func=custom_dataset_check_func_with_ref_dfs,
            columns=["a", "b"],
        ).get_rules()
    )

    ref_df = spark.createDataFrame([[1, 1, 1], [1, 1, 1]], SCHEMA)
    ref_dfs = {"ref_df": ref_df}

    checked = dq_engine.apply_checks(test_df, checks, ref_dfs=ref_dfs)

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
                ],
            ],
            [
                1,
                1,
                1,
                None,
                [
                    {
                        "name": "a_custom_dataset_check",
                        "message": "dataset check ref failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_dataset_check_func_with_ref_dfs",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "b_custom_dataset_check",
                        "message": "dataset check ref failed",
                        "columns": ["b"],
                        "filter": None,
                        "function": "custom_dataset_check_func_with_ref_dfs",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                1,
                1,
                1,
                None,
                [
                    {
                        "name": "a_custom_dataset_check",
                        "message": "dataset check ref failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_dataset_check_func_with_ref_dfs",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "b_custom_dataset_check",
                        "message": "dataset check ref failed",
                        "columns": ["b"],
                        "filter": None,
                        "function": "custom_dataset_check_func_with_ref_dfs",
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
            result_column_names={
                ColumnArguments.ERRORS.value: "dq_errors",
                ColumnArguments.WARNINGS.value: "dq_warnings",
            },
            run_time=RUN_TIME.isoformat(),
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
            result_column_names={
                ColumnArguments.ERRORS.value: "dq_errors",
                ColumnArguments.WARNINGS.value: "dq_warnings",
            },
            run_time=RUN_TIME.isoformat(),
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
            result_column_names={"errors_invalid": "dq_errors", "warnings_invalid": "dq_warnings"},
            run_time=RUN_TIME.isoformat(),
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
        {
            "criticality": "error",
            "check": {
                "function": "sql_expression",
                # columns can be optionally passed for reporting
                "arguments": {"columns": ["col1", "col2"], "expression": "col2 not like 'val%'"},
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
                        "name": "not_col1_not_like_val",
                        "message": "Value is not matching expression: col1 not like \"val%\"",
                        "columns": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "not_col2_not_like_val",
                        "message": "Value is not matching expression: col2 not like 'val%'",
                        "columns": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col1_col2_not_col2_not_like_val",
                        "message": "Value is not matching expression: col2 not like 'val%'",
                        "columns": ["col1", "col2"],
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


def test_apply_checks_with_sql_expression_using_classes(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    schema = "col1: string, col2: string"
    test_df = spark.createDataFrame([["str1", "str2"], ["val1", "val2"]], schema)

    checks = [
        DQRowRule(
            criticality="error",
            check_func=check_funcs.sql_expression,
            check_func_kwargs={"expression": "col1 not like \"val%\""},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.sql_expression,
            column="col1",
            check_func_kwargs={"expression": "col2 not like 'val%'"},
        ),
        DQRowRule(
            name="should_report_columns",
            criticality="error",
            check_func=check_funcs.sql_expression,
            columns=["col1", "col2"],  # columns can be passed optionally for reporting
            check_func_kwargs={"expression": "col2 not like 'val%'"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.sql_expression,
            # columns can be passed optionally for reporting also in kwargs
            check_func_kwargs={"columns": ["col1", "col2"], "expression": "col2 not like 'val%'"},
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            ["str1", "str2", None, None],
            [
                "val1",
                "val2",
                [
                    {
                        "name": "not_col1_not_like_val",
                        "message": "Value is not matching expression: col1 not like \"val%\"",
                        "columns": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "not_col2_not_like_val",
                        "message": "Value is not matching expression: col2 not like 'val%'",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "should_report_columns",
                        "message": "Value is not matching expression: col2 not like 'val%'",
                        "columns": ["col1", "col2"],
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col1_col2_not_col2_not_like_val",
                        "message": "Value is not matching expression: col2 not like 'val%'",
                        "columns": ["col1", "col2"],
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
            "check": {"function": "is_unique", "arguments": {"columns": ["col3"]}},
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
                None,
                None,
                "",
                None,
                [
                    {
                        "name": "col3_is_not_unique",
                        "message": "Value '' in column 'col3' is not unique, found 2 duplicates",
                        "columns": ["col3"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                None,
                None,
                "",
                None,
                [
                    {
                        "name": "col3_is_not_unique",
                        "message": "Value '' in column 'col3' is not unique, found 2 duplicates",
                        "columns": ["col3"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                1,
                datetime(2025, 1, 1),
                "a",
                [
                    {
                        "name": "col1_is_not_unique",
                        "message": "Value '1' in column 'col1' is not unique, found 2 duplicates",
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
                [
                    {
                        "name": "col3_is_not_unique",
                        "message": "Value 'a' in column 'col3' is not unique, found 2 duplicates",
                        "columns": ["col3"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                1,
                datetime(2025, 1, 2),
                "a",
                [
                    {
                        "name": "col1_is_not_unique",
                        "message": "Value '1' in column 'col1' is not unique, found 2 duplicates",
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
                [
                    {
                        "name": "col3_is_not_unique",
                        "message": "Value 'a' in column 'col3' is not unique, found 2 duplicates",
                        "columns": ["col3"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                2,
                None,
                "b",
                [
                    {
                        "name": "col1_is_not_unique",
                        "message": "Value '2' in column 'col1' is not unique, found 2 duplicates",
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
                        "message": "Value '2' in column 'col1' is not unique, found 2 duplicates",
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
                        "name": "col1_is_not_unique",
                        "message": "Value '2' in column 'col1' is not unique, found 2 duplicates",
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
                        "message": "Value '2' in column 'col1' is not unique, found 2 duplicates",
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
                None,
                None,
                "",
                [
                    {
                        "name": "col1_is_not_unique",
                        "message": "Value 'null' in column 'col1' is not unique, found 2 duplicates",
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
                        "message": "Value 'null' in column 'col1' is not unique, found 2 duplicates",
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
                        "name": "col1_is_not_unique",
                        "message": "Value 'null' in column 'col1' is not unique, found 2 duplicates",
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
                        "message": "Value 'null' in column 'col1' is not unique, found 2 duplicates",
                        "columns": ["col1"],
                        "filter": "col2 is null",
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                1,
                datetime(2025, 1, 1),
                "a",
                [
                    {
                        "name": "col1_is_not_unique",
                        "message": "Value '1' in column 'col1' is not unique, found 2 duplicates",
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
                        "name": "col1_is_not_unique",
                        "message": "Value '1' in column 'col1' is not unique, found 2 duplicates",
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
                2,
                None,
                "b",
                [
                    {
                        "name": "col1_is_not_unique",
                        "message": "Value '2' in column 'col1' is not unique, found 2 duplicates",
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
                        "message": "Value '2' in column 'col1' is not unique, found 2 duplicates",
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
                        "name": "col1_is_not_unique",
                        "message": "Value '2' in column 'col1' is not unique, found 2 duplicates",
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
                        "message": "Value '2' in column 'col1' is not unique, found 2 duplicates",
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
    input_table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"
    output_table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema_name)

    file_path = Path(__file__).parent.parent / "resources" / "all_row_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        checks = yaml.safe_load(f)

    dq_engine = DQEngine(ws)
    assert not dq_engine.validate_checks(checks).has_errors

    schema = (
        "col1: string, col2: int, col3: int, col4 array<int>, col5: date, col6: timestamp, "
        "col7: map<string, int>, col8: struct<field1: int>, col10: int, col11: string, "
        "col_ipv4: string, col_ipv6: string"
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
                2,
                "val2",
                "192.168.1.1",
                "2001:0db8:85a3:08d3:1319:8a2e:0370:7344",
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
                2,
                "val2",
                "192.168.1.2",
                "2001:0db8:85a3:08d3:ffff:ffff:ffff:ffff",
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
                2,
                "val2",
                "192.168.1.3",
                "2001:db8:85a3:8d3:1319:8a2e:3.112.115.68",
            ],
        ],
        schema,
    )
    test_df.write.saveAsTable(input_table_name)
    streaming_test_df = spark.readStream.table(input_table_name)

    streaming_checked_df = dq_engine.apply_checks_by_metadata(streaming_test_df, checks)
    dq_engine.save_results_in_table(
        output_df=streaming_checked_df,
        output_config=OutputConfig(
            location=output_table_name,
            mode="append",
            trigger={"availableNow": True},
            options={
                "checkpointLocation": f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{make_random(10).lower()}"
            },
        ),
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
                2,
                "val2",
                "192.168.1.1",
                "2001:0db8:85a3:08d3:1319:8a2e:0370:7344",
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
                2,
                "val2",
                "192.168.1.2",
                "2001:0db8:85a3:08d3:ffff:ffff:ffff:ffff",
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
                2,
                "val2",
                "192.168.1.3",
                "2001:db8:85a3:8d3:1319:8a2e:3.112.115.68",
                None,
                None,
            ],
        ],
        expected_schema,
    )

    assert_df_equality(checked_df, expected, ignore_nullable=True)


def test_apply_checks_all_row_geo_checks_as_yaml_with_streaming(
    skip_if_runtime_not_geo_compatible, ws, make_schema, make_random, make_volume, spark
):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    input_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    output_table_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    volume = make_volume(catalog_name=catalog_name, schema_name=schema_name)

    file_path = Path(__file__).parent.parent / "resources" / "all_row_geo_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        checks = yaml.safe_load(f)

    dq_engine = DQEngine(ws)
    assert not dq_engine.validate_checks(checks).has_errors

    schema = (
        "col3: int, point_geom: string, linestring_geom: string, "
        "polygon_geom: string, multipoint_geom: string, multilinestring_geom: string, "
        "multipolygon_geom: string, geometrycollection_geom: string"
    )
    test_df = spark.createDataFrame(
        [
            [
                1,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
            ],
            [
                2,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
            ],
            [
                3,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
            ],
        ],
        schema,
    )
    test_df.write.saveAsTable(input_table_name)
    streaming_test_df = spark.readStream.table(input_table_name)

    streaming_checked_df = dq_engine.apply_checks_by_metadata(streaming_test_df, checks)
    dq_engine.save_results_in_table(
        output_df=streaming_checked_df,
        output_config=OutputConfig(
            location=output_table_name,
            mode="append",
            trigger={"availableNow": True},
            options={
                "checkpointLocation": f"/Volumes/{volume.catalog_name}/{volume.schema_name}/{volume.name}/{make_random(6).lower()}"
            },
        ),
    )

    checked_df = spark.table(output_table_name)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                1,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
                None,
                None,
            ],
            [
                2,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
                None,
                None,
            ],
            [
                3,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
                None,
                None,
            ],
        ],
        expected_schema,
    )

    assert_df_equality(checked_df, expected, ignore_nullable=True)


def test_apply_checks_all_checks_as_yaml(ws, spark):
    """Test applying all checks from a yaml file.

    The checks used in the test are also showcased in the docs under /docs/reference/quality_checks.mdx
    The checks should be kept up to date with the docs to make sure the documentation examples are validated.
    """
    file_path = Path(__file__).parent.parent / "resources" / "all_dataset_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        checks = yaml.safe_load(f)

    file_path = Path(__file__).parent.parent / "resources" / "all_row_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        checks.extend(yaml.safe_load(f))

    # Geo checks are executed in a separate test as they require specific DBR

    dq_engine = DQEngine(ws)
    status = dq_engine.validate_checks(checks)
    assert not status.has_errors

    schema = (
        "col1: string, col2: int, col3: int, col4 array<int>, col5: date, col6: timestamp, "
        "col7: map<string, int>, col8: struct<field1: int>, col10: int, col11: string, "
        "col_ipv4: string, col_ipv6: string"
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
                2,
                "val2",
                "192.168.1.0",
                "2001:0db8:85a3:08d3:0000:0000:0000:0001",
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
                2,
                "val2",
                "192.168.1.1",
                "2001:0db8:85a3:08d3:0000:0000:0000:1",
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
                2,
                "val2",
                "192.168.1.2",
                "2001:0db8:85a3:08d3:0000::2",
            ],
        ],
        schema,
    )

    ref_df = test_df.withColumnRenamed("col1", "ref_col1").withColumnRenamed("col2", "ref_col2")
    ref_dfs = {"ref_df_key": ref_df}

    checked = dq_engine.apply_checks_by_metadata(test_df, checks, ref_dfs=ref_dfs)

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
                2,
                "val2",
                "192.168.1.0",
                "2001:0db8:85a3:08d3:0000:0000:0000:0001",
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
                2,
                "val2",
                "192.168.1.1",
                "2001:0db8:85a3:08d3:0000:0000:0000:1",
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
                2,
                "val2",
                "192.168.1.2",
                "2001:0db8:85a3:08d3:0000::2",
                None,
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_all_geo_checks_as_yaml(skip_if_runtime_not_geo_compatible, ws, spark):
    """Test applying all geo checks from a yaml file."""
    file_path = Path(__file__).parent.parent / "resources" / "all_row_geo_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        checks = yaml.safe_load(f)

    dq_engine = DQEngine(ws)
    status = dq_engine.validate_checks(checks)
    assert not status.has_errors

    schema = (
        "col3: int, point_geom: string, linestring_geom: string, "
        "polygon_geom: string, multipoint_geom: string, multilinestring_geom: string, "
        "multipolygon_geom: string, geometrycollection_geom: string"
    )
    test_df = spark.createDataFrame(
        [
            [
                1,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
            ],
            [
                2,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
            ],
            [
                3,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
            ],
        ],
        schema,
    )

    ref_df = test_df.withColumnRenamed("col1", "ref_col1").withColumnRenamed("col2", "ref_col2")
    ref_dfs = {"ref_df_key": ref_df}

    checked = dq_engine.apply_checks_by_metadata(test_df, checks, ref_dfs=ref_dfs)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                1,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
                None,
                None,
            ],
            [
                2,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
                None,
                None,
            ],
            [
                3,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
                None,
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_all_checks_using_classes(ws, spark):
    """Test applying all checks using DQX classes.

    The checks used in the test are also showcased in the docs under /docs/reference/quality_checks.mdx
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
        # is_equal_to check (numeric literal)
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_equal_to,
            column="col10",  # or as expr: F.col("col10")
            check_func_kwargs={"value": 2},  # or as expr: F.lit(2)
        ),
        # is_equal_to check (column expression)
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_equal_to,
            column="col3",  # or as expr: F.col("col3")
            check_func_kwargs={"value": "col2"},  # or as expr: F.col("col2")
        ),
        # is_not_equal_to check (string literal)
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_equal_to,
            column="col1",  # or as expr: F.col("col1")
            check_func_kwargs={"value": "'unknown'"},  # or as expr: F.lit("unknown")
        ),
        # is_not_equal_to check (date literal)
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_equal_to,
            column="col5",  # or as expr: F.col("col5")
            check_func_kwargs={"value": datetime(2025, 2, 3).date()},
        ),
        # is_not_equal_to check (timestamp literal)
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_equal_to,
            column="col6",  # or as expr: F.col("col6")
            check_func_kwargs={"value": datetime(2025, 1, 1, 1, 0, 0)},
        ),
        # is_not_equal_to check (column expression)
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_equal_to,
            column="col3",  # or as expr: F.col("col3")
            check_func_kwargs={"value": "col2 + 5"},  # or as expr: F.col("col2") + F.lit(5)
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
        # is_aggr_equal check with count aggregation over all rows
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_equal,
            column="*",
            check_func_kwargs={"aggr_type": "count", "limit": 3},
        ),
        # is_aggr_equal check with aggregation over col2 (skip nulls)
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_equal,
            column="col2",
            check_func_kwargs={"aggr_type": "avg", "limit": 2.0},
        ),
        # is_aggr_equal check with aggregation over col2 grouped by col3 (skip nulls)
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_equal,
            column="col2",
            check_func_kwargs={"aggr_type": "max", "limit": 3},
        ),
        # is_aggr_not_equal check with count aggregation over all rows
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_equal,
            column="*",
            check_func_kwargs={"aggr_type": "count", "limit": 5},
        ),
        # is_aggr_not_equal check with aggregation over col2 (skip nulls)
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_equal,
            column="col2",
            check_func_kwargs={"aggr_type": "avg", "limit": 5.0},
        ),
        # is_aggr_not_equal check with aggregation over col2 grouped by col3 (skip nulls)
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_equal,
            column="col2",
            check_func_kwargs={"aggr_type": "max", "group_by": ["col3"], "limit": 10},
        ),
        # is_aggr_not_greater_than check with count aggregation over all rows
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="*",
            check_func_kwargs={"aggr_type": "count", "limit": 10},
        ),
        # is_aggr_not_greater_than check with aggregation over col2 (skip nulls)
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="col2",
            check_func_kwargs={"aggr_type": "count", "limit": 10},
        ),
        # is_aggr_not_greater_than check with aggregation over col2 grouped by col3 (skip nulls)
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="col2",
            check_func_kwargs={"aggr_type": "count", "group_by": ["col3"], "limit": 10},
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
        # optionally column or columns (depending on check func) can be provided as keyword/named argument
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            check_func_kwargs={"column": "col2", "aggr_type": "count", "limit": 10},
        ),
        # optionally arguments can be provided using positional arguments
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_greater_than,
            column="col2",
            check_func_args=[10, "count"],
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
        # apply check to multiple columns
        *DQForEachColRule(
            check_func=check_funcs.is_not_null,  # 'column' as first argument
            criticality="error",
            columns=["col3", "col5"],  # apply the check for each column in the list
            user_metadata={"tag1": "multi column"},
        ).get_rules(),
        *DQForEachColRule(
            check_func=check_funcs.is_unique,  # 'columns' as first argument
            criticality="error",
            columns=[["col3", "col5"], ["col1"]],  # apply the check for each list of columns
            user_metadata={"tag1": "multi column"},
        ).get_rules(),
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
        # is_equal_to check applied to a struct column element (dot notation)
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_equal_to,
            column="col8.field1",
            check_func_kwargs={"value": 1},
        ),
        # is_not_equal_to check applied to a map column element
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_equal_to,
            column=F.try_element_at("col7", F.lit("key1")),
            check_func_kwargs={"value": "col10"},
        ),
        # is_not_less_than check applied to an array column
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_less_than,
            column=F.array_min("col4"),
            check_func_kwargs={"limit": 1},
        ),
        # is_not_greater_than check applied to an array column
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_not_greater_than,
            column=F.array_max("col4"),
            check_func_kwargs={"limit": 10},
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
        # optionally column can be provided as keyword/named argument
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_in_range,
            check_func_kwargs={"column": "col2", "min_limit": 1, "max_limit": 10},
        ),
        # optionally arguments can be provided using positional arguments
        DQRowRule(criticality="error", check_func=check_funcs.is_in_range, column="col2", check_func_args=[1, 10]),
        *DQForEachColRule(
            check_func=check_funcs.is_not_null,
            criticality="error",
            columns=[
                "col1",  # col as string
                F.col("col2"),  # col
                "col8.field1",  # struct col
                F.try_element_at("col7", F.lit("key1")),  # map col
                F.try_element_at("col4", F.lit(1)),  # array col
            ],
        ).get_rules(),
        # is_valid_ipv4_address check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_ipv4_address,
            column="col_ipv4",
            user_metadata={"tag1": "value4", "tag2": "030"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_ipv4_address,
            column=F.col("col_ipv4"),
            user_metadata={"tag1": "value5", "tag2": "031"},
        ),
        # is_ipv4_address_in_cidr check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_ipv4_address_in_cidr,
            column="col_ipv4",
            user_metadata={"tag1": "value6", "tag2": "032"},
            check_func_kwargs={"cidr_block": "255.255.255.255/16"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_ipv4_address_in_cidr,
            column=F.col("col_ipv4"),
            user_metadata={"tag1": "value7", "tag2": "033"},
            check_func_kwargs={"cidr_block": "255.255.255.255/16"},
        ),
        # is_valid_ipv6_address check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_ipv6_address,
            column="col_ipv6",
            user_metadata={"tag1": "value8", "tag2": "034"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_valid_ipv6_address,
            column=F.col("col_ipv6"),
            user_metadata={"tag1": "value8", "tag2": "034"},
        ),
        # is_ipv6_address_in_cidr check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_ipv6_address_in_cidr,
            column="col_ipv6",
            user_metadata={"tag1": "value9", "tag2": "035"},
            check_func_kwargs={"cidr_block": "2001:db8:85a3:8d3:1319:8a2e:3.112.115.68/64"},
        ),
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_ipv6_address_in_cidr,
            column=F.col("col_ipv6"),
            user_metadata={"tag1": "value9", "tag2": "036"},
            check_func_kwargs={"cidr_block": "2001:0db8:85a3:08d3:0000:0000:0000:0000/64"},
        ),
        # is_data_fresh check
        DQRowRule(
            criticality="error",
            check_func=check_funcs.is_data_fresh,
            column="col5",
            check_func_kwargs={"max_age_minutes": 18000, "base_timestamp": "col6"},
        ),
        # is_data_fresh_per_time_window check
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_data_fresh_per_time_window,
            column="col6",
            check_func_kwargs={"window_minutes": 1, "min_records_per_window": 1, "lookback_windows": 3},
        ),
    ]

    dq_engine = DQEngine(ws)

    schema = (
        "col1: string, col2: int, col3: int, col4 array<int>, col5: date, col6: timestamp, "
        "col7: map<string, int>, col8: struct<field1: int>, col10: int, col11: string, "
        "col_ipv4: string, col_ipv6: string"
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
                2,
                "val2",
                "255.255.255.255",
                "2001:0db8:85a3:08d3:1319:8a2e:0370:7344",
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
                2,
                "val2",
                "255.255.255.1",
                "2001:0db8:85a3:08d3:ffff:ffff:ffff:ffff",
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
                2,
                "val2",
                "255.255.255.2",
                "2001:db8:85a3:8d3:1319:8a2e:3.112.115.68",
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
                2,
                "val2",
                "255.255.255.255",
                "2001:0db8:85a3:08d3:1319:8a2e:0370:7344",
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
                2,
                "val2",
                "255.255.255.1",
                "2001:0db8:85a3:08d3:ffff:ffff:ffff:ffff",
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
                2,
                "val2",
                "255.255.255.2",
                "2001:db8:85a3:8d3:1319:8a2e:3.112.115.68",
                None,
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_all_geo_checks_using_classes(skip_if_runtime_not_geo_compatible, ws, spark):
    """Test applying all geo checks using DQX classes.

    The checks used in the test are also showcased in the docs under /docs/reference/quality_checks.mdx
    The checks should be kept up to date with the docs to make sure the documentation examples are validated.
    """
    checks = [
        # is_latitude check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_latitude,
            column="col2",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_latitude,
            column=F.col("col2"),
        ),
        # is_longitude check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_longitude,
            column="col2",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_longitude,
            column=F.col("col2"),
        ),
        # is_geometry check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_geometry,
            column="point_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_geometry,
            column=F.col("point_geom"),
        ),
        # is_geography check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_geography,
            column="point_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_geography,
            column=F.col("point_geom"),
        ),
        # is_point check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_point,
            column="point_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_point,
            column=F.col("point_geom"),
        ),
        # is_linestring check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_linestring,
            column="linestring_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_linestring,
            column=F.col("linestring_geom"),
        ),
        # is_polygon check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_polygon,
            column="polygon_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_polygon,
            column=F.col("polygon_geom"),
        ),
        # is_multipoint check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_multipoint,
            column="multipoint_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_multipoint,
            column=F.col("multipoint_geom"),
        ),
        # is_multilinestring check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_multilinestring,
            column="multilinestring_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_multilinestring,
            column=F.col("multilinestring_geom"),
        ),
        # is_multipolygon check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_multipolygon,
            column="multipolygon_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_multipolygon,
            column=F.col("multipolygon_geom"),
        ),
        # is_geometrycollection check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_geometrycollection,
            column="geometrycollection_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_geometrycollection,
            column=F.col("geometrycollection_geom"),
        ),
        # is_ogc_valid check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_ogc_valid,
            column="point_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_ogc_valid,
            column=F.col("point_geom"),
        ),
        # is_non_empty_geometry check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_non_empty_geometry,
            column="point_geom",
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.is_non_empty_geometry,
            column=F.col("point_geom"),
        ),
        # has_dimension check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.has_dimension,
            column="polygon_geom",
            check_func_kwargs={"dimension": 2},
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.has_dimension,
            column=F.col("polygon_geom"),
            check_func_kwargs={"dimension": 2},
        ),
        # has_x_coordinate_between check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.has_x_coordinate_between,
            column="polygon_geom",
            check_func_kwargs={"min_value": 0.0, "max_value": 10.0},
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.has_x_coordinate_between,
            column=F.col("polygon_geom"),
            check_func_kwargs={"min_value": 0.0, "max_value": 10.0},
        ),
        # has_y_coordinate_between check
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.has_y_coordinate_between,
            column="polygon_geom",
            check_func_kwargs={"min_value": 0.0, "max_value": 10.0},
        ),
        DQRowRule(
            criticality="error",
            check_func=geo_check_funcs.has_y_coordinate_between,
            column=F.col("polygon_geom"),
            check_func_kwargs={"min_value": 0.0, "max_value": 10.0},
        ),
    ]

    dq_engine = DQEngine(ws)

    schema = (
        "col2: int, point_geom: string, linestring_geom: string, "
        "polygon_geom: string, multipoint_geom: string, multilinestring_geom: string, "
        "multipolygon_geom: string, geometrycollection_geom: string"
    )
    test_df = spark.createDataFrame(
        [
            [
                1,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
            ],
            [
                2,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
            ],
            [
                3,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
            ],
        ],
        schema,
    )

    checked = dq_engine.apply_checks(test_df, checks)

    expected_schema = schema + REPORTING_COLUMNS
    expected = spark.createDataFrame(
        [
            [
                1,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
                None,
                None,
            ],
            [
                2,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
                None,
                None,
            ],
            [
                3,
                "POINT(1 1)",
                "LINESTRING(1 1, 2 2)",
                "POLYGON((1 1, 3 1, 3 3, 1 3, 1 1))",
                "MULTIPOINT(1 1, 2 2)",
                "MULTILINESTRING((1 1, 2 2))",
                "MULTIPOLYGON(((1 1, 3 1, 3 3, 1 3, 1 1)))",
                "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(1 1, 2 2), POLYGON((1 1, 3 1, 3 3, 1 3, 1 1)))",
                None,
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_define_user_metadata_and_extract_dq_results(ws, spark):
    user_metadata = {"key1": "value1", "key2": "value2"}
    extra_params = ExtraParams(run_time=RUN_TIME.isoformat(), user_metadata=user_metadata)
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
                        "name": "not_not_exists_col2_x_x_key1_10",
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
    extra_params = ExtraParams(
        run_time=RUN_TIME.isoformat(), user_metadata={"tag2": "from_engine", "tag3": "from_engine"}
    )
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
    extra_params = ExtraParams(
        run_time=RUN_TIME.isoformat(), user_metadata={"tag2": "from_engine", "tag3": "from_engine"}
    )
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
            criticality="error",
            check_func=check_funcs.is_aggr_equal,
            column="*",
            check_func_kwargs={"aggr_type": "count", "limit": 3},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_equal,
            column="a",
            filter="b is not null",
            check_func_kwargs={"aggr_type": "count", "limit": 1},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_equal,
            column="a",
            check_func_kwargs={"aggr_type": "avg", "limit": 1},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_equal,
            column="b",
            check_func_kwargs={"aggr_type": "min", "limit": 2},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_equal,
            column="b",
            check_func_kwargs={"aggr_type": "sum", "group_by": ["a"], "limit": 8},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_equal,
            column="*",
            check_func_kwargs={"aggr_type": "count", "limit": 5},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_equal,
            column="a",
            filter="b is not null",
            check_func_kwargs={"aggr_type": "count", "limit": 3},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_equal,
            column="a",
            check_func_kwargs={"aggr_type": "avg", "limit": 5},
        ),
        DQDatasetRule(
            criticality="error",
            check_func=check_funcs.is_aggr_not_equal,
            column="b",
            check_func_kwargs={"aggr_type": "sum", "group_by": ["a"], "limit": 10},
        ),
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
                None,
                5,
                [
                    {
                        "name": "b_sum_group_by_a_not_equal_to_limit",
                        "message": "Sum 2 in column 'b' per group of columns 'a' is not equal to limit: 8",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_aggr_equal",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_group_by_a_greater_than_limit",
                        "message": "Count 2 in column 'a' per group of columns 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "row_count_group_by_a_b_greater_than_limit",
                        "message": "Count 1 in column 'a' per group of columns 'a, b' is greater than limit: 0",
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
                        "message": "Avg 4.0 in column 'c' per group of columns 'a' is greater than limit: 0",
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
                        "name": "b_sum_group_by_a_not_equal_to_limit",
                        "message": "Sum 2 in column 'b' per group of columns 'a' is not equal to limit: 8",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_aggr_equal",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_group_by_a_greater_than_limit",
                        "message": "Count 2 in column 'a' per group of columns 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_group_by_a_greater_than_limit_with_b_not_null",
                        "message": "Count 1 in column 'a' per group of columns 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": "b is not null",
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "row_count_group_by_a_b_greater_than_limit",
                        "message": "Count 1 in column 'a' per group of columns 'a, b' is greater than limit: 0",
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
                        "message": "Avg 4.0 in column 'c' per group of columns 'a' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_group_by_a_less_than_limit_with_b_not_null",
                        "message": "Count 1 in column 'a' per group of columns 'a' is less than limit: 10",
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
        {
            "criticality": "warn",
            "check": {
                "function": "is_aggr_equal",
                "arguments": {"column": "*", "aggr_type": "count", "limit": 3},
            },
        },
        {
            "name": "a_count_equal_to_limit_with_filter",
            "criticality": "error",
            "check": {
                "function": "is_aggr_equal",
                "arguments": {"column": "a", "aggr_type": "count", "limit": 1},
            },
            "filter": "b is not null",
        },
        {
            "criticality": "warn",
            "check": {
                "function": "is_aggr_not_equal",
                "arguments": {"column": "*", "aggr_type": "count", "limit": 3},
            },
        },
        {
            "name": "a_count_not_equal_to_limit",
            "criticality": "error",
            "check": {
                "function": "is_aggr_not_equal",
                "arguments": {"column": "a", "aggr_type": "count", "limit": 2},
            },
        },
        {
            "name": "a_count_not_equal_to_limit_with_filter",
            "criticality": "error",
            "check": {
                "function": "is_aggr_not_equal",
                "arguments": {"column": "a", "aggr_type": "count", "limit": 1},
            },
            "filter": "b is not null",
        },
        {
            "name": "c_avg_not_equal_to_limit",
            "criticality": "warn",
            "check": {
                "function": "is_aggr_not_equal",
                "arguments": {"column": "c", "aggr_type": "avg", "limit": 4.0},
            },
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
                    },
                    {
                        "name": "a_count_not_equal_to_limit",
                        "message": "Count 2 in column 'a' is equal to limit: 2",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_equal",
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
                    {
                        "name": "count_equal_to_limit",
                        "message": "Count 3 in column '*' is equal to limit: 3",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_equal",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_not_equal_to_limit",
                        "message": "Avg 4.0 in column 'c' is equal to limit: 4.0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_equal",
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
                        "message": "Count 2 in column 'a' per group of columns 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "row_count_group_by_a_b_greater_than_limit",
                        "message": "Count 1 in column 'a' per group of columns 'a, b' is greater than limit: 0",
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
                        "message": "Avg 4.0 in column 'c' per group of columns 'a' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_not_equal_to_limit",
                        "message": "Count 2 in column 'a' is equal to limit: 2",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_equal",
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
                    {
                        "name": "count_equal_to_limit",
                        "message": "Count 3 in column '*' is equal to limit: 3",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_equal",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_not_equal_to_limit",
                        "message": "Avg 4.0 in column 'c' is equal to limit: 4.0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_equal",
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
                        "message": "Count 2 in column 'a' per group of columns 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_group_by_a_greater_than_limit_with_b_not_null",
                        "message": "Count 1 in column 'a' per group of columns 'a' is greater than limit: 0",
                        "columns": ["a"],
                        "filter": "b is not null",
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "row_count_group_by_a_b_greater_than_limit",
                        "message": "Count 1 in column 'a' per group of columns 'a, b' is greater than limit: 0",
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
                        "message": "Avg 4.0 in column 'c' per group of columns 'a' is greater than limit: 0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_group_by_a_less_than_limit_with_b_not_null",
                        "message": "Count 1 in column 'a' per group of columns 'a' is less than limit: 10",
                        "columns": ["a"],
                        "filter": "b is not null",
                        "function": "is_aggr_not_less_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_not_equal_to_limit",
                        "message": "Count 2 in column 'a' is equal to limit: 2",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_aggr_not_equal",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "a_count_not_equal_to_limit_with_filter",
                        "message": "Count 1 in column 'a' is equal to limit: 1",
                        "columns": ["a"],
                        "filter": "b is not null",
                        "function": "is_aggr_not_equal",
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
                    {
                        "name": "count_equal_to_limit",
                        "message": "Count 3 in column '*' is equal to limit: 3",
                        "columns": ["*"],
                        "filter": None,
                        "function": "is_aggr_not_equal",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "c_avg_not_equal_to_limit",
                        "message": "Avg 4.0 in column 'c' is equal to limit: 4.0",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_aggr_not_equal",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(all_df, expected_df)


def test_apply_checks_raises_error_when_passed_dict_instead_of_dqrules(ws, spark):
    dq_engine = DQEngine(ws)
    src_df = spark.createDataFrame([[1, 3, 3]], SCHEMA)
    checks_yaml = yaml.safe_load(
        """
        - criticality: error
          check:
            function: is_not_null_and_not_empty
            arguments:
              column: a

        - criticality: error
          check:
            function: is_not_null_and_not_empty
            arguments:
              column: b
    """
    )

    with pytest.raises(
        InvalidCheckError,
        match="All elements in the 'checks' list must be instances of DQRule. Use 'apply_checks_by_metadata' to pass checks as list of dicts instead.",
    ):
        dq_engine.apply_checks(src_df, checks=checks_yaml)


def test_apply_checks_and_split_raises_error_when_passed_dict_instead_of_dqrules(ws, spark):
    dq_engine = DQEngine(ws)
    src_df = spark.createDataFrame([[1, 3, 3]], SCHEMA)
    checks_yaml = yaml.safe_load(
        """
        - criticality: error
          check:
            function: is_not_null_and_not_empty
            arguments:
              column: a

        - criticality: error
          check:
            function: is_not_null_and_not_empty
            arguments:
              column: b
    """
    )

    with pytest.raises(
        InvalidCheckError,
        match="All elements in the 'checks' list must be instances of DQRule. Use 'apply_checks_by_metadata_and_split' to pass checks as list of dicts instead.",
    ):
        dq_engine.apply_checks_and_split(src_df, checks=checks_yaml)


def test_compare_datasets_check(ws, spark, set_utc_timezone):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    schema = "id1 long, id2 long, name string, dt date, ts timestamp, score float, likes bigint, active boolean"

    src_df = spark.createDataFrame(
        [
            [1, 1, "Grzegorz", datetime(2017, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.7, 123234234345, True],
            # extra row
            [2, 1, "Tim", datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 54545, True],
            [3, 1, "Mike", datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False],
        ],
        schema,
    )

    ref_df = spark.createDataFrame(
        [
            # diff in dt and score
            [1, 1, "Grzegorz", datetime(2018, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.9, 123234234345, True],
            # no diff
            [3, 1, "Mike", datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False],
            # missing record
            [2, 2, "Timmy", datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 8754857845, True],
        ],
        schema,
    )

    pk_columns = ["id1", "id2"]

    checks = [
        DQDatasetRule(
            name="id1_id2_compare_datasets",
            criticality="error",
            check_func=check_funcs.compare_datasets,
            columns=pk_columns,
            filter="id1 != 2",
            check_func_kwargs={"ref_columns": pk_columns, "ref_df_name": "ref_df"},
            user_metadata={"tag1": "value1"},
        ),
    ]

    refs_df = {"ref_df": ref_df}

    checked = dq_engine.apply_checks(src_df, checks, refs_df)

    expected = spark.createDataFrame(
        [
            [
                1,
                1,
                "Grzegorz",
                datetime(2017, 1, 1),
                datetime(2018, 1, 1, 12, 34, 56),
                26.7,
                123234234345,
                True,
                [
                    {
                        "name": "id1_id2_compare_datasets",
                        "message": json.dumps(
                            {
                                "row_missing": False,
                                "row_extra": False,
                                "changed": {
                                    "dt": {"df": "2017-01-01", "ref": "2018-01-01"},
                                    "score": {"df": "26.7", "ref": "26.9"},
                                },
                            },
                            separators=(',', ':'),
                        ),
                        "columns": pk_columns,
                        "filter": "id1 != 2",
                        "function": "compare_datasets",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value1"},
                    }
                ],
                None,
            ],
            [
                2,
                1,
                "Tim",
                datetime(2018, 1, 1),
                datetime(2018, 2, 1, 12, 34, 56),
                36.7,
                54545,
                True,
                None,
                None,
            ],  # no issues due to filter
            [3, 1, "Mike", datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False, None, None],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(checked.sort(pk_columns), expected.sort(pk_columns), ignore_nullable=True)


def test_compare_datasets_check_missing_records(ws, spark, set_utc_timezone):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    schema = "id1 long, id2 long, name string, dt date, ts timestamp, score float, likes bigint, active boolean"

    src_df = spark.createDataFrame(
        [
            [1, 1, "Grzegorz", datetime(2017, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.7, 123234234345, True],
            # extra row
            [2, 1, "Marcin", datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 54545, True],
            [3, 1, "Mike", datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False],
        ],
        schema,
    )

    ref_df = spark.createDataFrame(
        [
            # diff in dt and score
            [1, 1, "Grzegorz", datetime(2018, 1, 1), datetime(2018, 1, 1, 12, 34, 56), 26.9, 123234234345, True],
            # no diff
            [3, 1, "Mike", datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False],
            # missing record
            [2, 2, "John", datetime(2018, 1, 1), datetime(2018, 2, 1, 12, 34, 56), 36.7, 8754857845, True],
        ],
        schema,
    )

    pk_columns = ["id1", "id2"]

    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.compare_datasets,
            columns=pk_columns,
            check_func_kwargs={
                "ref_columns": pk_columns,
                "ref_df_name": "ref_df",
                "check_missing_records": True,
                "exclude_columns": ["score"],
            },
        ),
    ]

    refs_df = {"ref_df": ref_df}

    checked = dq_engine.apply_checks(src_df, checks, refs_df)

    expected = spark.createDataFrame(
        [
            [
                1,
                1,
                "Grzegorz",
                datetime(2017, 1, 1),
                datetime(2018, 1, 1, 12, 34, 56),
                26.7,
                123234234345,
                True,
                None,
                [
                    {
                        "name": "datasets_diff_pk_id1_id2_ref_id1_id2",
                        "message": json.dumps(
                            {
                                "row_missing": False,
                                "row_extra": False,
                                "changed": {
                                    "dt": {"df": "2017-01-01", "ref": "2018-01-01"},
                                },
                            },
                            separators=(',', ':'),
                        ),
                        "columns": pk_columns,
                        "filter": None,
                        "function": "compare_datasets",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                2,
                1,
                "Marcin",
                datetime(2018, 1, 1),
                datetime(2018, 2, 1, 12, 34, 56),
                36.7,
                54545,
                True,
                None,
                [
                    {
                        "name": "datasets_diff_pk_id1_id2_ref_id1_id2",
                        "message": json.dumps(
                            {
                                "row_missing": False,
                                "row_extra": True,
                                "changed": {
                                    "name": {"df": "Marcin"},
                                    "dt": {"df": "2018-01-01"},
                                    "ts": {"df": "2018-02-01 12:34:56"},
                                    "likes": {"df": "54545"},
                                    "active": {"df": "true"},
                                },
                            },
                            separators=(',', ':'),
                        ),
                        "columns": pk_columns,
                        "filter": None,
                        "function": "compare_datasets",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [3, 1, "Mike", datetime(2019, 1, 1), datetime(2018, 3, 1, 12, 34, 56), 46.7, 5667888989, False, None, None],
            [
                2,
                2,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                [
                    {
                        "name": "datasets_diff_pk_id1_id2_ref_id1_id2",
                        "message": json.dumps(
                            {
                                "row_missing": True,
                                "row_extra": False,
                                "changed": {
                                    "name": {"ref": "John"},
                                    "dt": {"ref": "2018-01-01"},
                                    "ts": {"ref": "2018-02-01 12:34:56"},
                                    "likes": {"ref": "8754857845"},
                                    "active": {"ref": "true"},
                                },
                            },
                            separators=(',', ':'),
                        ),
                        "columns": pk_columns,
                        "filter": None,
                        "function": "compare_datasets",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(checked.sort(pk_columns), expected.sort(pk_columns), ignore_nullable=True)


def test_compare_datasets_check_missing_records_with_filter(ws, spark, set_utc_timezone):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    schema = "id long, name string"
    src_df = spark.createDataFrame(
        [
            [1, "Tim"],
        ],
        schema,
    )

    schema_ref = "id2 long, name string"
    ref_df = spark.createDataFrame(
        [
            [1, "Marcin"],
            [2, "Marcin"],
        ],
        schema_ref,
    )

    pk_columns = ["id"]
    pk_ref_columns = ["id2"]

    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.compare_datasets,
            columns=pk_columns,
            filter="id not in (1, 2)",
            check_func_kwargs={"ref_columns": pk_ref_columns, "ref_df_name": "ref_df", "check_missing_records": True},
        ),
    ]

    refs_df = {"ref_df": ref_df}
    checked = dq_engine.apply_checks(src_df, checks, refs_df)

    expected = spark.createDataFrame(
        [
            [
                1,
                "Tim",
                None,
                None,  # issues filtered
            ],
            [
                2,
                None,
                None,
                None,  # issues filtered
            ],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(checked.sort(pk_columns), expected.sort(pk_columns), ignore_nullable=True)


def test_compare_datasets_check_missing_records_with_partial_filter(
    ws, spark, set_utc_timezone, make_schema, make_random
):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)

    schema = "id long, name string"
    src_df = spark.createDataFrame(
        [
            [1, "Tim"],
            [3, "Marcin"],
        ],
        schema,
    )

    schema_ref = "id2 long, name string"
    ref_df = spark.createDataFrame(
        [
            [1, "Marcin"],
            [2, "Marcin"],
        ],
        schema_ref,
    )

    catalog_name = "main"
    ref_table_schema = make_schema(catalog_name=catalog_name)
    ref_table = f"{catalog_name}.{ref_table_schema.name}.{make_random(10).lower()}"
    ref_df.write.saveAsTable(ref_table)

    pk_columns = ["id"]
    pk_ref_columns = ["id2"]
    filter_str = "id not in (1)"

    checks = [
        DQDatasetRule(
            criticality="warn",
            check_func=check_funcs.compare_datasets,
            columns=pk_columns,
            filter=filter_str,
            check_func_kwargs={
                "ref_columns": pk_ref_columns,
                "ref_table": ref_table,
                "check_missing_records": True,
            },
        ),
    ]

    checked = dq_engine.apply_checks(src_df, checks)

    expected = spark.createDataFrame(
        [
            [
                1,
                "Tim",
                None,
                None,  # issues filtered
            ],
            [
                2,
                None,
                None,
                [
                    {
                        "name": "datasets_diff_pk_id_ref_id2",
                        "message": json.dumps(
                            {
                                "row_missing": True,
                                "row_extra": False,
                                "changed": {
                                    "name": {"ref": "Marcin"},
                                },
                            },
                            separators=(',', ':'),
                        ),
                        "columns": pk_columns,
                        "filter": filter_str,
                        "function": "compare_datasets",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
            [
                3,
                "Marcin",
                None,
                [
                    {
                        "name": "datasets_diff_pk_id_ref_id2",
                        "message": json.dumps(
                            {
                                "row_missing": False,
                                "row_extra": True,
                                "changed": {
                                    "name": {"df": "Marcin"},
                                },
                            },
                            separators=(',', ':'),
                        ),
                        "columns": pk_columns,
                        "filter": filter_str,
                        "function": "compare_datasets",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
            ],
        ],
        schema + REPORTING_COLUMNS,
    )

    assert_df_equality(checked.sort(pk_columns), expected.sort(pk_columns), ignore_nullable=True)


def test_apply_checks_with_is_data_fresh_per_time_window(ws, spark, set_utc_timezone):
    schema = "id: int, col1: timestamp"
    test_df = spark.createDataFrame(
        [
            [1, datetime(2025, 1, 1)],
            [1, datetime(2025, 1, 1)],
            [2, datetime(2025, 1, 2)],
            [3, None],
        ],
        schema,
    )

    checks = [
        {
            "criticality": "error",
            "check": {
                "function": "is_data_fresh_per_time_window",
                "arguments": {
                    "column": "col1",
                    "window_minutes": 1440,
                    "min_records_per_window": 2,
                },
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
                None,
                None,
            ],
            [
                1,
                datetime(2025, 1, 1),
                None,
                None,
            ],
            [
                2,
                datetime(2025, 1, 2),
                [
                    {
                        "name": "col1_is_data_fresh_per_time_window",
                        "message": "Data arrival completeness check failed: only 1 records found in 1440-minute interval starting at 2025-01-02 00:00:00 and ending at 2025-01-03 00:00:00, expected at least 2 records",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_data_fresh_per_time_window",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                None,
            ],
            [
                3,
                None,
                None,
                None,
            ],
        ],
        expected_schema,
    )
    assert_df_equality(checked.sort("id"), expected, ignore_nullable=True)


def test_apply_checks_and_save_in_tables_for_patterns_missing_output_suffix(ws, spark):
    dq_engine = DQEngine(ws)

    with pytest.raises(InvalidParameterError, match="Output table suffix cannot be empty"):
        dq_engine.apply_checks_and_save_in_tables_for_patterns(
            patterns=["*"],
            checks_location="catalog.schema.checks",
            run_config_template=RunConfig(),
            output_table_suffix="",
        )


def test_apply_checks_and_save_in_tables_for_patterns_missing_quarantine_suffix(ws, spark):
    dq_engine = DQEngine(ws)

    with pytest.raises(InvalidParameterError, match="Quarantine table suffix cannot be empty"):
        dq_engine.apply_checks_and_save_in_tables_for_patterns(
            patterns=["*"],
            checks_location="catalog.schema.checks",
            run_config_template=RunConfig(quarantine_config=OutputConfig("catalog.schema.table")),
            quarantine_table_suffix="",
        )
