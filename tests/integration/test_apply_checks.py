from datetime import datetime
from pathlib import Path
import yaml
import pyspark.sql.functions as F
import pytest
from pyspark.sql import Column
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.row_checks import (
    is_not_null_and_not_empty,
    make_condition,
    sql_expression,
    regex_match,
    is_unique,
    is_older_than_col2_for_n_days,
    is_older_than_n_days,
    is_not_in_near_future,
    is_not_in_future,
    is_valid_timestamp,
    is_valid_date,
    is_not_greater_than,
    is_not_less_than,
    is_not_in_range,
    is_in_range,
    is_not_null_and_not_empty_array,
    is_not_null_and_is_in_list,
    is_in_list,
    is_not_empty,
    is_not_null,
)
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import ExtraParams, DQColRule, DQColSetRule, ColumnArguments
from databricks.labs.dqx.schema import dq_result_schema

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
        DQColRule(
            name="col_a_is_null_or_empty",
            criticality="warn",
            check_func=is_not_null_and_not_empty,
            column="a",
        ),
        DQColRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            column="b",
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame([[1, 3, 3, None, None]], EXPECTED_SCHEMA)
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQColRule(
            name="col_a_is_null_or_empty",
            criticality="warn",
            check_func=is_not_null_and_not_empty,
            column="a",
        ),
        DQColRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            column="b",
        ),
        DQColRule(
            name="col_c_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            column="c",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
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
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
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
        DQColRule(
            name="col_c_is_null_or_empty",
            criticality="invalid",
            check_func=is_not_null_and_not_empty,
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
                        "name": "col_col1_is_null",
                        "message": "Column 'col1' value is null",
                        "column": "col1",
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_null",
                        "message": "Column 'col2' value is null",
                        "column": "col2",
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col3_is_null",
                        "message": "Column 'col3' value is null",
                        "column": "col3",
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
        DQColRule(criticality="error", check_func=is_not_null, column="col1"),
        DQColRule(
            # missing criticality, default to "error"
            check_func=is_not_null,
            column="col2",
        ),
    ] + DQColSetRule(
        # missing criticality, default to "error"
        check_func=is_not_null,
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
                        "name": "col_col1_is_null",
                        "message": "Column 'col1' value is null",
                        "column": "col1",
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_null",
                        "message": "Column 'col2' value is null",
                        "column": "col2",
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col3_is_null",
                        "message": "Column 'col3' value is null",
                        "column": "col3",
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
        DQColRule(criticality="warn", check_func=is_not_null_and_not_empty, column="a"),
        DQColRule(criticality="error", check_func=is_not_null_and_not_empty, column="b"),
        DQColRule(criticality="error", check_func=is_not_null_and_not_empty, column="c"),
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
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
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
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
        DQColRule(name="col_a_is_null_or_empty", criticality="warn", check_func=is_not_null_and_not_empty, column="a"),
        DQColRule(name="col_b_is_null_or_empty", criticality="error", check_func=is_not_null_and_not_empty, column="b"),
        DQColRule(name="col_c_is_null_or_empty", criticality="warn", check_func=is_not_null_and_not_empty, column="c"),
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
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
            "name": "col_a_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "a"}},
        },
        {
            "name": "col_b_is_null_or_empty",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
        },
        {
            "name": "col_c_is_null_or_empty",
            "criticality": "warn",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "c"}},
        },
        {
            "name": "col_a_is_not_in_the_list",
            "criticality": "warn",
            "check": {"function": "is_in_list", "arguments": {"column": "a", "allowed": [1, 3, 4]}},
        },
        {
            "name": "col_c_is_not_in_the_list",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "col_a_is_not_in_the_list",
                        "message": "Value '2' in Column 'a' is not in the allowed list: [1, 3, 4]",
                        "column": "a",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "col_a_is_not_in_the_list",
                        "message": "Value '2' in Column 'a' is not in the allowed list: [1, 3, 4]",
                        "column": "a",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "col_a_is_not_in_the_list",
                        "message": "Value '2' in Column 'a' is not in the allowed list: [1, 3, 4]",
                        "column": "a",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
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

    checks = DQColSetRule(
        check_func=is_not_null_and_not_empty, criticality="warn", filter="b>3", columns=["a", "c"]
    ).get_rules() + [
        DQColRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
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
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
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

    checks = DQColSetRule(
        name="common_name", check_func=is_not_null, criticality="warn", columns=["a", "b"]
    ).get_rules()

    checked = dq_engine.apply_checks(test_df, checks)

    expected = spark.createDataFrame(
        [
            [
                1,
                None,
                None,
                None,
                [
                    {
                        "name": "common_name",
                        "message": "Column 'b' value is null",
                        "column": "b",
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                ],
            ],
            [
                None,
                2,
                None,
                None,
                [
                    {
                        "name": "common_name",
                        "message": "Column 'a' value is null",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    }
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
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "b"}},
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "column": "b",
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
                        "name": "col_c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "column": "c",
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
                        "name": "col_col2_is_null",
                        "message": "Column 'col2' value is null",
                        "column": "col2",
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
                        "name": "col_col2_is_null",
                        "message": "Column 'col2' value is null",
                        "column": "col2",
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


def custom_check_func_global(column: str) -> Column:
    col_expr = F.col(column)
    return make_condition(col_expr.isNull(), "custom check failed", f"{column}_is_null_custom")


def test_apply_checks_with_custom_check(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQColRule(criticality="warn", check_func=is_not_null_and_not_empty, column="a"),
        DQColRule(criticality="warn", check_func=custom_check_func_global, column="a"),
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check failed",
                        "column": "a",
                        "filter": None,
                        "function": "custom_check_func_global",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check failed",
                        "column": "a",
                        "filter": None,
                        "function": "custom_check_func_global",
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
        {"criticality": "warn", "check": {"function": "custom_check_func_global", "arguments": {"column": "a"}}},
    ]

    checked = dq_engine.apply_checks_by_metadata(
        test_df, checks, {"custom_check_func_global": custom_check_func_global}
    )
    # or for simplicity use globals
    checked2 = dq_engine.apply_checks_by_metadata(test_df, checks, globals())

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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check failed",
                        "column": "a",
                        "filter": None,
                        "function": "custom_check_func_global",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check failed",
                        "column": "a",
                        "filter": None,
                        "function": "custom_check_func_global",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "check failed",
                        "column": "a",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "check failed",
                        "column": "b",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "check failed",
                        "column": "a",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "check failed",
                        "column": "b",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "check failed",
                        "column": "a",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "check failed",
                        "column": "b",
                        "filter": None,
                        "function": "col_a_is_null_or_empty",
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
            column_names={ColumnArguments.ERRORS.value: "dq_errors", ColumnArguments.WARNINGS.value: "dq_warnings"},
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "column": "a",
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
            column_names={ColumnArguments.ERRORS.value: "dq_errors", ColumnArguments.WARNINGS.value: "dq_warnings"},
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
                            "name": "col_b_is_null_or_empty",
                            "message": "Column 'b' value is null or empty",
                            "column": "b",
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
                            "name": "col_a_is_null_or_empty",
                            "message": "Column 'a' value is null or empty",
                            "column": "a",
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
                            "name": "col_b_is_null_or_empty",
                            "message": "Column 'b' value is null or empty",
                            "column": "b",
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": RUN_TIME,
                            "user_metadata": {},
                        }
                    ],
                    [
                        {
                            "name": "col_a_is_null_or_empty",
                            "message": "Column 'a' value is null or empty",
                            "column": "a",
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
            column_names={"errors_invalid": "dq_errors", "warnings_invalid": "dq_warnings"}, run_time=RUN_TIME
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
                            "name": "col_b_is_null_or_empty",
                            "message": "Column 'b' value is null or empty",
                            "column": "b",
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
                            "name": "col_a_is_null_or_empty",
                            "message": "Column 'a' value is null or empty",
                            "column": "a",
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
                            "name": "col_b_is_null_or_empty",
                            "message": "Column 'b' value is null or empty",
                            "column": "b",
                            "filter": None,
                            "function": "is_not_null_and_not_empty",
                            "run_time": RUN_TIME,
                            "user_metadata": {},
                        }
                    ],
                    [
                        {
                            "name": "col_a_is_null_or_empty",
                            "message": "Column 'a' value is null or empty",
                            "column": "a",
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
            "check": {"function": "sql_expression", "arguments": {"expression": "col2 not like 'val%'"}},
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
                        "name": "col_col1_not_like_val",
                        "message": "Value is not matching expression: col1 not like \"val%\"",
                        "column": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_not_like_val",
                        "message": "Value is not matching expression: col2 not like 'val%'",
                        "column": None,
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
            "criticality": "error",
            "name": "col_col2_is_not_unique",
            "check": {
                "function": "is_unique",
                "arguments": {"columns": ["col2"], "window_spec": "window(coalesce(col2, '1970-01-01'), '30 days')"},
            },
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
                1,
                datetime(2025, 1, 1),
                "a",
                [
                    {
                        "name": "col_col1_is_not_unique",
                        "message": "Value '1' in Column 'col1' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '2025-01-01 00:00:00' in Column 'col2' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in Column 'struct(col1, col3)' is not unique",
                        "column": None,
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
                        "name": "col_col1_is_not_unique",
                        "message": "Value '1' in Column 'col1' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '2025-01-02 00:00:00' in Column 'col2' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in Column 'struct(col1, col3)' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
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


def test_apply_checks_with_is_unique_nulls_not_distinct(ws, spark, set_utc_timezone):
    schema = "col1: int, col2: timestamp, col3: string"
    test_df = spark.createDataFrame(
        [
            [1, datetime(2025, 1, 1), "a"],
            [1, datetime(2025, 1, 2), "a"],
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
            "criticality": "error",
            "name": "col_col2_is_not_unique",
            "check": {
                "function": "is_unique",
                "arguments": {
                    "columns": ["col2"],
                    "window_spec": "window(coalesce(col2, '1970-01-01'), '30 days')",
                    "nulls_distinct": False,
                },
            },
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
                        "name": "composite_key_col1_and_col2_is_not_unique",
                        "message": "Value '{null, null}' in Column 'struct(col1, col2)' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{null, }' in Column 'struct(col1, col3)' is not unique",
                        "column": None,
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
                        "name": "composite_key_col1_and_col2_is_not_unique",
                        "message": "Value '{null, null}' in Column 'struct(col1, col2)' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{null, }' in Column 'struct(col1, col3)' is not unique",
                        "column": None,
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
                datetime(2025, 1, 1),
                "a",
                [
                    {
                        "name": "col_col1_is_not_unique",
                        "message": "Value '1' in Column 'col1' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '2025-01-01 00:00:00' in Column 'col2' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in Column 'struct(col1, col3)' is not unique",
                        "column": None,
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
                        "name": "col_col1_is_not_unique",
                        "message": "Value '1' in Column 'col1' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '2025-01-02 00:00:00' in Column 'col2' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in Column 'struct(col1, col3)' is not unique",
                        "column": None,
                        "filter": None,
                        "function": "is_unique",
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


def test_apply_checks_all_checks_as_yaml(ws, spark):
    """Test applying all checks from a yaml file.

    The checks used in the test are also showcased in the docs under /docs/reference/quality_rules.mdx
    The checks should be kept up to date with the docs to make sure the documentation examples are validated.
    """
    file_path = Path(__file__).parent.parent / "resources" / "all_checks.yaml"
    with open(file_path, "r", encoding="utf-8") as f:
        checks = yaml.safe_load(f)

    dq_engine = DQEngine(ws)
    status = dq_engine.validate_checks(checks)
    assert not status.has_errors

    schema = (
        "col1: string, col2: int, col3: int, col4 array<int>, col5: date, col6: timestamp, "
        "col7: map<string, int>, col8: struct<field1: int>"
    )
    test_df = spark.createDataFrame(
        [
            ["val1", 1, 1, [1], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 1, 0, 0), {"key1": 1}, {"field1": 1}],
            ["val2", 2, 2, [2], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 2, 0, 0), {"key1": 1}, {"field1": 1}],
            ["val3", 3, 3, [3], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 3, 0, 0), {"key1": 1}, {"field1": 1}],
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
                datetime(2025, 1, 2, 1, 0, 0),
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
                datetime(2025, 1, 2, 2, 0, 0),
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
                datetime(2025, 1, 2, 3, 0, 0),
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
        DQColRule(criticality="error", check_func=is_not_null, column="col1"),
        # is_not_empty check
        DQColRule(criticality="error", check_func=is_not_empty, column="col1"),
        # is_not_null_and_not_empty check
        DQColRule(
            criticality="error",
            check_func=is_not_null_and_not_empty,
            column="col1",
            check_func_kwargs={"trim_strings": True},
        ),
        # is_in_list check
        DQColRule(criticality="error", check_func=is_in_list, column="col2", check_func_args=[[1, 2, 3]]),
        # is_not_null_and_is_in_list check
        DQColRule(
            criticality="error", check_func=is_not_null_and_is_in_list, column="col2", check_func_args=[[1, 2, 3]]
        ),
        # is_not_null_and_not_empty_array check
        DQColRule(criticality="error", check_func=is_not_null_and_not_empty_array, column="col4"),
        # is_in_range check
        DQColRule(
            criticality="error",
            check_func=is_in_range,
            column="col2",
            check_func_kwargs={"min_limit": 1, "max_limit": 10},
        ),
        DQColRule(
            criticality="error",
            check_func=is_in_range,
            column="col5",
            check_func_kwargs={"min_limit": datetime(2025, 1, 1).date(), "max_limit": datetime(2025, 2, 24).date()},
        ),
        DQColRule(
            criticality="error",
            check_func=is_in_range,
            column="col6",
            check_func_kwargs={"min_limit": datetime(2025, 1, 1, 0, 0, 0), "max_limit": datetime(2025, 2, 24, 1, 0, 0)},
        ),
        DQColRule(
            criticality="error",
            check_func=is_in_range,
            column="col3",
            check_func_kwargs={"min_limit": "col2", "max_limit": "col2 * 2"},
        ),
        # is_not_in_range check
        DQColRule(
            criticality="error",
            check_func=is_not_in_range,
            column="col2",
            check_func_kwargs={"min_limit": 11, "max_limit": 20},
        ),
        DQColRule(
            criticality="error",
            check_func=is_not_in_range,
            column="col5",
            check_func_kwargs={"min_limit": datetime(2025, 2, 25).date(), "max_limit": datetime(2025, 2, 26).date()},
        ),
        DQColRule(
            criticality="error",
            check_func=is_not_in_range,
            column="col6",
            check_func_kwargs={
                "min_limit": datetime(2025, 2, 25, 0, 0, 0),
                "max_limit": datetime(2025, 2, 26, 1, 0, 0),
            },
        ),
        DQColRule(
            criticality="error",
            check_func=is_not_in_range,
            column="col3",
            check_func_kwargs={"min_limit": "col2 + 10", "max_limit": "col2 * 10"},
        ),
        # is_not_less_than check
        DQColRule(criticality="error", check_func=is_not_less_than, column="col2", check_func_kwargs={"limit": 0}),
        DQColRule(
            criticality="error",
            check_func=is_not_less_than,
            column="col5",
            check_func_kwargs={"limit": datetime(2025, 1, 1).date()},
        ),
        DQColRule(
            criticality="error",
            check_func=is_not_less_than,
            column="col6",
            check_func_kwargs={"limit": datetime(2025, 1, 1, 1, 0, 0)},
        ),
        DQColRule(
            criticality="error", check_func=is_not_less_than, column="col3", check_func_kwargs={"limit": "col2 - 10"}
        ),
        # is_not_greater_than check
        DQColRule(criticality="error", check_func=is_not_greater_than, column="col2", check_func_kwargs={"limit": 10}),
        DQColRule(
            criticality="error",
            check_func=is_not_greater_than,
            column="col5",
            check_func_kwargs={"limit": datetime(2025, 3, 1).date()},
        ),
        DQColRule(
            criticality="error",
            check_func=is_not_greater_than,
            column="col6",
            check_func_kwargs={"limit": datetime(2025, 3, 24, 1, 0, 0)},
        ),
        DQColRule(
            criticality="error",
            check_func=is_not_greater_than,
            column="col3",
            check_func_kwargs={"limit": "col2 + 10"},
        ),
        # is_valid_date check
        DQColRule(criticality="error", check_func=is_valid_date, column="col5"),
        DQColRule(
            criticality="error",
            check_func=is_valid_date,
            column="col5",
            check_func_kwargs={"date_format": "yyyy-MM-dd"},
            name="col5_is_not_valid_date2",
        ),
        # is_valid_timestamp check
        DQColRule(criticality="error", check_func=is_valid_timestamp, column="col6"),
        DQColRule(
            criticality="error",
            check_func=is_valid_timestamp,
            column="col6",
            check_func_kwargs={"timestamp_format": "yyyy-MM-dd HH:mm:ss"},
            name="col6_is_not_valid_timestamp2",
        ),
        # is_not_in_future check
        DQColRule(criticality="error", check_func=is_not_in_future, column="col6", check_func_kwargs={"offset": 86400}),
        # is_not_in_near_future check
        DQColRule(
            criticality="error", check_func=is_not_in_near_future, column="col6", check_func_kwargs={"offset": 36400}
        ),
        # is_older_than_n_days check
        DQColRule(
            criticality="error", check_func=is_older_than_n_days, column="col5", check_func_kwargs={"days": 10000}
        ),
        # is_older_than_col2_for_n_days check
        DQColRule(criticality="error", check_func=is_older_than_col2_for_n_days, check_func_args=["col5", "col6", 2]),
        # is_unique check defined using list of columns as string
        DQColRule(criticality="error", check_func=is_unique, check_func_kwargs={"columns": ["col1"]}),
        # is_unique check defined using list of columns
        DQColRule(criticality="error", check_func=is_unique, check_func_kwargs={"columns": [F.col("col1")]}),
        # is_unique for multiple columns (composite key), nulls as distinct (default behavior)
        # eg. (1, NULL) not equals (1, NULL) and (NULL, NULL) not equals (NULL, NULL)
        DQColRule(
            criticality="error",
            name="composite_key_col1_and_col2_is_not_unique",
            check_func=is_unique,
            check_func_kwargs={"columns": ["col1", "col2"]},
        ),
        # is_unique for multiple columns (composite key), nulls as not distinct
        # eg. (1, NULL) equals (1, NULL) and (NULL, NULL) equals (NULL, NULL)
        DQColRule(
            criticality="error",
            name="composite_key_col1_and_col2_is_not_unique_nulls_not_distinct",
            check_func=is_unique,
            check_func_kwargs={"columns": ["col1", "col2"], "nulls_distinct": False},
        ),
        # is_unique check with custom window
        DQColRule(
            criticality="error",
            name="col1_is_not_unique_custom_window",
            # provide default value for NULL in the time column of the window spec using coalesce()
            # to prevent rows exclusion!
            check_func=is_unique,
            check_func_kwargs={
                "columns": ["col1"],
                "window_spec": F.window(F.coalesce(F.col("col6"), F.lit(datetime(1970, 1, 1))), "10 minutes"),
            },
        ),
        # regex_match check
        DQColRule(
            criticality="error",
            check_func=regex_match,
            column="col2",
            check_func_kwargs={"regex": "[0-9]+", "negate": False},
        ),
        # sql_expression check
        DQColRule(
            criticality="error",
            check_func=sql_expression,
            check_func_kwargs={
                "expression": "col3 >= col2 and col3 <= 10",
                "msg": "col3 is less than col2 and col3 is greater than 10",
                "name": "custom_output_name",
                "negate": False,
            },
        ),
        # is_not_null check applied to a struct column element (dot notation)
        DQColRule(
            criticality="error",
            check_func=is_not_null,
            column="col8.field1",
        ),
        # is_not_null check applied to a map column element
        DQColRule(
            criticality="error",
            check_func=is_not_null,
            column=F.try_element_at("col7", F.lit("key1")),
        ),
        # is_not_null check applied to an array column element at the specified position
        DQColRule(
            criticality="error",
            check_func=is_not_null,
            column=F.try_element_at("col4", F.lit(1)),
        ),
        # is_not_greater_than check applied to an array column
        DQColRule(
            criticality="error",
            check_func=is_not_greater_than,
            column=F.array_max("col4"),
            check_func_kwargs={"limit": 10},
        ),
        # is_not_less_than check applied to an array column
        DQColRule(
            criticality="error",
            check_func=is_not_less_than,
            column=F.array_min("col4"),
            check_func_kwargs={"limit": 1},
        ),
        # sql_expression check applied to a map column element
        DQColRule(
            criticality="error",
            check_func=sql_expression,
            check_func_kwargs={
                "expression": "try_element_at(col7, 'key1') < 10",
                "msg": "col7 element 'key1' is less than 10",
                "name": "col7_element_key1_less_than_10",
                "negate": False,
            },
        ),
        # sql_expression check applied to an array of map column elements
        DQColRule(
            criticality="error",
            check_func=sql_expression,
            check_func_kwargs={
                "expression": "not exists(col4, x -> x >= 10)",
                "msg": "array col4 has an element greater than 10",
                "name": "col4_all_elements_less_than_10",
                "negate": False,
            },
        ),
    ] + DQColSetRule(  # apply check to multiple columns (simple col, map and array)
        check_func=is_not_null,
        criticality="error",
        columns=[
            "col1",  # col as string
            F.col("col2"),  # col
            "col8.field1",  # struct col
            F.try_element_at("col7", F.lit("key1")),  # map col
            F.try_element_at("col4", F.lit(1)),  # array col
        ],
    ).get_rules()

    dq_engine = DQEngine(ws)

    schema = (
        "col1: string, col2: int, col3: int, col4 array<int>, col5: date, col6: timestamp, "
        "col7: map<string, int>, col8: struct<field1: int>"
    )
    test_df = spark.createDataFrame(
        [
            ["val1", 1, 1, [1], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 1, 0, 0), {"key1": 1}, {"field1": 1}],
            ["val2", 2, 2, [2], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 2, 0, 0), {"key1": 1}, {"field1": 1}],
            ["val3", 3, 3, [3], datetime(2025, 1, 2).date(), datetime(2025, 1, 2, 3, 0, 0), {"key1": 1}, {"field1": 1}],
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
                datetime(2025, 1, 2, 1, 0, 0),
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
                datetime(2025, 1, 2, 2, 0, 0),
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
                datetime(2025, 1, 2, 3, 0, 0),
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
        DQColRule(
            name="col_a_is_null_or_empty",
            criticality="error",
            check_func=is_not_null_and_not_empty,
            column="a",
        ),
        DQColRule(
            name="col_a_is_null",
            criticality="error",
            check_func=is_not_null,
            column="a",
            filter="b = 1",
        ),
        DQColRule(
            name="col_a_is_null_or_empty",
            criticality="warn",
            check_func=is_not_null_and_not_empty,
            column="a",
        ),
        DQColRule(
            name="col_a_is_null",
            criticality="warn",
            check_func=is_not_null,
            column="a",
            filter="b = 1",
        ),
    ]

    checked = dq_engine.apply_checks(test_df, checks)

    result_errors = checked.select(F.explode(F.col("_errors")).alias("dq")).select(F.expr("dq.*"))
    result_warnings = checked.select(F.explode(F.col("_warnings")).alias("dq")).select(F.expr("dq.*"))

    expected_schema = (
        "name: string, message: string, column: string, filter: string, function: string, run_time: timestamp, "
        "user_metadata: map<string, string>"
    )
    expected = spark.createDataFrame(
        [
            [
                "col_a_is_null_or_empty",
                "Column 'a' value is null or empty",
                "a",
                None,
                "is_not_null_and_not_empty",
                RUN_TIME,
                user_metadata,
            ],
            [
                "col_a_is_null",
                "Column 'a' value is null",
                "a",
                "b = 1",
                "is_not_null",
                RUN_TIME,
                user_metadata,
            ],
        ],
        expected_schema,
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
            "name": "col_map_element_at_col1_key1_is_not_greater_than_10",
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
                        "name": "col_map_element_at_col1_key1_is_not_greater_than_10",
                        "message": "Value is not matching expression: try_element_at(col1, 'key1') < 10",
                        "column": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_not_exists_col2_x_x_key1_10",
                        "message": "Value is not matching expression: not exists(col2, x -> x.key1 >= 10)",
                        "column": None,
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
            "name": "col_map_element_at_col1_key1_is_not_greater_than_5",
            "check": {
                "function": "is_not_greater_than",
                "arguments": {"column": "try_element_at(col1, 'key1')", "limit": 5},
            },
        },
        {
            "criticality": "error",
            "name": "col_array_element_at_position_2_key1_is_not_greater_than_5",
            "check": {
                "function": "is_not_greater_than",
                "arguments": {"column": "try_element_at(try_element_at(col2, 2), 'key1')", "limit": 5},
            },
        },
        {  # map key does not exist
            "criticality": "error",
            "name": "col_map_element_at_col1_not_exists_is_not_greater_than_5",
            "check": {
                "function": "is_not_greater_than",
                "arguments": {"column": "try_element_at(col1, 'not_exists')", "limit": 5},
            },
        },
        {  # element does not exist at the given position
            "criticality": "error",
            "name": "col_array_element_at_position_1000_key1_is_not_greater_than_5",
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
                        "name": "col_map_element_at_col1_key1_is_not_greater_than_5",
                        "message": "Value '10' in Column 'try_element_at(col1, 'key1')' is greater than limit: 5",
                        "column": "try_element_at(col1, 'key1')",
                        "filter": None,
                        "function": "is_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_array_element_at_position_2_key1_is_not_greater_than_5",
                        "message": "Value '10' in Column 'try_element_at(try_element_at(col2, 2), 'key1')' is greater than limit: 5",
                        "column": "try_element_at(try_element_at(col2, 2), 'key1')",
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
        DQColRule(
            criticality="error",
            name="col_map_element_at_col1_key1_is_not_greater_than_5",
            check_func=is_not_greater_than,
            column=F.try_element_at("col1", F.lit("key1")),
            check_func_kwargs={"limit": 5},
        ),
        DQColRule(
            criticality="error",
            name="col_array_element_at_position_2_key1_is_not_greater_than_5",
            check_func=is_not_greater_than,
            column=F.try_element_at(F.try_element_at("col2", F.lit(2)), F.lit("key1")),
            check_func_kwargs={"limit": 5},
        ),
        DQColRule(
            criticality="error",
            name="col_map_element_at_col1_not_exists_is_not_greater_than_5",
            check_func=is_not_greater_than,
            column=F.try_element_at("col1", F.lit("not_exists")),
            check_func_kwargs={"limit": 5},
        ),
        DQColRule(
            criticality="error",
            name="col_array_element_at_position_1000_key1_is_not_greater_than_5",
            check_func=is_not_greater_than,
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
                        "name": "col_map_element_at_col1_key1_is_not_greater_than_5",
                        "message": "Value '10' in Column 'try_element_at(col1, key1)' is greater than limit: 5",
                        "column": "try_element_at(col1, key1)",
                        "filter": None,
                        "function": "is_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_array_element_at_position_2_key1_is_not_greater_than_5",
                        "message": "Value '10' in Column 'try_element_at(try_element_at(col2, 2), key1)' is greater than limit: 5",
                        "column": "try_element_at(try_element_at(col2, 2), key1)",
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
