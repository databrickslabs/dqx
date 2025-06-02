from datetime import datetime
from pathlib import Path
import yaml
import pyspark.sql.functions as F
import pytest
from pyspark.sql import Column
from chispa.dataframe_comparer import assert_df_equality  # type: ignore
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import (
    ExtraParams,
    DQRowRuleForEachCol,
    ColumnArguments,
    register_rule,
    DQRowRule,
)
from databricks.labs.dqx.schema import dq_result_schema
from databricks.labs.dqx import row_checks


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
            name="col_a_is_null_or_empty",
            criticality="warn",
            check_func=row_checks.is_not_null_and_not_empty,
            column="a",
        ),
        DQRowRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty,
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
        DQRowRule(
            name="col_a_is_null_or_empty",
            criticality="warn",
            check_func=row_checks.is_not_null_and_not_empty,
            column="a",
            user_metadata={"tag1": "value11", "tag2": "value21"},
        ),
        DQRowRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty,
            column="b",
            user_metadata={"tag1": "value12", "tag2": "value22"},
        ),
        DQRowRule(
            name="col_c_is_null_or_empty",
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty,
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
                        "name": "col_b_is_null_or_empty",
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
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value12", "tag2": "value22"},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
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
            name="col_c_is_null_or_empty",
            criticality="invalid",
            check_func=row_checks.is_not_null_and_not_empty,
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
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_null",
                        "message": "Column 'col2' value is null",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col3_is_null",
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
        DQRowRule(criticality="error", check_func=row_checks.is_not_null, column="col1"),
        DQRowRule(
            # missing criticality, default to "error"
            check_func=row_checks.is_not_null,
            column="col2",
        ),
    ] + DQRowRuleForEachCol(
        # missing criticality, default to "error"
        check_func=row_checks.is_not_null,
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
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_null",
                        "message": "Column 'col2' value is null",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col3_is_null",
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
        DQRowRule(criticality="warn", check_func=row_checks.is_not_null_and_not_empty, column="a"),
        DQRowRule(criticality="error", check_func=row_checks.is_not_null_and_not_empty, column="b"),
        DQRowRule(criticality="error", check_func=row_checks.is_not_null_and_not_empty, column="c"),
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
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
                        "message": "Column 'b' value is null or empty",
                        "columns": ["b"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
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
            name="col_a_is_null_or_empty",
            criticality="warn",
            check_func=row_checks.is_not_null_and_not_empty,
            column="a",
        ),
        DQRowRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty,
            column="b",
        ),
        DQRowRule(
            name="col_c_is_null_or_empty",
            criticality="warn",
            check_func=row_checks.is_not_null_and_not_empty,
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
                        "name": "col_b_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
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
                        "columns": ["b"],
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
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
                        "name": "col_a_is_not_in_the_list",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
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
                        "name": "col_a_is_not_in_the_list",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_b_is_null_or_empty",
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
                        "name": "col_a_is_null_or_empty",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_c_is_null_or_empty",
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

    checks = DQRowRuleForEachCol(
        check_func=row_checks.is_not_null_and_not_empty, criticality="warn", filter="b>3", columns=["a", "c"]
    ).get_rules() + [
        DQRowRule(
            name="col_b_is_null_or_empty",
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty,
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
                        "name": "col_c_is_null_or_empty",
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
        DQRowRuleForEachCol(
            name="common_name", check_func=row_checks.is_not_null, criticality="warn", columns=["a", "b"]
        ).get_rules()
        + DQRowRuleForEachCol(
            name="common_name2",
            check_func=row_checks.is_unique,
            criticality="warn",
            columns=[["a", "b"], ["c"]],
            check_func_kwargs={"nulls_distinct": False},
        ).get_rules()
    )

    checked = dq_engine.apply_checks(test_df, checks)

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
                        "message": "Value '{null}' in Column 'struct(c)' is not unique",
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
                        "message": "Value '{null}' in Column 'struct(c)' is not unique",
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
                        "name": "col_b_is_null_or_empty",
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
                        "name": "col_c_is_null_or_empty",
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
                        "name": "col_col2_is_null",
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
                        "name": "col_col2_is_null",
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


def custom_check_func_global(column: str) -> Column:
    col_expr = F.col(column)
    return row_checks.make_condition(col_expr.isNull(), "custom check failed", f"{column}_is_null_custom")


@register_rule("single_column")
def custom_check_func_global_annotated(column: str) -> Column:
    col_expr = F.col(column)
    return row_checks.make_condition(col_expr.isNull(), "custom check annotated failed", f"{column}_is_null_custom")


def test_apply_checks_with_custom_check(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRowRule(criticality="warn", check_func=row_checks.is_not_null_and_not_empty, column="a"),
        DQRowRule(criticality="warn", check_func=custom_check_func_global, column="a"),
        DQRowRule(criticality="warn", check_func=custom_check_func_global_annotated, column="a"),
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
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check annotated failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_check_func_global_annotated",
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
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check annotated failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_check_func_global_annotated",
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
        {
            "criticality": "warn",
            "check": {"function": "custom_check_func_global_annotated", "arguments": {"column": "a"}},
        },
    ]

    checked = dq_engine.apply_checks_by_metadata(
        test_df,
        checks,
        {
            "custom_check_func_global": custom_check_func_global,
            "custom_check_func_global_annotated": custom_check_func_global_annotated,
        },
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
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check annotated failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_check_func_global_annotated",
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
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_check_func_global",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_a_is_null_custom",
                        "message": "custom check annotated failed",
                        "columns": ["a"],
                        "filter": None,
                        "function": "custom_check_func_global_annotated",
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
                        "columns": ["a"],
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
                        "columns": ["b"],
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
                        "columns": ["a"],
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
                        "columns": ["b"],
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
                        "columns": ["a"],
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
                        "columns": ["b"],
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
                        "name": "col_a_is_null_or_empty",
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
                            "name": "col_a_is_null_or_empty",
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
                            "name": "col_b_is_null_or_empty",
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
                            "name": "col_a_is_null_or_empty",
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
                            "name": "col_a_is_null_or_empty",
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
                            "name": "col_b_is_null_or_empty",
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
                            "name": "col_a_is_null_or_empty",
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
                        "columns": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_not_like_val",
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
                        "name": "col_struct_col1_is_not_unique",
                        "message": "Value '{1}' in Column 'struct(col1)' is not unique",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '{2025-01-01 00:00:00}' in Column 'struct(col2)' is not unique",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in Column 'struct(col1, col3)' is not unique",
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
                        "name": "col_struct_col1_is_not_unique",
                        "message": "Value '{1}' in Column 'struct(col1)' is not unique",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '{2025-01-02 00:00:00}' in Column 'struct(col2)' is not unique",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in Column 'struct(col1, col3)' is not unique",
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
                        "name": "col_struct_col1_is_not_unique",
                        "message": "Value '{2}' in Column 'struct(col1)' is not unique",
                        "columns": ["col1"],
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
                "c",
                [
                    {
                        "name": "col_struct_col1_is_not_unique",
                        "message": "Value '{2}' in Column 'struct(col1)' is not unique",
                        "columns": ["col1"],
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
                        "name": "col_struct_col1_is_not_unique",
                        "message": "Value '{null}' in Column 'struct(col1)' is not unique",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '{null}' in Column 'struct(col2)' is not unique",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col2_is_not_unique",
                        "message": "Value '{null, null}' in Column 'struct(col1, col2)' is not unique",
                        "columns": ["col1", "col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{null, }' in Column 'struct(col1, col3)' is not unique",
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
                        "name": "col_struct_col1_is_not_unique",
                        "message": "Value '{null}' in Column 'struct(col1)' is not unique",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '{null}' in Column 'struct(col2)' is not unique",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col2_is_not_unique",
                        "message": "Value '{null, null}' in Column 'struct(col1, col2)' is not unique",
                        "columns": ["col1", "col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{null, }' in Column 'struct(col1, col3)' is not unique",
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
                datetime(2025, 1, 1),
                "a",
                [
                    {
                        "name": "col_struct_col1_is_not_unique",
                        "message": "Value '{1}' in Column 'struct(col1)' is not unique",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '{2025-01-01 00:00:00}' in Column 'struct(col2)' is not unique",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in Column 'struct(col1, col3)' is not unique",
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
                        "name": "col_struct_col1_is_not_unique",
                        "message": "Value '{1}' in Column 'struct(col1)' is not unique",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '{2025-01-02 00:00:00}' in Column 'struct(col2)' is not unique",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col3_is_not_unique",
                        "message": "Value '{1, a}' in Column 'struct(col1, col3)' is not unique",
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
                        "name": "col_struct_col1_is_not_unique",
                        "message": "Value '{2}' in Column 'struct(col1)' is not unique",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '{null}' in Column 'struct(col2)' is not unique",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col2_is_not_unique",
                        "message": "Value '{2, null}' in Column 'struct(col1, col2)' is not unique",
                        "columns": ["col1", "col2"],
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
                "c",
                [
                    {
                        "name": "col_struct_col1_is_not_unique",
                        "message": "Value '{2}' in Column 'struct(col1)' is not unique",
                        "columns": ["col1"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_col2_is_not_unique",
                        "message": "Value '{null}' in Column 'struct(col2)' is not unique",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_unique",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "composite_key_col1_and_col2_is_not_unique",
                        "message": "Value '{2, null}' in Column 'struct(col1, col2)' is not unique",
                        "columns": ["col1", "col2"],
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
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_null,
            column="col1",
            user_metadata={"tag1": "value1", "tag2": "001"},
        ),
        # is_not_empty check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_empty,
            column="col1",
            user_metadata={"tag1": "value1", "tag2": "002"},
        ),
        # is_not_null_and_not_empty check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty,
            column="col1",
            check_func_kwargs={"trim_strings": True},
            user_metadata={"tag1": "value1", "tag2": "003"},
        ),
        # is_in_list check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_in_list,
            column="col2",
            check_func_kwargs={"allowed": [1, 2, 3]},
            user_metadata={"tag1": "value1", "tag2": "004"},
        ),
        # is_not_null_and_is_in_list check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_null_and_is_in_list,
            column="col2",
            check_func_kwargs={"allowed": [1, 2, 3]},
            user_metadata={"tag1": "value1", "tag2": "005"},
        ),
        # is_not_null_and_not_empty_array check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty_array,
            column="col4",
            user_metadata={"tag1": "value1", "tag2": "006"},
        ),
        # is_not_null_and_not_empty check, use args to pass arguments
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty,
            column="col1",
            check_func_kwargs={"trim_strings": True},
            user_metadata={"tag1": "value1", "tag2": "007"},
        ),
        # is_in_range check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_in_range,
            column="col2",
            check_func_kwargs={"min_limit": 1, "max_limit": 10},
            user_metadata={"tag1": "value1", "tag2": "008"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_in_range,
            column="col5",
            check_func_kwargs={"min_limit": datetime(2025, 1, 1).date(), "max_limit": datetime(2025, 2, 24).date()},
            user_metadata={"tag1": "value1", "tag2": "009"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_in_range,
            column="col6",
            check_func_kwargs={"min_limit": datetime(2025, 1, 1, 0, 0, 0), "max_limit": datetime(2025, 2, 24, 1, 0, 0)},
            user_metadata={"tag1": "value1", "tag2": "010"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_in_range,
            column="col3",
            check_func_kwargs={"min_limit": "col2", "max_limit": "col2 * 2"},
            user_metadata={"tag1": "value2", "tag2": "011"},
        ),
        # is_not_in_range check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_in_range,
            column="col2",
            check_func_kwargs={"min_limit": 11, "max_limit": 20},
            user_metadata={"tag1": "value2", "tag2": "012"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_in_range,
            column="col5",
            check_func_kwargs={"min_limit": datetime(2025, 2, 25).date(), "max_limit": datetime(2025, 2, 26).date()},
            user_metadata={"tag1": "value2", "tag2": "013"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_in_range,
            column="col6",
            check_func_kwargs={
                "min_limit": datetime(2025, 2, 25, 0, 0, 0),
                "max_limit": datetime(2025, 2, 26, 1, 0, 0),
            },
            user_metadata={"tag1": "value2", "tag2": "014"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_in_range,
            column="col3",
            check_func_kwargs={"min_limit": "col2 + 10", "max_limit": "col2 * 10"},
            user_metadata={"tag1": "value2", "tag2": "015"},
        ),
        # is_not_less_than check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_less_than,
            column="col2",
            check_func_kwargs={"limit": 0},
            user_metadata={"tag1": "value2", "tag2": "016"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_less_than,
            column="col5",
            check_func_kwargs={"limit": datetime(2025, 1, 1).date()},
            user_metadata={"tag1": "value2", "tag2": "017"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_less_than,
            column="col6",
            check_func_kwargs={"limit": datetime(2025, 1, 1, 1, 0, 0)},
            user_metadata={"tag1": "value2", "tag2": "018"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_less_than,
            column="col3",
            check_func_kwargs={"limit": "col2 - 10"},
            user_metadata={"tag1": "value2", "tag2": "019"},
        ),
        # is_not_greater_than check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_greater_than,
            column="col2",
            check_func_kwargs={"limit": 10},
            user_metadata={"tag1": "value3", "tag2": "020"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_greater_than,
            column="col5",
            check_func_kwargs={"limit": datetime(2025, 3, 1).date()},
            user_metadata={"tag1": "value3", "tag2": "021"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_greater_than,
            column="col6",
            check_func_kwargs={"limit": datetime(2025, 3, 24, 1, 0, 0)},
            user_metadata={"tag1": "value3", "tag2": "022"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_greater_than,
            column="col3",
            check_func_kwargs={"limit": "col2 + 10"},
            user_metadata={"tag1": "value3", "tag2": "023"},
        ),
        # is_valid_date check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_valid_date,
            column="col5",
            user_metadata={"tag1": "value3", "tag2": "024"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_valid_date,
            column="col5",
            check_func_kwargs={"date_format": "yyyy-MM-dd"},
            name="col5_is_not_valid_date2",
            user_metadata={"tag1": "value3", "tag2": "025"},
        ),
        # is_valid_timestamp check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_valid_timestamp,
            column="col6",
            user_metadata={"tag1": "value3", "tag2": "026"},
        ),
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_valid_timestamp,
            column="col6",
            check_func_kwargs={"timestamp_format": "yyyy-MM-dd HH:mm:ss"},
            name="col6_is_not_valid_timestamp2",
            user_metadata={"tag1": "value3", "tag2": "027"},
        ),
        # is_not_in_future check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_in_future,
            column="col6",
            check_func_kwargs={"offset": 86400},
            user_metadata={"tag1": "value3", "tag2": "028"},
        ),
        # is_not_in_near_future check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_in_near_future,
            column="col6",
            check_func_kwargs={"offset": 36400},
            user_metadata={"tag1": "value3", "tag2": "029"},
        ),
        # is_older_than_n_days check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_older_than_n_days,
            column="col5",
            check_func_kwargs={"days": 10000},
            user_metadata={"tag1": "value4"},
        ),
        # is_older_than_col2_for_n_days check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_older_than_col2_for_n_days,
            check_func_kwargs={"column1": "col5", "column2": "col6", "days": 2},
            user_metadata={"tag1": "value4"},
        ),
        # is_unique check
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_unique,
            columns=["col1"],  # this check require list of columns
            user_metadata={"tag1": "value4"},
        ),
        # is_unique check defined using list of columns
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_unique,
            columns=[F.col("col1")],  # this check require list of columns
            user_metadata={"tag1": "value4"},
        ),
        # is_unique on multiple columns (composite key), nulls are distinct (default behavior)
        # eg. (1, NULL) not equals (1, NULL) and (NULL, NULL) not equals (NULL, NULL)
        DQRowRule(
            criticality="error",
            name="composite_key_col1_and_col2_is_not_unique",
            check_func=row_checks.is_unique,
            columns=["col1", "col2"],
            user_metadata={"tag1": "value4"},
        ),
        # is_unique on multiple columns (composite key), nulls are not distinct
        # eg. (1, NULL) equals (1, NULL) and (NULL, NULL) equals (NULL, NULL)
        DQRowRule(
            criticality="error",
            name="composite_key_col1_and_col2_is_not_unique_nulls_not_distinct",
            check_func=row_checks.is_unique,
            columns=["col1", "col2"],
            check_func_kwargs={"nulls_distinct": False},
            user_metadata={"tag1": "value4"},
        ),
        # is_unique check with custom window
        DQRowRule(
            criticality="error",
            name="col1_is_not_unique_custom_window",
            # provide default value for NULL in the time column of the window spec using coalesce()
            # to prevent rows exclusion!
            check_func=row_checks.is_unique,
            columns=["col1"],
            check_func_kwargs={
                "window_spec": F.window(F.coalesce(F.col("col6"), F.lit(datetime(1970, 1, 1))), "10 minutes"),
            },
            user_metadata={"tag1": "value5"},
        ),
        # regex_match check
        DQRowRule(
            criticality="error",
            check_func=row_checks.regex_match,
            column="col2",
            check_func_kwargs={"regex": "[0-9]+", "negate": False},
            user_metadata={"tag1": "value5"},
        ),
        # sql_expression check
        DQRowRule(
            criticality="error",
            check_func=row_checks.sql_expression,
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
        + DQRowRuleForEachCol(
            check_func=row_checks.is_not_null,  # 'column' as first argument
            criticality="error",
            columns=["col3", "col5"],  # apply the check for each column in the list
            user_metadata={"tag1": "multi column"},
        ).get_rules()
        + DQRowRuleForEachCol(
            check_func=row_checks.is_unique,  # 'columns' as first argument
            criticality="error",
            columns=[["col3", "col5"], ["col1"]],  # apply the check for each list of columns
            user_metadata={"tag1": "multi column"},
        ).get_rules()
    )

    checks = checks + [
        # is_not_null check applied to a struct column element (dot notation)
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_null,
            column="col8.field1",
        ),
        # is_not_null check applied to a map column element
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_null,
            column=F.try_element_at("col7", F.lit("key1")),
        ),
        # is_not_null check applied to an array column element at the specified position
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_null,
            column=F.try_element_at("col4", F.lit(1)),
        ),
        # is_not_greater_than check applied to an array column
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_greater_than,
            column=F.array_max("col4"),
            check_func_kwargs={"limit": 10},
        ),
        # is_not_less_than check applied to an array column
        DQRowRule(
            criticality="error",
            check_func=row_checks.is_not_less_than,
            column=F.array_min("col4"),
            check_func_kwargs={"limit": 1},
        ),
        # sql_expression check applied to a map column element
        DQRowRule(
            criticality="error",
            check_func=row_checks.sql_expression,
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
            check_func=row_checks.sql_expression,
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
        + DQRowRuleForEachCol(
            check_func=row_checks.is_not_null,
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
        DQRowRule(
            name="col_a_is_null_or_empty",
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty,
            column="a",
        ),
        DQRowRule(
            name="col_a_is_null",
            criticality="error",
            check_func=row_checks.is_not_null,
            column="a",
            filter="b = 1",
        ),
        DQRowRule(
            name="col_a_is_null_or_empty",
            criticality="warn",
            check_func=row_checks.is_not_null_and_not_empty,
            column="a",
        ),
        DQRowRule(
            name="col_a_is_null",
            criticality="warn",
            check_func=row_checks.is_not_null,
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
                "col_a_is_null_or_empty",
                "Column 'a' value is null or empty",
                ["a"],
                None,
                "is_not_null_and_not_empty",
                RUN_TIME,
                user_metadata,
            ],
            [
                "col_a_is_null",
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
                        "columns": None,
                        "filter": None,
                        "function": "sql_expression",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_not_exists_col2_x_x_key1_10",
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
                        "columns": ["try_element_at(col1, 'key1')"],
                        "filter": None,
                        "function": "is_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_array_element_at_position_2_key1_is_not_greater_than_5",
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
            name="col_map_element_at_col1_key1_is_not_greater_than_5",
            check_func=row_checks.is_not_greater_than,
            column=F.try_element_at("col1", F.lit("key1")),
            check_func_kwargs={"limit": 5},
        ),
        DQRowRule(
            criticality="error",
            name="col_array_element_at_position_2_key1_is_not_greater_than_5",
            check_func=row_checks.is_not_greater_than,
            column=F.try_element_at(F.try_element_at("col2", F.lit(2)), F.lit("key1")),
            check_func_kwargs={"limit": 5},
        ),
        DQRowRule(
            criticality="error",
            name="col_map_element_at_col1_not_exists_is_not_greater_than_5",
            check_func=row_checks.is_not_greater_than,
            column=F.try_element_at("col1", F.lit("not_exists")),
            check_func_kwargs={"limit": 5},
        ),
        DQRowRule(
            criticality="error",
            name="col_array_element_at_position_1000_key1_is_not_greater_than_5",
            check_func=row_checks.is_not_greater_than,
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
                        "columns": ["try_element_at(col1, key1)"],
                        "filter": None,
                        "function": "is_not_greater_than",
                        "run_time": RUN_TIME,
                        "user_metadata": {},
                    },
                    {
                        "name": "col_array_element_at_position_2_key1_is_not_greater_than_5",
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


def test_apply_checks_with_check_metadata_from_config(ws, spark):
    extra_params = ExtraParams(run_time=RUN_TIME, user_metadata={"tag2": "from_engine", "tag3": "from_engine"})
    dq_engine = DQEngine(workspace_client=ws, extra_params=extra_params)
    schema = "col1: string, col2: string"
    test_df = spark.createDataFrame([["str1", "str2"], [None, "val2"], ["val1", ""], [None, None]], schema)

    checks = [
        {
            "name": "col1_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
            "user_metadata": {"tag1": "value1", "tag2": "value1"},
        },
        {
            "name": "col2_is_null",
            "criticality": "error",
            "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "col2"}},
            "user_metadata": {"tag1": "value2", "tag2": "value1"},
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
                        "user_metadata": {"tag1": "value1", "tag2": "value1", "tag3": "from_engine"},
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
                        "user_metadata": {"tag1": "value2", "tag2": "value1", "tag3": "from_engine"},
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
                        "user_metadata": {"tag1": "value1", "tag2": "value1", "tag3": "from_engine"},
                    },
                    {
                        "name": "col2_is_null",
                        "message": "Column 'col2' value is null or empty",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value2", "tag2": "value1", "tag3": "from_engine"},
                    },
                ],
                None,
            ],
        ],
        expected_schema,
    )

    actual_df = dq_engine.apply_checks_by_metadata(test_df, checks)
    assert_df_equality(actual_df, expected_df)


def test_apply_checks_with_check_metadata_from_classes(ws, spark):
    extra_params = ExtraParams(run_time=RUN_TIME, user_metadata={"tag2": "from_engine", "tag3": "from_engine"})
    dq_engine = DQEngine(workspace_client=ws, extra_params=extra_params)
    schema = "col1: string, col2: string"
    test_df = spark.createDataFrame([["str1", "str2"], [None, "val2"], ["val1", ""], [None, None]], schema)

    checks = [
        DQRowRule(
            name="col1_is_null",
            criticality="error",
            check_func=row_checks.is_not_null,
            column="col1",
            user_metadata={"tag1": "value1", "tag2": "value1"},
        ),
        DQRowRule(
            name="col2_is_null",
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty,
            column="col2",
            user_metadata={"tag1": "value2", "tag2": "value1"},
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
                        "user_metadata": {"tag1": "value1", "tag2": "value1", "tag3": "from_engine"},
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
                        "user_metadata": {"tag1": "value2", "tag2": "value1", "tag3": "from_engine"},
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
                        "user_metadata": {"tag1": "value1", "tag2": "value1", "tag3": "from_engine"},
                    },
                    {
                        "name": "col2_is_null",
                        "message": "Column 'col2' value is null or empty",
                        "columns": ["col2"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "user_metadata": {"tag1": "value2", "tag2": "value1", "tag3": "from_engine"},
                    },
                ],
                None,
            ],
        ],
        expected_schema,
    )

    actual_df = dq_engine.apply_checks(test_df, checks)
    assert_df_equality(actual_df, expected_df)


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
            check_func=row_checks.is_not_null,
            column="col1",
            user_metadata={"tag1": "value1", "tag2": "from_check"},
        ),
        DQRowRule(
            name="col2_is_null",
            criticality="error",
            check_func=row_checks.is_not_null_and_not_empty,
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
