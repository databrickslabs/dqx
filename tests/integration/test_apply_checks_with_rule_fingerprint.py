from chispa import assert_df_equality  # type: ignore [import-untyped]
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule
from databricks.labs.dqx import check_funcs
from tests.integration.conftest import (
    REPORTING_COLUMNS,
    RUN_TIME,
    EXTRA_PARAMS,
    RUN_ID,
    generate_checks_with_rule_and_set_fingerprint,
    get_rule_fingerprint_from_checks,
    get_rule_set_fingerprint_from_checks,
)

SCHEMA = "a: int, b: int, c: int"
EXPECTED_SCHEMA = SCHEMA + REPORTING_COLUMNS


def test_apply_checks_with_fingerprints(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        DQRowRule(
            name="a_is_null",
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
    versioning_rules_checks = generate_checks_with_rule_and_set_fingerprint(checks)

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
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "b_is_null_or_empty", "error"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
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
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "c_is_null_or_empty", "error"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
                        "user_metadata": {"tag1": "value13", "tag2": "value23"},
                    }
                ],
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "a_is_null", "warn"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
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
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "b_is_null_or_empty", "error"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
                        "user_metadata": {"tag1": "value12", "tag2": "value22"},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "c_is_null_or_empty", "error"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
                        "user_metadata": {"tag1": "value13", "tag2": "value23"},
                    },
                ],
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "a_is_null", "warn"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
                        "user_metadata": {"tag1": "value11", "tag2": "value21"},
                    }
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_and_split_by_metadata_with_fingerprints(ws, spark):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None], [None, None, None]], SCHEMA)

    checks = [
        {
            "name": "a_is_null",
            "criticality": "error",
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
    versioning_rules_checks = generate_checks_with_rule_and_set_fingerprint(checks)
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
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "b_is_null_or_empty", "error"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
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
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "a_is_not_in_the_list", "warn"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
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
                        "name": "a_is_null",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "a_is_null", "error"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "c_is_null_or_empty", "warn"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
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
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "b_is_null_or_empty", "error"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
                        "user_metadata": {},
                    }
                ],
                [
                    {
                        "name": "a_is_null",
                        "message": "Column 'a' value is null or empty",
                        "columns": ["a"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "a_is_null", "error"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
                        "user_metadata": {},
                    },
                    {
                        "name": "c_is_null_or_empty",
                        "message": "Column 'c' value is null or empty",
                        "columns": ["c"],
                        "filter": None,
                        "function": "is_not_null_and_not_empty",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "rule_fingerprint": get_rule_fingerprint_from_checks(
                            versioning_rules_checks, "c_is_null_or_empty", "warn"
                        ),
                        "rule_set_fingerprint": get_rule_set_fingerprint_from_checks(versioning_rules_checks),
                        "user_metadata": {},
                    },
                ],
            ],
        ],
        EXPECTED_SCHEMA,
    )

    assert_df_equality(bad, expected_bad, ignore_nullable=True)
