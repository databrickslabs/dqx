import dataclasses
from databricks.labs.dqx.engine import DQEngine, DQEngineCore
from databricks.labs.dqx.config import FileChecksStorageConfig
from tests.integration.conftest import (
    REPORTING_COLUMNS,
    RUN_TIME,
    EXTRA_PARAMS,
    RUN_ID,
    assert_df_equality_ignore_fingerprints as assert_df_equality,
)

SCHEMA = "a: int, b: int, c: int"
EXPECTED_SCHEMA = SCHEMA + REPORTING_COLUMNS


def test_apply_checks_by_metadata_with_variables(ws, spark, tmp_path):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None]], SCHEMA)

    checks_yaml = """
        - criticality: error
          check:
            function: is_not_null_and_not_empty
            arguments:
              column: "{{ col }}"
        """
    checks_file = tmp_path / "checks.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")
    checks = DQEngineCore.load_checks_from_local_file(str(checks_file), variables={"col": "b"})

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
                        "run_id": RUN_ID,
                        "user_metadata": {},
                    }
                ],
                None,
            ],
            [None, 4, None, None, None],
        ],
        EXPECTED_SCHEMA,
    )
    assert_df_equality(checked, expected, ignore_nullable=True)


def test_apply_checks_by_metadata_and_split_with_variables(ws, spark, tmp_path):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None]], SCHEMA)

    checks_yaml = """
        - criticality: error
          name: "{{ col }}_null_check"
          check:
            function: is_not_null_and_not_empty
            arguments:
              column: "{{ col }}"
        - criticality: warn
          check:
            function: sql_expression
            arguments:
              expression: "{{ expr_col }} > {{ threshold }}"
        """
    checks_file = tmp_path / "checks.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")
    checks = DQEngineCore.load_checks_from_local_file(
        str(checks_file), variables={"col": "b", "expr_col": "a", "threshold": 1}
    )

    good, bad = dq_engine.apply_checks_by_metadata_and_split(test_df, checks)

    # Row [1, 3, 3]: b is not null, a > 1 passes -> good only
    # Row [2, None, 4]: b is null (error), a > 1 passes -> bad only
    # Row [None, 4, None]: b is not null, a is null so "a > 1" fails (warn) -> both good and bad
    assert good.count() == 2
    assert bad.count() == 2


def test_apply_checks_by_metadata_with_variables_name_and_filter(ws, spark, tmp_path):
    dq_engine = DQEngine(workspace_client=ws, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None]], SCHEMA)

    checks_yaml = """
        - criticality: error
          name: "{{ col }}_greater_than_{{ threshold }}"
          check:
            function: sql_expression
            arguments:
              expression: "{{ col }} > {{ threshold }}"
          filter: "{{ filter_col }} IS NOT NULL"
        """
    checks_file = tmp_path / "checks.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")
    checks = DQEngineCore.load_checks_from_local_file(
        str(checks_file), variables={"col": "a", "threshold": 1, "filter_col": "a"}
    )

    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    # Row with a=1 should have an error since a > 1 is false
    result_rows = checked.collect()
    row_a1 = [r for r in result_rows if r["a"] == 1][0]
    assert row_a1["_errors"] is not None
    assert len(row_a1["_errors"]) == 1
    assert row_a1["_errors"][0]["name"] == "a_greater_than_1"

    # Row with a=2 should have no errors
    row_a2 = [r for r in result_rows if r["a"] == 2][0]
    assert row_a2["_errors"] is None

    # Row with a=None should have no errors (filtered out)
    row_null = [r for r in result_rows if r["a"] is None][0]
    assert row_null["_errors"] is None


def test_validate_checks_with_variables(ws, tmp_path):
    checks_yaml = """
        - criticality: "{{ crit }}"
          check:
            function: is_not_null
            arguments:
              column: "{{ col }}"
        """
    checks_file = tmp_path / "checks.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")
    checks = DQEngineCore.load_checks_from_local_file(str(checks_file), variables={"crit": "error", "col": "b"})

    status = DQEngine.validate_checks(checks)
    assert not status.has_errors


def test_validate_checks_with_variables_invalid_after_substitution(ws, tmp_path):
    checks_yaml = """
        - criticality: "{{ crit }}"
          check:
            function: is_not_null
            arguments:
              column: b
        """
    checks_file = tmp_path / "checks.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")
    checks = DQEngineCore.load_checks_from_local_file(str(checks_file), variables={"crit": "not_a_valid_criticality"})

    status = DQEngine.validate_checks(checks)
    assert status.has_errors


def test_validate_checks_without_variables_fails_on_placeholders(ws):
    checks = [
        {
            "criticality": "{{ crit }}",
            "check": {
                "function": "is_not_null",
                "arguments": {"column": "b"},
            },
        },
    ]

    status = DQEngine.validate_checks(checks)
    assert status.has_errors


def test_extra_params_variables_substitution_and_overrides(ws, spark, tmp_path):
    # Setup data specific to this test
    schema = "id int, name string"
    expected_schema = schema + REPORTING_COLUMNS
    df = spark.createDataFrame([(1, "John"), (None, "Doe")], schema)

    # Define Checks with placeholders in nested structure (user_metadata)
    # and deep inside check arguments
    checks_yaml = """
        - criticality: error
          name: "id_check"
          check:
            function: is_not_null
            arguments:
              column: "{{ target_col }}"
          user_metadata:
            env: "{{ environment }}"
            rule_id: "{{ nested_var }}"
        """
    checks_file = tmp_path / "checks_extra.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")

    # Setup DQEngine with ExtraParams variables (Default values)
    # Default variables: target_col=id, environment=dev, nested_var=old
    extra_params = dataclasses.replace(
        EXTRA_PARAMS,
        variables={
            "target_col": "id",
            "environment": "dev",
            "nested_var": "old",
        },
    )
    dq_engine = DQEngine(ws, spark, extra_params=extra_params)

    # Load Checks with overrides
    # target_col: id (from ExtraParams default)
    # environment: prod (per-call override wins)
    # nested_var: new (per-call override wins)
    config = FileChecksStorageConfig(location=str(checks_file))
    checks = dq_engine.load_checks(config, variables={"environment": "prod", "nested_var": "new"})

    # Verify substitution (Structural check)
    assert checks[0]["check"]["arguments"]["column"] == "id"
    assert checks[0]["user_metadata"]["env"] == "prod"
    assert checks[0]["user_metadata"]["rule_id"] == "new"

    # Apply checks to DataFrame (Functional check)
    checked_df = dq_engine.apply_checks_by_metadata(df, checks)

    expected = spark.createDataFrame(
        [
            [1, "John", None, None],
            [
                None,
                "Doe",
                [
                    {
                        "name": "id_check",
                        "message": "Column 'id' value is null",
                        "columns": ["id"],
                        "filter": None,
                        "function": "is_not_null",
                        "run_time": RUN_TIME,
                        "run_id": RUN_ID,
                        "user_metadata": {"env": "prod", "rule_id": "new"},
                    }
                ],
                None,
            ],
        ],
        expected_schema,
    )

    assert_df_equality(checked_df, expected, ignore_nullable=True)


def test_extra_params_variables_conflict_resolution(ws, spark, tmp_path):
    # Verify that a conflict where a variable is defined in both ExtraParams and per-call
    # results in the per-call variable taking precedence.

    # 1. Setup DQEngine with ExtraParams variables
    extra_params = dataclasses.replace(EXTRA_PARAMS, variables={"my_var": "default"})
    dq_engine = DQEngine(ws, spark, extra_params=extra_params)

    # 2. File with placeholder
    checks_yaml = """
        - name: "check_{{ my_var }}"
          check:
            function: is_not_null
            arguments:
              column: id
        """
    checks_file = tmp_path / "checks_conflict.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")
    config = FileChecksStorageConfig(location=str(checks_file))

    # 3. Load with override
    checks = dq_engine.load_checks(config, variables={"my_var": "override"})

    # 4. Verify that "override" won
    assert checks[0]["name"] == "check_override"


def test_extra_params_variables_fallback_to_defaults(ws, spark, tmp_path):
    # Verify that if a variable is NOT provided in the call, it falls back to ExtraParams.

    # 1. Setup DQEngine with ExtraParams variables
    extra_params = dataclasses.replace(EXTRA_PARAMS, variables={"my_var": "default"})
    dq_engine = DQEngine(ws, spark, extra_params=extra_params)

    # 2. File with placeholder
    checks_yaml = """
        - name: "check_{{ my_var }}"
          check:
            function: is_not_null
            arguments:
              column: id
        """
    checks_file = tmp_path / "checks_fallback.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")
    config = FileChecksStorageConfig(location=str(checks_file))

    # 3. Load WITHOUT specific variables in the call - should use engine defaults
    checks = dq_engine.load_checks(config)

    # 4. Verify that "default" was used
    assert checks[0]["name"] == "check_default"


def test_apply_checks_with_missing_variable(ws, spark, tmp_path):
    dq_engine = DQEngine(workspace_client=ws, spark=spark, extra_params=EXTRA_PARAMS)
    test_df = spark.createDataFrame([[1, 3, 3], [2, None, 4], [None, 4, None]], SCHEMA)

    checks_yaml = """
        - criticality: error
          check:
            function: is_not_null
            arguments:
              column: "{{ missing_col }}"
        """
    checks_file = tmp_path / "checks_missing.yml"
    checks_file.write_text(checks_yaml, encoding="utf-8")

    # Load file, which will warn and leave the placeholder
    checks = DQEngineCore.load_checks_from_local_file(str(checks_file), variables={"different_var": "val"})

    # Assert that the placeholder was left in the metadata (unresolved variable)
    assert checks[0]["check"]["arguments"]["column"] == "{{ missing_col }}"

    # Check function apply should not raise an exception, but instead skip the check and report it in the results
    checked = dq_engine.apply_checks_by_metadata(test_df, checks)

    errors = checked.select("_errors").collect()
    for row in errors:
        assert row["_errors"] is not None
        assert len(row["_errors"]) == 1
        assert "Check evaluation skipped due to invalid check columns" in row["_errors"][0]["message"]
        assert "{{ missing_col }}" in row["_errors"][0]["message"]
