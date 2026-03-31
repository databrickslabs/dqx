import dataclasses
from databricks.labs.dqx.engine import DQEngine, DQEngineCore
from databricks.labs.dqx.config import FileChecksStorageConfig
from tests.integration.conftest import (
    REPORTING_COLUMNS,
    EXTRA_PARAMS,
)

SCHEMA = "a: int, b: int, c: int"
EXPECTED_SCHEMA = SCHEMA + REPORTING_COLUMNS


def test_load_checks_by_metadata_with_variables(tmp_path):

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

    assert checks == [
        {
            "criticality": "error",
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": "b"},
            },
        }
    ]


def test_load_checks_by_metadata_and_split_with_variables(tmp_path):

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

    assert checks == [
        {
            "criticality": "error",
            "name": "b_null_check",
            "check": {
                "function": "is_not_null_and_not_empty",
                "arguments": {"column": "b"},
            },
        },
        {
            "criticality": "warn",
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "a > 1"},
            },
        },
    ]


def test_load_checks_by_metadata_with_variables_name_and_filter(tmp_path):

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

    assert checks == [
        {
            "criticality": "error",
            "name": "a_greater_than_1",
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": "a > 1"},
            },
            "filter": "a IS NOT NULL",
        }
    ]


def test_validate_checks_with_variables(tmp_path):
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


def test_validate_checks_with_variables_invalid_after_substitution(tmp_path):
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
    expected_error = (
        "Invalid 'criticality' value: 'not_a_valid_criticality'. Expected 'warn' or 'error'. "
        "Check details: {'criticality': 'not_a_valid_criticality', "
        "'check': {'function': 'is_not_null', 'arguments': {'column': 'b'}}}"
    )
    assert status.errors[0] == expected_error


def test_validate_checks_without_variables_fails_on_placeholders():
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
    expected_error = (
        "Invalid 'criticality' value: '{{ crit }}'. Expected 'warn' or 'error'. "
        "Check details: {'criticality': '{{ crit }}', "
        "'check': {'function': 'is_not_null', 'arguments': {'column': 'b'}}}"
    )
    assert status.errors[0] == expected_error


def test_extra_params_variables_substitution_and_overrides(ws, spark, tmp_path):
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


def test_load_checks_with_missing_variable(tmp_path):

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
