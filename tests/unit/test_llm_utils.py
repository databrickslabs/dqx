import inspect
import json
from unittest.mock import Mock
import pytest
import dspy  # type: ignore
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StringType, IntegerType

from databricks.labs.dqx.check_funcs import make_condition, register_rule
from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY
from databricks.labs.dqx.llm.llm_utils import (
    get_check_function_definitions,
    create_optimizer_training_set,
    get_required_check_functions_definitions,
    get_column_metadata,
    create_optimizer_training_set_with_stats,
    extract_json_rules,
)


@register_rule("row")
def dummy_custom_check_function_test(column: str, suffix: str):
    """
    Test the custom check function.
    """
    return make_condition(
        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"
    )


def test_get_check_function_definitions():
    custom_check_functions = {"dummy_custom_check_function_test": dummy_custom_check_function_test}
    result = list(
        filter(
            lambda x: x["name"] == "dummy_custom_check_function_test",
            get_check_function_definitions(custom_check_functions),
        )
    )
    sig = inspect.signature(dummy_custom_check_function_test)
    assert result[0] == {
        "name": "dummy_custom_check_function_test",
        "type": "row",
        "doc": "Test the custom check function.",
        "signature": str(sig),
        "parameters": str(sig.parameters),
        "implementation": '@register_rule("row")\ndef dummy_custom_check_function_test(column: str, suffix: str):\n    """\n    Test the custom check function.\n    """\n    return make_condition(\n        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"\n    )\n',
    }


def test_get_check_function_definitions_with_missing_custom_check_functions():
    result = list(
        filter(
            lambda x: x["name"] == "dummy_custom_check_function_test",
            get_check_function_definitions(),
        )
    )
    assert not result


def test_get_check_function_definitions_with_custom_check_functions_missing_specific_function():
    # Provide custom_check_functions dict but without the function we're looking for
    custom_check_functions = {"some_other_function": dummy_custom_check_function_test}

    result = list(
        filter(
            lambda x: x["name"] == "dummy_custom_check_function_test",
            get_check_function_definitions(custom_check_functions),
        )
    )
    assert not result


def test_get_required_check_function_definitions():
    custom_check_functions = {"dummy_custom_check_function_test": dummy_custom_check_function_test}

    result = list(
        filter(
            lambda x: x["check_function_name"] == "dummy_custom_check_function_test",
            get_required_check_functions_definitions(custom_check_functions),
        )
    )
    sig = inspect.signature(dummy_custom_check_function_test)
    assert result[0] == {
        "check_function_name": "dummy_custom_check_function_test",
        "parameters": str(sig.parameters),
    }


def test_get_required_check_functions_definitions_with_missing_custom_check_functions():
    result = list(
        filter(
            lambda x: x["check_function_name"] == "dummy_custom_check_function_test",
            get_required_check_functions_definitions(),
        )
    )
    assert not result


def test_get_required_check_function_definition_with_custom_check_functions_missing_specific_function():
    # Provide custom_check_functions dict but without the function we're looking for
    custom_check_functions = {"some_other_function": dummy_custom_check_function_test}

    result = list(
        filter(
            lambda x: x["check_function_name"] == "dummy_custom_check_function_test",
            get_required_check_functions_definitions(custom_check_functions),
        )
    )
    assert not result


def test_get_training_examples():
    """Test that get_training_examples returns properly formatted dspy.Example objects."""

    examples = create_optimizer_training_set()

    # Verify it returns a list
    assert isinstance(examples, list)

    # Verify it has at least one example
    assert len(examples) >= 1

    # Verify all items are dspy.Example objects
    for example in examples:
        assert isinstance(example, dspy.Example)

        # Verify required attributes exist
        assert hasattr(example, "schema_info")
        assert hasattr(example, "business_description")
        assert hasattr(example, "available_functions")
        assert hasattr(example, "quality_rules")
        assert hasattr(example, "reasoning")

        schema_info = json.loads(example.schema_info)
        assert isinstance(schema_info, dict)
        assert "columns" in schema_info

        # Verify available_functions is valid JSON
        available_functions = json.loads(example.available_functions)
        assert isinstance(available_functions, list)

        # Verify quality_rules is a string containing YAML
        assert isinstance(example.quality_rules, str)


def test_get_training_examples_with_custom_check_functions():
    custom_check_functions = {"dummy_custom_check_function_test": dummy_custom_check_function_test}
    examples = create_optimizer_training_set(custom_check_functions)

    filtered_examples = [
        example
        for example in examples
        if any(
            func.get("check_function_name") == "dummy_custom_check_function_test"
            for func in json.loads(example.available_functions)
        )
    ]
    assert filtered_examples


@pytest.mark.parametrize(
    "training_set_factory",
    [create_optimizer_training_set, create_optimizer_training_set_with_stats],
)
def test_training_examples_reference_only_registered_functions(training_set_factory):
    """Every function name used in the few-shot training examples must resolve in the registry.

    Guards against typos like ``is_email`` (should be ``is_valid_email``): an unregistered
    name silently teaches the LLM to emit rules that get dropped by validation at generation time.
    """
    referenced_functions = set()
    for example in training_set_factory():
        for rule in json.loads(example.quality_rules):
            referenced_functions.add(rule["check"]["function"])

    unknown_functions = referenced_functions - set(CHECK_FUNC_REGISTRY)
    assert not unknown_functions, f"Training examples reference unregistered check functions: {unknown_functions}"


@pytest.mark.parametrize(
    "location, spark_read_mock_method",
    [
        ("catalog.schema.table", "table"),
        ("s3://bucket/path/to/data", "load"),
    ],
)
def test_get_column_metadata(location, spark_read_mock_method, mock_spark):
    mock_df = Mock()
    mock_schema = Mock()
    mock_schema.fields = [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
    mock_df.schema = mock_schema

    # Dynamically set the mock method (either table or load)
    setattr(mock_spark.read.options.return_value, spark_read_mock_method, Mock(return_value=mock_df))

    result = get_column_metadata(mock_spark, InputConfig(location=location))
    expected_result = {
        "columns": [
            {"name": "customer_id", "type": "string"},
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "age", "type": "int"},
        ]
    }
    assert result == json.dumps(expected_result)


def test_create_optimizer_training_set_with_stats():
    """Test that create_optimizer_training_set_with_stats returns properly formatted dspy.Example objects."""

    examples = create_optimizer_training_set_with_stats()

    # Verify it returns a list
    assert isinstance(examples, list)

    # Verify it has at least one example
    assert len(examples) >= 1

    # Verify all items are dspy.Example objects
    for example in examples:
        assert isinstance(example, dspy.Example)

        # Verify required attributes exist
        assert hasattr(example, "business_description")
        assert hasattr(example, "data_summary_stats")
        assert hasattr(example, "available_functions")
        assert hasattr(example, "quality_rules")
        assert hasattr(example, "reasoning")

        # Verify data_summary_stats is valid JSON
        data_summary_stats = json.loads(example.data_summary_stats)
        assert isinstance(data_summary_stats, dict)

        # Verify available_functions is valid JSON
        available_functions = json.loads(example.available_functions)
        assert isinstance(available_functions, list)

        # Verify quality_rules is a string
        assert isinstance(example.quality_rules, str)


_RULES = '[{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "c"}}}]'


def test_extract_json_rules_plain_array():
    parsed = extract_json_rules(_RULES)
    assert parsed == [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "c"}}}]


def test_extract_json_rules_ignores_trailing_explanation():
    # The 'Extra data' failure: a valid array followed by prose the model appended.
    raw = _RULES + "\n\nThese rules validate that column c is not null."
    assert extract_json_rules(raw) == json.loads(_RULES)


def test_extract_json_rules_strips_json_code_fence():
    raw = f"```json\n{_RULES}\n```"
    assert extract_json_rules(raw) == json.loads(_RULES)


def test_extract_json_rules_strips_bare_code_fence():
    raw = f"```\n{_RULES}\n```"
    assert extract_json_rules(raw) == json.loads(_RULES)


def test_extract_json_rules_skips_prose_preamble():
    raw = f"Here are the generated rules:\n{_RULES}"
    assert extract_json_rules(raw) == json.loads(_RULES)


def test_extract_json_rules_raises_when_no_json():
    with pytest.raises(json.JSONDecodeError):
        extract_json_rules("no json here at all")


def test_extract_json_rules_raises_on_truncated_object():
    # A non-array top-level value (e.g. truncated output missing the array brackets) is not salvaged
    # into a fragment — it must raise so callers score it as invalid rather than a partial rule.
    with pytest.raises(json.JSONDecodeError):
        extract_json_rules('{"check": {"function": "is_not_null", "arguments": {"column": "c"}}')


def test_extract_json_rules_ignores_bracket_in_preamble():
    # A stray '[' inside prose must not be mistaken for the start of the rules array.
    raw = f"Rules for [orders]:\n{_RULES}"
    assert extract_json_rules(raw) == json.loads(_RULES)


def test_extract_json_rules_ignores_brace_in_preamble():
    raw = f"Here is the JSON {{see below}}:\n{_RULES}"
    assert extract_json_rules(raw) == json.loads(_RULES)


def test_extract_json_rules_prefers_array_over_leading_object():
    # A reasoning object emitted before the rules array must not shadow the array.
    raw = '{"reasoning": "why"} ' + _RULES
    assert extract_json_rules(raw) == json.loads(_RULES)


def test_extract_json_rules_keeps_triple_backticks_inside_string_value():
    raw = '```json\n[{"check": {"arguments": {"query": "a ``` b"}}}]\n```'
    parsed = extract_json_rules(raw)
    assert parsed[0]["check"]["arguments"]["query"] == "a ``` b"
