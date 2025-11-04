import inspect
import json
import dspy  # type: ignore
import pyspark.sql.functions as F
from databricks.labs.dqx.check_funcs import make_condition, register_rule
from databricks.labs.dqx.llm.llm_utils import (
    get_check_function_definitions,
    create_optimizer_training_set,
    get_required_check_functions_definitions,
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
            lambda x: x['name'] == 'dummy_custom_check_function_test',
            get_check_function_definitions(custom_check_functions),
        )
    )
    sig = inspect.signature(dummy_custom_check_function_test)
    assert result[0] == {
        'name': 'dummy_custom_check_function_test',
        'type': 'row',
        'doc': 'Test the custom check function.',
        'signature': str(sig),
        'parameters': str(sig.parameters),
        'implementation': '@register_rule("row")\ndef dummy_custom_check_function_test(column: str, suffix: str):\n    """\n    Test the custom check function.\n    """\n    return make_condition(\n        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"\n    )\n',
    }


def test_get_check_function_definitions_with_missing_custom_check_functions():
    result = list(
        filter(
            lambda x: x['name'] == 'dummy_custom_check_function_test',
            get_check_function_definitions(),
        )
    )
    assert not result


def test_get_check_function_definitions_with_custom_check_functions_missing_specific_function():
    # Provide custom_check_functions dict but without the function we're looking for
    custom_check_functions = {"some_other_function": dummy_custom_check_function_test}

    result = list(
        filter(
            lambda x: x['name'] == 'dummy_custom_check_function_test',
            get_check_function_definitions(custom_check_functions),
        )
    )
    assert not result


def test_get_required_check_function_definitions():
    custom_check_functions = {"dummy_custom_check_function_test": dummy_custom_check_function_test}
    print(get_required_check_functions_definitions(custom_check_functions))
    result = list(
        filter(
            lambda x: x['check_function_name'] == 'dummy_custom_check_function_test',
            get_required_check_functions_definitions(custom_check_functions),
        )
    )
    sig = inspect.signature(dummy_custom_check_function_test)
    assert result[0] == {
        'check_function_name': 'dummy_custom_check_function_test',
        'parameters': str(sig.parameters),
    }


def test_get_required_check_functions_definitions_with_missing_custom_check_functions():
    result = list(
        filter(
            lambda x: x['check_function_name'] == 'dummy_custom_check_function_test',
            get_required_check_functions_definitions(),
        )
    )
    assert not result


def test_get_required_check_function_definition_with_custom_check_functions_missing_specific_function():
    # Provide custom_check_functions dict but without the function we're looking for
    custom_check_functions = {"some_other_function": dummy_custom_check_function_test}

    result = list(
        filter(
            lambda x: x['check_function_name'] == 'dummy_custom_check_function_test',
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
        assert hasattr(example, 'schema_info')
        assert hasattr(example, 'business_description')
        assert hasattr(example, 'available_functions')
        assert hasattr(example, 'quality_rules')
        assert hasattr(example, 'reasoning')

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
