import inspect
from unittest.mock import Mock
import json
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StringType, IntegerType
from databricks.labs.dqx.check_funcs import make_condition, register_rule
from databricks.labs.dqx.llm.utils import (
    get_check_function_definition,
    load_yaml_checks_examples,
    get_column_metadata,
    create_optimizer_training_set,
)


@register_rule("row")
def dummy_custom_check_function_test(column: str, suffix: str):
    """
    Test the custom check function.
    """
    return make_condition(
        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"
    )


def test_get_check_function_definition():
    custom_check_functions = {"dummy_custom_check_function_test": dummy_custom_check_function_test}
    result = list(
        filter(
            lambda x: x['name'] == 'dummy_custom_check_function_test',
            get_check_function_definition(custom_check_functions),
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


def test_get_check_function_definition_with_missing_custom_check_functions():
    result = list(
        filter(
            lambda x: x['name'] == 'dummy_custom_check_function_test',
            get_check_function_definition(),
        )
    )
    assert not result


def test_get_check_function_definition_with_custom_check_functions_missing_specific_function():
    """Test case when custom_check_functions is provided but doesn't contain the specific function."""
    # Provide custom_check_functions dict but without the function we're looking for
    custom_check_functions = {"some_other_function": dummy_custom_check_function_test}

    result = list(
        filter(
            lambda x: x['name'] == 'dummy_custom_check_function_test',
            get_check_function_definition(custom_check_functions),
        )
    )
    assert not result


def test_load_yaml_checks_examples():
    yaml_examples = load_yaml_checks_examples()
    assert yaml_examples


def test_column_metadata():
    mock_spark = Mock()
    mock_df = Mock()
    mock_df.schema.fields = [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
    mock_spark.table.return_value = mock_df

    result = get_column_metadata("test_table", mock_spark)

    expected_result = [
        {"name": "customer_id", "type": "string"},
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "age", "type": "int"},
    ]
    assert result == json.dumps(expected_result)


def test_get_training_examples():
    """Test that get_training_examples returns properly formatted dspy.Example objects."""
    import dspy  # type: ignore

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
