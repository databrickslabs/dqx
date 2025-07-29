import inspect
import pyspark.sql.functions as F

from databricks.labs.dqx.check_funcs import make_condition, register_rule
from databricks.labs.dqx.llm.utils import get_check_function_definition


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
