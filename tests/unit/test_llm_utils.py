from unittest.mock import patch

import pyspark.sql.functions as F

from databricks.labs.dqx.check_funcs import make_condition
from databricks.labs.dqx.llm.utils import get_check_function_defintion


def dummy_check_function(column: str, suffix: str):
    """
    This is a dummy check function. It is used to test the get_check_function_defintion function.
    """
    return make_condition(
        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"
    )


def dummy_custom_check_function(column: str, suffix: str):
    """
    Test the custom check function.
    """
    return make_condition(
        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"
    )


@patch('databricks.labs.dqx.engine.DQEngineCore.resolve_check_function')
@patch.dict(
    'databricks.labs.dqx.rule.CHECK_FUNC_REGISTRY',
    [("dummy_check_function", "row"), ("dummy_custom_check_function", "row")],
    clear=True,
)
def test_get_check_function_defintion(mock_resolve_check_function):
    mock_resolve_check_function.side_effect = [dummy_check_function, dummy_custom_check_function]
    custom_check_functions = {"dummy_custom_check_function": "row"}
    result = get_check_function_defintion(custom_check_functions)
    assert result[0] == {
        'name': 'dummy_check_function',
        'type': 'row',
        'doc': 'This is a dummy check function. It is used to test the get_check_function_defintion function.',
        'signature': '(column: str, suffix: str)',
        'parameters': 'OrderedDict([(\'column\', <Parameter "column: str">), (\'suffix\', <Parameter "suffix: str">)])',
        'implementation': 'def dummy_check_function(column: str, suffix: str):\n    """\n    This is a dummy check function. It is used to test the get_check_function_defintion function.\n    """\n    return make_condition(\n        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"\n    )\n',
    }
    assert result[1] == {
        'name': 'dummy_custom_check_function',
        'type': 'row',
        'doc': 'Test the custom check function.',
        'signature': '(column: str, suffix: str)',
        'parameters': 'OrderedDict([(\'column\', <Parameter "column: str">), (\'suffix\', <Parameter "suffix: str">)])',
        'implementation': 'def dummy_custom_check_function(column: str, suffix: str):\n    """\n    Test the custom check function.\n    """\n    return make_condition(\n        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"\n    )\n',
    }
