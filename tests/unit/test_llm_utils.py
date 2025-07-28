import pyspark.sql.functions as F

from databricks.labs.dqx.check_funcs import make_condition, register_rule
from databricks.labs.dqx.llm.utils import get_check_function_defintion


@register_rule("row")
def dummy_custom_check_function_test(column: str, suffix: str):
    """
    Test the custom check function.
    """
    return make_condition(
        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"
    )


def test_get_check_function_defintion():
    custom_check_functions = {"dummy_custom_check_function_test": dummy_custom_check_function_test}
    result = list(
        filter(
            lambda x: x['name'] == 'dummy_custom_check_function_test',
            get_check_function_definition(custom_check_functions),
        )
    )
    assert result[0] == {
        'name': 'dummy_custom_check_function_test',
        'type': 'row',
        'doc': 'Test the custom check function.',
        'signature': '(column: str, suffix: str)',
        'parameters': 'OrderedDict([(\'column\', <Parameter "column: str">), (\'suffix\', <Parameter "suffix: str">)])',
        'implementation': '@register_rule("row")\ndef dummy_custom_check_function_test(column: str, suffix: str):\n    """\n    Test the custom check function.\n    """\n    return make_condition(\n        F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"\n    )\n',
    }
