import tempfile
import os
import inspect
from pathlib import Path
import pyspark.sql.functions as F
import pytest
from databricks.labs.dqx.check_funcs import make_condition, register_rule
from databricks.labs.dqx.llm.utils import get_check_function_definition, load_yaml_checks_examples


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


def test_load_yaml_checks_examples_from_custom_path():
    """It should load a valid YAML file from a given path."""
    yaml_checks = """
            - criticality: error
              check:
                function: is_not_null
                arguments:
                  column: col1
            """
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".yml", delete=False) as temp_file:
        temp_file.write(yaml_checks)
        temp_path = temp_file.name

    try:
        result = load_yaml_checks_examples(temp_path)
        assert result == yaml_checks
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def test_load_invalid_yaml_checks_examples_from_custom_path(tmp_path):
    file_path = tmp_path / "bad.yml"
    file_path.write_text("invalid_yaml_here", encoding="utf-8")

    with pytest.raises(ValueError, match="YAML file must contain a list at the root level"):
        try:
            load_yaml_checks_examples(file_path.as_posix())
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)


def test_load_missing_yaml_checks_examples_from_custom_path():
    """It should raise FileNotFoundError if file does not exist."""
    missing_path = Path(tempfile.gettempdir()) / "does_not_exist.yml"
    with pytest.raises(FileNotFoundError):
        load_yaml_checks_examples(missing_path.as_posix())
