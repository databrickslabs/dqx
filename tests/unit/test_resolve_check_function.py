import textwrap

import pytest
from databricks.labs.dqx.checks_resolver import resolve_check_function, import_check_function_from_path


def test_resolve_predefined_function():
    result = resolve_check_function('is_not_null')
    assert result


def custom_check_func():
    pass


def test_resolve_custom_check_function():
    result = resolve_check_function('custom_check_func', {"custom_check_func": custom_check_func})
    assert result

    # or for simplicity use globals
    result = resolve_check_function('custom_check_func', globals())
    assert result


def test_resolve_function_fail_on_missing():
    with pytest.raises(AttributeError):
        resolve_check_function('missing_func', fail_on_missing=True)


def test_resolve_function_not_fail_on_missing():
    result = resolve_check_function('missing_func', fail_on_missing=False)
    assert not result


@pytest.fixture
def temp_module_file(tmp_path):
    """Creates a temporary Python module file."""

    def _create(content: str, name: str = "temp_module.py") -> str:
        module_path = tmp_path / name
        module_path.write_text(textwrap.dedent(content))
        return str(module_path)

    return _create


def test_import_function_from_path_success(temp_module_file):
    path = temp_module_file(
        """
        def my_function():
            return "hello world"
    """
    )

    func = import_check_function_from_path(path, "my_function")
    assert callable(func)
    assert func() == "hello world"


def test_import_function_from_path_not_found(temp_module_file):
    path = temp_module_file(
        """
        def other_function():
            pass
    """
    )

    with pytest.raises(ImportError) as exc:
        import_check_function_from_path(path, "missing_function")
    assert "Function 'missing_function' not found" in str(exc.value)


def test_import_module_from_path_not_found():
    func_module_full_path = "/nonexistent/path/module.py"
    with pytest.raises(ImportError) as exc:
        import_check_function_from_path(func_module_full_path, "func")
    assert f"Module file '{func_module_full_path}' does not exist" in str(exc.value)


def test_import_module_spec_none_without_mock(tmp_path):
    # Create a file with a non-Python extension
    fake_module_path = tmp_path / "not_a_module.txt"
    fake_module_path.write_text("this is not a python module")

    with pytest.raises(ImportError) as exc:
        import_check_function_from_path(str(fake_module_path), "some_func")
    assert f"Cannot find module at {fake_module_path}" in str(exc.value)


def test_import_function_from_path_with_dependency(tmp_path):
    # Create helper.py in tmp_path
    helper_path = tmp_path / "helper.py"
    helper_path.write_text("def helper_func(): return 'dependency ok'")

    # Create main module in same tmp_path
    main_path = tmp_path / "main_module.py"
    main_path.write_text("from helper import helper_func\ndef main_func():\n    return helper_func()\n")

    func = import_check_function_from_path(str(main_path), "main_func")
    assert func() == "dependency ok"
