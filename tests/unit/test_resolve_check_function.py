import textwrap

import pytest
from databricks.labs import dqx
from databricks.labs.dqx.checks_resolver import resolve_check_function, resolve_custom_check_functions_from_path
from databricks.labs.dqx.errors import InvalidCheckError


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
    with pytest.raises(InvalidCheckError):
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


def test_resolve_custom_check_functions_from_path_success(temp_module_file):
    module_path = temp_module_file(
        """
        def my_function():
            return "hello world"
    """
    )

    funcs = resolve_custom_check_functions_from_path({"my_function": module_path})
    assert "my_function" in funcs
    func = funcs["my_function"]
    assert callable(func)
    assert func() == "hello world"


def test_resolve_custom_check_functions_from_path_missing_func(temp_module_file):
    module_path = temp_module_file(
        """
        def other_function():
            pass
    """
    )

    with pytest.raises(InvalidCheckError) as exc:
        resolve_custom_check_functions_from_path({"missing_function": module_path})
    assert "Function 'missing_function' not found" in str(exc.value)


def test_resolve_custom_check_functions_from_path_not_found():
    module_path = "/nonexistent/path/module.py"
    with pytest.raises(ImportError) as exc:
        resolve_custom_check_functions_from_path({"func": module_path})
    assert f"Module file '{module_path}' does not exist" in str(exc.value)


def test_resolve_custom_check_functions_from_path_non_python_module(tmp_path):
    # Create a file with a non-Python extension
    fake_module_path = tmp_path / "not_a_module.txt"
    fake_module_path.write_text("this is not a python module")

    with pytest.raises(ImportError) as exc:
        resolve_custom_check_functions_from_path({"some_func": str(fake_module_path)})
    assert f"Cannot load module from {fake_module_path}" in str(exc.value)


def test_resolve_custom_check_functions_from_path_with_dependency(tmp_path):
    # Create helper.py in tmp_path
    helper_path = tmp_path / "helper.py"
    helper_path.write_text("def helper_func(): return 'dependency ok'")

    # Create main module in same tmp_path
    main_path = tmp_path / "main_module.py"
    main_path.write_text("from helper import helper_func\ndef main_func():\n    return helper_func()\n")

    func = resolve_custom_check_functions_from_path({"main_func": str(main_path)})["main_func"]
    assert func() == "dependency ok"


def test_optional_module_import_failure():
    """Test that optional check modules can be unavailable without breaking core resolution."""
    resolver = dqx.checks_resolver
    optional_modules = getattr(resolver, "_OPTIONAL_CHECK_MODULES")
    original_cache = dict(getattr(resolver, "_optional_modules_cache"))

    try:
        for module_path in optional_modules:
            getattr(resolver, "_optional_modules_cache")[module_path] = None

        func = resolver.resolve_check_function("is_not_null")
        assert func is not None

        func = resolver.resolve_check_function("some_missing_func", fail_on_missing=False)
        assert func is None
    finally:
        setattr(resolver, "_optional_modules_cache", original_cache)
