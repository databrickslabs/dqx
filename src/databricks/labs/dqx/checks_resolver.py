import os
import sys
import logging
from collections.abc import Callable
import importlib.util
from contextlib import contextmanager

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.geo import check_funcs as geo_check_funcs
from databricks.labs.dqx.errors import InvalidCheckError

logger = logging.getLogger(__name__)


def resolve_check_function(
    function_name: str, custom_check_functions: dict[str, Callable] | None = None, fail_on_missing: bool = True
) -> Callable | None:
    """
    Resolves a function by name from the predefined functions and custom checks.

    Args:
            function_name: name of the function to resolve.
            custom_check_functions: dictionary with custom check functions (e.g. *globals()* of the calling module).
            fail_on_missing: if True, raise an InvalidCheckError if the function is not found.

    Returns:
            function or None if not found.

    Raises:
        InvalidCheckError: if the function is not found and fail_on_missing is True.
    """
    logger.debug(f"Resolving function: {function_name}")
    func = getattr(check_funcs, function_name, None)  # resolve using predefined checks first
    if not func:
        # resolve using predefined geo checks, requires Databricks serverless or DBR >= 17.1
        func = getattr(geo_check_funcs, function_name, None)
    if not func and custom_check_functions:
        func = custom_check_functions.get(function_name)  # returns None if not found
    if fail_on_missing and not func:
        raise InvalidCheckError(f"Function '{function_name}' not found.")
    logger.debug(f"Function {function_name} resolved successfully: {func}")
    return func


def resolve_custom_check_functions_from_path(check_functions: dict[str, str] | None = None) -> dict[str, Callable]:
    """
    Resolve custom check functions from a path in the local filesystem, Databricks workspace, or Unity Catalog volume.

    Args:
        check_functions: a mapping where each key is the name of a function (e.g., "my_func")
            and each value is the file path to the Python module that defines it.
            The path can be absolute or relative to the installation folder, and may refer to a local filesystem location,
            a Databricks workspace path (e.g. /Workspace/my_repo/my_module.py),
            or a Unity Catalog volume (e.g. /Volumes/catalog/schema/volume/my_module.py).
    Returns:
        A dictionary mapping function names to the actual function objects.
    """
    resolved_funcs: dict[str, Callable] = {}
    if check_functions:
        logger.info("Resolving custom check functions.")
        for func_name, module_path in check_functions.items():
            resolved_funcs[func_name] = _import_check_function_from_path(module_path, func_name)
    return resolved_funcs


@contextmanager
def _temp_sys_path(path: str):
    """
    Context manager to temporarily add a path to sys.path.
    This is useful for importing modules from specific paths without permanently modifying sys.path.
    """
    added = False
    if path not in sys.path:
        sys.path.insert(0, path)
        added = True
    try:
        yield
    finally:
        if added:
            sys.path.remove(path)


def _import_check_function_from_path(module_path: str, func_name: str) -> Callable:
    """
    Import a function by name from a module specified by its file path.

    Supports importing from:
    - Local filesystem Python files (e.g., paths like /path/to/my_module.py)
    - Databricks workspace files (e.g., paths under /Workspace/my_repo/my_module.py). Must be prefixed with "/Workspace"
    - Unity Catalog volumes (e.g., paths under /Volumes/catalog/schema/volume/my_module.py)

    Args:
        module_path: The full path to the module containing the function.
        func_name: The name of the function to import.

    Returns:
        The imported function.

    Raises:
        ImportError: If the module file does not exist or cannot be loaded.
        InvalidCheckError: If the function is not found in the module.
    """
    logger.info(f"Resolving custom check function '{func_name}' from module '{module_path}'.")

    if not os.path.exists(module_path):
        raise ImportError(f"Module file '{module_path}' does not exist.")

    module_dir = os.path.dirname(module_path)
    module_name = os.path.splitext(os.path.basename(module_path))[0]

    with _temp_sys_path(module_dir):
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load module from {module_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)  # type: ignore

    try:
        return getattr(module, func_name)
    except AttributeError as exc:
        raise InvalidCheckError(f"Function '{func_name}' not found in '{module_path}'.") from exc
