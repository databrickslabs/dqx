import os
import sys
import logging
from collections.abc import Callable
import importlib.util
from contextlib import contextmanager

from databricks.labs.dqx import check_funcs


logger = logging.getLogger(__name__)


def resolve_check_function(
    function_name: str, custom_check_functions: dict[str, Callable] | None = None, fail_on_missing: bool = True
) -> Callable | None:
    """
    Resolves a function by name from the predefined functions and custom checks.

    :param function_name: name of the function to resolve.
    :param custom_check_functions: dictionary with custom check functions (eg. ``globals()`` of the calling module).
    :param fail_on_missing: if True, raise an AttributeError if the function is not found.
    :return: function or None if not found.
    """
    logger.debug(f"Resolving function: {function_name}")
    func = getattr(check_funcs, function_name, None)  # resolve using predefined checks first
    if not func and custom_check_functions:
        func = custom_check_functions.get(function_name)  # returns None if not found
    if fail_on_missing and not func:
        raise AttributeError(f"Function '{function_name}' not found.")
    logger.debug(f"Function {function_name} resolved successfully: {func}")
    return func


@contextmanager
def temp_sys_path(path: str):
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


def import_check_function_from_path(func_module_full_path: str, func_name: str) -> Callable:
    """
    Import a function by name from a module specified by its file path.

    Supports importing from:
    - Local filesystem Python files (e.g., paths like /path/to/my_module.py)
    - Databricks workspace files (e.g., paths under /Workspace/my_repo/my_module.py). Must be prefixed with "/Workspace"
    - Unity Catalog volumes (e.g., paths under /Volumes/catalog/schema/volume/my_module.py)

    :param func_module_full_path: The full path to the module containing the function.
    :param func_name: The name of the function to import.
    :return: The imported function.
    """
    logger.info(f"Resolving custom check function '{func_name}' from module '{func_module_full_path}'.")

    if not os.path.exists(func_module_full_path):
        raise ImportError(f"Module file '{func_module_full_path}' does not exist.")

    module_dir = os.path.dirname(func_module_full_path)
    module_name = os.path.splitext(os.path.basename(func_module_full_path))[0]

    with temp_sys_path(module_dir):
        spec = importlib.util.spec_from_file_location(module_name, func_module_full_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load module from {func_module_full_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)  # type: ignore

    try:
        return getattr(module, func_name)
    except AttributeError as exc:
        raise ImportError(f"Function '{func_name}' not found in '{func_module_full_path}'.") from exc
