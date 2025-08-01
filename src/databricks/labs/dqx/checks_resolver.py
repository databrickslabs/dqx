import logging
from collections.abc import Callable
from typing import Any

from databricks.labs.dqx import check_funcs


logger = logging.getLogger(__name__)


def resolve_check_function(
    function_name: str, custom_check_functions: dict[str, Any] | None = None, fail_on_missing: bool = True
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
