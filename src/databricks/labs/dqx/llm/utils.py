import logging
import inspect
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path

import yaml

from databricks.labs.dqx.checks_resolver import resolve_check_function
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY

logger = logging.getLogger(__name__)


def get_check_function_definition(custom_check_functions: dict[str, Callable] | None = None) -> list[dict[str, str]]:
    """
    A utility function to get the definition of all check functions.
    This function is primarily used to generate a prompt for the LLM to generate check functions.

    If provided, the function will use the custom check functions to resolve the check function.
    If not provided, the function will use only the built-in check functions.

    Args:
        custom_check_functions: A dictionary of custom check functions.

    Returns:
        list[dict]: A list of dictionaries, each containing the definition of a check function.
    """
    function_docs: list[dict[str, str]] = []
    for name, func_type in CHECK_FUNC_REGISTRY.items():
        func = resolve_check_function(name, custom_check_functions, fail_on_missing=False)
        if func is None:
            logger.warning(f"Check function {name} not found in the registry")
            continue
        sig = inspect.signature(func)
        doc = inspect.getdoc(func)
        function_docs.append(
            {
                "name": name,
                "type": func_type,
                "doc": doc or "",
                "signature": str(sig),
                "parameters": str(sig.parameters),
                "implementation": inspect.getsource(func),
            }
        )
    return function_docs


def load_yaml_checks_examples() -> str:
    """
    Load yaml_checks_examples.yml file from the llm/resources folder.

    Returns:
        checks examples as yaml string.
    """
    resource = Path(str(files("databricks.labs.dqx.llm.resources") / "yaml_checks_examples.yml"))

    yaml_checks_as_text = resource.read_text(encoding="utf-8")
    parsed = yaml.safe_load(yaml_checks_as_text)
    if not isinstance(parsed, list):
        raise InvalidParameterError("YAML file must contain a list at the root level.")

    return yaml_checks_as_text
