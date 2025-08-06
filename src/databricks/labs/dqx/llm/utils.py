import logging
import inspect
from importlib.resources import files
from pathlib import Path
from typing import Any

import yaml

from databricks.labs.dqx.checks_resolver import resolve_check_function
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY
from databricks.labs.dqx.llm.core import get_dspy_compiler

logger = logging.getLogger(__name__)


def get_check_function_definition(custom_check_functions: dict[str, Any] | None = None) -> list[dict[str, str]]:
    """A utility function to get the definition of all check functions.
    This function is primarily used to generate a prompt for the LLM to generate check functions.

    :param custom_check_functions: A dictionary of custom check functions.
        If provided, the function will use the custom check functions to resolve the check function.
        If not provided, the function will use only the built-in check functions.

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
    """Load yaml_checks_examples.yml file from the llm/resources folder.

    :return: checks examples as yaml string.
    """
    resource = Path(str(files("databricks.labs.dqx.llm.resources") / "yaml_checks_examples.yml"))

    yaml_checks_as_text = resource.read_text(encoding="utf-8")
    parsed = yaml.safe_load(yaml_checks_as_text)
    if not isinstance(parsed, list):
        raise ValueError("YAML file must contain a list at the root level.")

    return yaml_checks_as_text


def _get_required_function_info() -> list[dict[str, str]]:
    """Private function to extract only required function information (name and doc).

    Returns:
        list[dict[str, str]]: A list of dictionaries containing only name and doc keys.
    """
    required_function_docs: list[dict[str, str]] = []
    for func in get_check_function_definition():
        required_func_info = {"name": func.get("name", ""), "doc": func.get("doc", "")}
        required_function_docs.append(required_func_info)
    return required_function_docs


def get_business_rules_with_llm(
    user_input: str, api_key: str, api_base: str, model: str = "databricks/databricks-meta-llama-3-3-70b-instruct"
) -> str:
    """A utility function to get the dql rules based on natural language request.

    Returns:
        str: A string containing the dqx rule.
    """
    predictor = get_dspy_compiler(api_key=api_key, api_base=api_base, model=model)
    return predictor(user_input=f"""{user_input} + "\n\nAvailable functions:\n" + {_get_required_function_info()}""")
