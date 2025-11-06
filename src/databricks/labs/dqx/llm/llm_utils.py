import logging
import inspect
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path
from typing import Any
import json
import yaml
import dspy  # type: ignore
from databricks.labs.dqx.checks_resolver import resolve_check_function
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY

logger = logging.getLogger(__name__)


def get_check_function_definitions(custom_check_functions: dict[str, Callable] | None = None) -> list[dict[str, str]]:
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


def get_required_check_functions_definitions(
    custom_check_functions: dict[str, Callable] | None = None
) -> list[dict[str, str]]:
    """
    Extract only required function information (name and doc).

    Returns:
        list[dict[str, str]]: A list of dictionaries containing the required fields for each check function.
    """
    required_function_docs: list[dict[str, str]] = []
    for func in get_check_function_definitions(custom_check_functions):
        # Tests showed that using function name and parameters alone yields better results
        # compared to full specification while reducing token count.
        # LLMs often dilute attention given too much specification.
        required_func_info = {
            "check_function_name": func.get("name", ""),
            "parameters": func.get("parameters", ""),
        }
        required_function_docs.append(required_func_info)
    return required_function_docs


def create_optimizer_training_set(custom_check_functions: dict[str, Callable] | None = None) -> list[dspy.Example]:
    """
    Get quality check training examples for the dspy optimizer.

    Args:
        custom_check_functions: A dictionary of custom check functions.

    Returns:
        list[dspy.Example]: A list of dspy.Example objects created from training examples.
    """
    training_examples = _load_training_examples()

    examples = []
    for example_data in training_examples:
        # Convert schema_info to JSON string format expected by dspy.Example
        schema_info_json = json.dumps(example_data["schema_info"])

        example = dspy.Example(
            schema_info=schema_info_json,
            business_description=example_data["business_description"],
            available_functions=json.dumps(get_required_check_functions_definitions(custom_check_functions)),
            quality_rules=example_data["quality_rules"],
            reasoning=example_data["reasoning"],
        ).with_inputs("schema_info", "business_description", "available_functions")

        examples.append(example)

    return examples


def _load_training_examples() -> list[dict[str, Any]]:
    """A function to load the training examples from the llm/resources/training_examples.yml file.

    Returns:
        list[dict[str, Any]]: Training examples as a list of dictionaries.
    """
    resource = Path(str(files("databricks.labs.dqx.llm.resources") / "training_examples.yml"))

    training_examples_as_text = resource.read_text(encoding="utf-8")
    training_examples = yaml.safe_load(training_examples_as_text)

    if not isinstance(training_examples, list):
        raise ValueError("YAML file must contain a list at the root level.")

    return training_examples
