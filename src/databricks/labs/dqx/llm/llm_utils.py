import logging
import inspect
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path
from typing import Any
import json
import yaml
import dspy  # type: ignore
from pyspark.sql import SparkSession
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


def get_column_metadata(table_name: str, spark: SparkSession) -> str:
    """
    Get the column metadata for a given table.

    Args:
        table_name (str): The name of the table to retrieve metadata for.
        spark (SparkSession): The Spark session used to access the table.

    Returns:
        str: A JSON string containing the column metadata with columns wrapped in a "columns" key.
    """
    df = spark.table(table_name)
    columns = [{"name": field.name, "type": field.dataType.simpleString()} for field in df.schema.fields]
    schema_info = {"columns": columns}
    return json.dumps(schema_info)


def load_training_examples() -> list[dict[str, Any]]:
    """A function to Load the training_examples.yml file from the llm/resources folder.

    Returns:
        list[dict[str, Any]]: Training examples as a list of dictionaries.
    """
    resource = Path(str(files("databricks.labs.dqx.llm.resources") / "training_examples.yml"))

    training_examples_as_text = resource.read_text(encoding="utf-8")
    training_examples = yaml.safe_load(training_examples_as_text)
    if not isinstance(training_examples, list):
        raise ValueError("YAML file must contain a list at the root level.")

    return training_examples


def _get_required_check_function_info() -> list[dict[str, str]]:
    """
    Extract only required function information (name and doc).

    Returns:
        list[dict[str, str]]: A list of dictionaries containing the name, doc, type, signature, and parameters of each function.
    """
    required_function_docs: list[dict[str, str]] = []
    for func in get_check_function_definition():
        required_func_info = {
            "check_function_name": func.get("name", ""),
            "parameters": func.get("parameters", ""),
        }
        required_function_docs.append(required_func_info)
    return required_function_docs


def create_optimizer_training_set() -> list[dspy.Example]:
    """
    Get examples for the dspy optimizer.

    Returns:
        list[dspy.Example]: A list of dspy.Example objects created from training examples.
    """
    training_examples = load_training_examples()

    examples = []
    available_functions = json.dumps(_get_required_check_function_info())

    for example_data in training_examples:
        # Convert schema_info to JSON string format expected by dspy.Example
        schema_info_json = json.dumps(example_data["schema_info"])

        example = dspy.Example(
            schema_info=schema_info_json,
            business_description=example_data["business_description"],
            available_functions=available_functions,
            quality_rules=example_data["quality_rules"],
            reasoning=example_data["reasoning"],
        ).with_inputs("schema_info", "business_description", "available_functions")

        examples.append(example)

    return examples
