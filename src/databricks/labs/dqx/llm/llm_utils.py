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
from databricks.labs.dqx.errors import DQXError
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY
from databricks.labs.dqx.config import InputConfig
from databricks.labs.dqx.io import read_input_data
from databricks.labs.dqx.llm.table_manager import TableManager

logger = logging.getLogger(__name__)

# Re-export TableManager for backward compatibility
__all__ = [
    "TableManager",
    "get_check_function_definitions",
    "get_required_check_functions_definitions",
    "get_required_summary_stats",
    "create_optimizer_training_set",
    "create_optimizer_training_set_with_stats",
    "get_column_metadata",
]


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
    custom_check_functions: dict[str, Callable] | None = None,
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


def get_required_summary_stats(summary_stats: dict[str, Any]) -> dict[str, Any]:
    """
    Filters the summary statistics to include only mean, min, and max values,
    which provide sufficient information for LLM-based rule generation while
    reducing token usage. Converts all values to JSON-serializable format.

    Args:
        summary_stats: Dictionary containing summary statistics for each column.

    Returns:
        dict: A dictionary containing the required fields for each summary stats with JSON-serializable values.
    """

    required_summary_stats: dict = {}
    for key, value in summary_stats.items():
        required_summary_stats[key] = {
            "mean": _convert_to_json_serializable(value.get("mean", None)),
            "min": _convert_to_json_serializable(value.get("min", None)),
            "max": _convert_to_json_serializable(value.get("max", None)),
        }
    return required_summary_stats


def _convert_to_json_serializable(value: Any) -> str | None:
    """
    Convert a value to JSON-serializable format.
    Converts all values to strings for JSON serialization.

    Args:
        value: The value to convert.

    Returns:
        String representation of the value, or None if value is None.
    """
    if value is None:
        return None
    return str(value)


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


def create_optimizer_training_set_with_stats(
    custom_check_functions: dict[str, Callable] | None = None,
) -> list[dspy.Example]:
    """
    Get quality check training examples using data summary statistics for the dspy optimizer.

    Args:
        custom_check_functions: A dictionary of custom check functions.

    Returns:
        list[dspy.Example]: A list of dspy.Example objects created from training examples with stats.
    """
    training_examples = _load_training_examples_with_stats()

    examples = []
    for example_data in training_examples:
        # Convert data_summary_stats to JSON string format expected by dspy.Example
        data_summary_stats_json = json.dumps(example_data["data_summary_stats"])

        example = dspy.Example(
            business_description=example_data.get("business_description", ""),
            data_summary_stats=data_summary_stats_json,
            available_functions=json.dumps(get_required_check_functions_definitions(custom_check_functions)),
            quality_rules=example_data["quality_rules"],
            reasoning=example_data["reasoning"],
        ).with_inputs("business_description", "data_summary_stats", "available_functions")

        examples.append(example)

    return examples


def get_column_metadata(spark: SparkSession, input_config: InputConfig) -> str:
    """
    Get the column metadata for a given table.

    Args:
        input_config (InputConfig): Input configuration for the table.
        spark (SparkSession): The Spark session used to access the table.

    Returns:
        str: A JSON string containing the column metadata with columns wrapped in a "columns" key.
    """
    df = read_input_data(spark, input_config)
    columns = [{"name": field.name, "type": field.dataType.simpleString()} for field in df.schema.fields]
    schema_info = {"columns": columns}
    return json.dumps(schema_info)


def _load_training_examples() -> list[dict[str, Any]]:
    """A function to load the training examples from the llm/resources/training_examples.yml file.

    Returns:
        list[dict[str, Any]]: Training examples as a list of dictionaries.
    """
    resource = Path(str(files("databricks.labs.dqx.llm.resources") / "training_examples.yml"))

    training_examples_as_text = resource.read_text(encoding="utf-8")
    training_examples = yaml.safe_load(training_examples_as_text)

    if not isinstance(training_examples, list):
        raise DQXError("YAML file must contain a list at the root level.")

    return training_examples


def _load_training_examples_with_stats() -> list[dict[str, Any]]:
    """A function to load the training examples with data stats from the llm/resources/training_examples_with_stats.yml file.

    Returns:
        list[dict[str, Any]]: Training examples with data summary statistics as a list of dictionaries.
    """
    resource = Path(str(files("databricks.labs.dqx.llm.resources") / "training_examples_with_stats.yml"))

    training_examples_as_text = resource.read_text(encoding="utf-8")
    training_examples = yaml.safe_load(training_examples_as_text)

    if not isinstance(training_examples, list):
        raise DQXError("YAML file must contain a list at the root level.")

    return training_examples
