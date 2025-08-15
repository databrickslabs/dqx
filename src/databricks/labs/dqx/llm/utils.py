import logging
import inspect
from importlib.resources import files
from pathlib import Path
from typing import Any

import yaml

from databricks.labs.dqx.checks_resolver import resolve_check_function
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY
import dspy
import json
from pyspark.sql import SparkSession

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


def get_column_metadata(table_name: str, spark: SparkSession) -> str:
    """A utility function to get the column metadata for a given table."""
    df = spark.table(table_name)
    schema_info = [
        {"name": field.name, "type": field.dataType.simpleString()}
        for field in df.schema.fields
        if not field.name.startswith("_")
    ]
    return json.dumps(schema_info)


def load_training_examples() -> str:
    """Load training_examples.yml file from the llm/resources folder.

    :return: training examples as yaml string.
    """
    resource = Path(str(files("databricks.labs.dqx.llm.resources") / "training_examples.yml"))

    training_examples_as_text = resource.read_text(encoding="utf-8")
    training_examples = yaml.safe_load(training_examples_as_text)
    if not isinstance(training_examples, list):
        raise ValueError("YAML file must contain a list at the root level.")

    return training_examples


def create_optimizer_training_set() -> list[dspy.Example]:
    """Function to get examples of dspy optimization.

    :return: list of dspy.Example objects.
    """
    # Load training examples from YAML file
    training_examples = load_training_examples()

    examples = []
    available_functions = json.dumps(get_check_function_definition())

    for example_data in training_examples:
        # Convert schema_info to JSON string format expected by dspy.Example
        schema_info_json = json.dumps(example_data["schema_info"])

        # Create dspy.Example with the data from YAML
        example = dspy.Example(
            schema_info=schema_info_json,
            business_description=example_data["business_description"],
            available_functions=available_functions,
            quality_rules=example_data["quality_rules"],
            reasoning=example_data["reasoning"],
        ).with_inputs("schema_info", "business_description", "available_functions")

        examples.append(example)

    return examples
