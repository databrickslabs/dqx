from databricks.labs.dqx.llm.core import DQRuleGen
from databricks.labs.dqx.llm.utils import get_check_function_definition
import json
import dspy


# def _get_required_function_info() -> list[dict[str, str]]:
#     """Private function to extract only required function information (name and doc).

#     Returns:
#         list[dict[str, str]]: A list of dictionaries containing only name and doc keys.
#     """
#     required_function_docs: list[dict[str, str]] = []
#     for func in get_check_function_definition():
#         required_func_info = {"name": func.get("name", ""), "doc": func.get("doc", "")}
#         required_function_docs.append(required_func_info)
#     return required_function_docs


def get_business_rules_with_llm(
    user_input: str, schema_info: str, dspy_compiler: DQRuleGen
) -> dspy.primitives.prediction.Prediction:
    """A function to get the dql rules based on natural language request and schema information.

    Returns:
        Prediction: A Prediction object containing the dqx rules and reasoning.
    """
    return dspy_compiler(
        schema_info=schema_info,
        user_input=f"""{user_input}""",
        available_functions=json.dumps(get_check_function_definition()),
    )
