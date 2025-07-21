import inspect
from databricks.labs.dqx.engine import DQEngineCore
from databricks.labs.dqx.rule import CHECK_FUNC_REGISTRY
from databricks.labs.dqx.check_funcs import *


def get_check_function_defintion():
    """A utility function to get the definition of all check functions.
    This is function is primarily used to generate a prompt for the LLM to generate a check functions.

    Returns:
      list[dict]: A list of dictionaries, each containing the definition of a check function.
    """
    function_docs = []
    for name, func_type in CHECK_FUNC_REGISTRY.items():
        func = DQEngineCore.resolve_check_function(name, fail_on_missing=True)
        function_docs.append(
            {
                "name": name,
                "function_check_type": func_type,
                "doc": inspect.getdoc(func),
                "signature": inspect.signature(func),
                "parameters": inspect.signature(func).parameters,
            }
        )
    return function_docs
