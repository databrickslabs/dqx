import dspy
from dspy.teleprompt import BootstrapFewShot


class GenerateDBXRuleConfig(dspy.Signature):
    """Generate a yaml configuration based on user input and provided examples."""

    user_input: str = dspy.InputField(
        desc="Natural language description of the desired configuration along with functions metadata"
    )
    config_output: str = dspy.OutputField(desc="Generated configuration in the yaml format")


def _get_few_shot_examples() -> list[dspy.Example]:
    """Private function to get the few shot examples.

    Returns:
        list[dspy.Example]: A list of few shot examples.
    """
    return [
        dspy.Example(
            user_input="I need to raise warning if my column c1 is null",
            config_output="""
- criticality: warn
  check:
    function: is_not_null
    arguments:
      column: c1
""",
        ).with_inputs("user_input"),
        dspy.Example(
            user_input="I need to raise error if my column c2 is in given list",
            config_output="""
- criticality: error
  check:
    function: is_in_list
    arguments:
      column: c2
""",
        ).with_inputs("user_input"),
        dspy.Example(
            user_input="I need to raise error if my column abc is  null ",
            config_output="""
- criticality: error
  check:
    function: is_not_null
    arguments:
      column: abc
""",
        ).with_inputs("user_input"),
    ]


def _configure_dspy_lm(
    model: str = "databricks/databricks-meta-llama-3-3-70b-instruct", api_key: str = None, api_base: str = None
):
    """Configure the Dspy language model.

    :param model: The model to use for the Dspy language model.
    """
    lm = dspy.LM(
        model=model,
        model_type="chat",
        api_key=api_key,
        api_base=api_base,
    )
    dspy.configure(lm=lm)


# Need to implement a metric to evaluate the quality of the generated configuration
def _dspy_metric(example, prediction, trace=None):
    return True


def get_dspy_compiler(
    api_key: str = None, api_base: str = None, model: str = "databricks/databricks-meta-llama-3-3-70b-instruct"
) -> str:
    """A utility function to get the Dspy compiler.

    :param custom_check_functions: A dictionary of custom check functions.
        If provided, the function will use the custom check functions to resolve the check function.
        If not provided, the function will use only the built-in check functions.
    """

    if not api_key or not api_base:
        raise ValueError("api_key and api_base must be provided")

    _configure_dspy_lm(api_key=api_key, api_base=api_base, model=model)
    # Initialize a Predict module with your signature
    predictor = dspy.Predict(signature=GenerateDBXRuleConfig)

    # Create the optimizer and compile the predictor with your examples
    optimizer = BootstrapFewShot(metric=_dspy_metric)
    compiled_predictor = optimizer.compile(predictor, trainset=_get_few_shot_examples())

    return compiled_predictor
