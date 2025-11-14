from importlib.util import find_spec

required_specs = [
    "dspy",
]

# Check if required llm packages are installed
if not all(find_spec(spec) for spec in required_specs):
    raise ImportError(
        "llm extras not installed. Install additional dependencies by running `pip install databricks-labs-dqx[llm]`."
    )
