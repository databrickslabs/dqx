from databricks.labs.dqx.utils import missing_required_packages

required_specs = [
    "dspy",
]

# Check if required llm packages are installed
if missing_required_packages(required_specs):
    raise ImportError(
        "llm extras not installed. Install additional dependencies by running `pip install databricks-labs-dqx[llm]`."
    )
