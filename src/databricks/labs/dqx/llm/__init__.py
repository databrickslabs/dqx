from importlib.util import find_spec

# Check only core libraries at import-time. Model packages are loaded when LLM detection is invoked.
required_specs = [
    "dspy",
    "databricks_langchain",
]

# Check that LLM detection modules are installed
if not all(find_spec(spec) for spec in required_specs):
    raise ImportError(
        "LLM detection extras not installed; Install additional "
        "dependencies by running `pip install databricks-labs-dqx[llm]`"
    )
