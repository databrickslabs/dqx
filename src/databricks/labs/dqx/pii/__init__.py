from databricks.labs.dqx.utils import missing_required_packages

# Check only core libraries at import-time. Model packages are loaded when pii detection is invoked.
required_specs = [
    "presidio_analyzer",
    "spacy",
]

# Check that PII detection modules are installed
if missing_required_packages(required_specs):
    raise ImportError(
        "PII detection extras not installed."
        "Install additional dependencies by running `pip install databricks-labs-dqx[pii]`."
    )
