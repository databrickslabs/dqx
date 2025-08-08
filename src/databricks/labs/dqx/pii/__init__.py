from importlib.util import find_spec

required_specs = [
    "presidio_analyzer",
    "spacy",
    "en_core_web_sm",
    "en_core_web_md",
    "en_core_web_lg",
]

# Check that PII detection modules are installed
if not all(find_spec(spec) for spec in required_specs):
    raise ImportError(
        "PII detection extras not installed; Install additional "
        "dependencies by running `pip install databricks-labs-dqx[pii]"
    )
