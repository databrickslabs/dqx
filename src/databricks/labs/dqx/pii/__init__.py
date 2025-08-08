from importlib.util import find_spec

# Check that PII detection modules are installed
if not find_spec("databricks.labs.dqx.pii"):
    raise ImportError(
        "PII detection extras not installed; Install additional "
        "dependencies by running `pip install databricks-labs-dqx[pii]"
    )
