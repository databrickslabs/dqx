"""
Anomaly detection public API.

Requires the 'anomaly' extras: pip install databricks-labs-dqx[anomaly]
"""

from databricks.labs.dqx.package_utils import missing_required_packages

# Check that anomaly detection dependencies are installed
required_specs = [
    "mlflow",
    "sklearn",
    "cloudpickle",
]

if missing_required_packages(required_specs):
    raise ImportError(
        "anomaly extras not installed. Install additional dependencies by running "
        "`pip install databricks-labs-dqx[anomaly]`."
    )
