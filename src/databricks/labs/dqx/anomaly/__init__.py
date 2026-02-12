"""
Anomaly detection public API.

Requires the 'anomaly' extras: pip install databricks-labs-dqx[anomaly]
"""

try:
    from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
    from databricks.labs.dqx.anomaly.check_funcs import has_no_anomalies
    from databricks.labs.dqx.config import AnomalyConfig
except ImportError as e:
    raise ImportError(
        "anomaly extras not installed. Install additional dependencies by running "
        "`pip install databricks-labs-dqx[anomaly]`."
    ) from e

__all__ = ["AnomalyEngine", "has_no_anomalies", "AnomalyConfig"]
