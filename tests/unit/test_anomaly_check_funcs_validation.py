import pytest

from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.errors import InvalidParameterError


def test_has_no_row_anomalies_rejects_threshold_range():
    with pytest.raises(InvalidParameterError):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            threshold=120.0,
        )


def test_has_no_row_anomalies_rejects_negative_drift_threshold():
    with pytest.raises(InvalidParameterError):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            drift_threshold=-0.1,
        )


def test_has_no_row_anomalies_requires_fully_qualified_model():
    with pytest.raises(InvalidParameterError):
        has_no_row_anomalies(
            model_name="schema.model",
            registry_table="catalog.schema.table",
        )
