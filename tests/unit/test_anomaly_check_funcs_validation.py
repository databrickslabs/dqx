from unittest.mock import patch

import pytest

from databricks.labs.dqx.anomaly import check_funcs
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.errors import InvalidParameterError


def test_has_no_row_anomalies_requires_model_name():
    """model_name is required (non-empty)."""
    with pytest.raises(InvalidParameterError, match="model_name parameter is required"):
        has_no_row_anomalies(
            model_name="",
            registry_table="catalog.schema.table",
        )
    with pytest.raises(InvalidParameterError, match="model_name parameter is required"):
        has_no_row_anomalies(
            model_name=None,
            registry_table="catalog.schema.table",
        )


def test_has_no_row_anomalies_requires_registry_table():
    """registry_table is required (non-empty)."""
    with pytest.raises(InvalidParameterError, match="registry_table parameter is required"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="",
        )
    with pytest.raises(InvalidParameterError, match="registry_table parameter is required"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table=None,
        )


def test_has_no_row_anomalies_requires_fully_qualified_model():
    """model_name must be fully qualified (catalog.schema.table)."""
    with pytest.raises(InvalidParameterError, match="model_name must be fully qualified"):
        has_no_row_anomalies(
            model_name="schema.model",
            registry_table="catalog.schema.table",
        )


def test_has_no_row_anomalies_requires_fully_qualified_registry_table():
    """registry_table must be fully qualified (catalog.schema.table)."""
    with pytest.raises(InvalidParameterError, match="registry_table must be fully qualified"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="schema.table",
        )


def test_has_no_row_anomalies_rejects_threshold_out_of_range():
    """threshold must be between 0.0 and 100.0."""
    with pytest.raises(InvalidParameterError, match="threshold must be between 0.0 and 100.0"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            threshold=120.0,
        )
    with pytest.raises(InvalidParameterError, match="threshold must be between 0.0 and 100.0"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            threshold=-0.1,
        )


def test_has_no_row_anomalies_rejects_non_positive_drift_threshold():
    """drift_threshold must be > 0 when provided."""
    with pytest.raises(InvalidParameterError, match="drift_threshold must be greater than 0"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            drift_threshold=-0.1,
        )
    with pytest.raises(InvalidParameterError, match="drift_threshold must be greater than 0"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            drift_threshold=0,
        )


def test_has_no_row_anomalies_include_contributions_requires_shap():
    """include_contributions=True requires SHAP; raises when SHAP is not available."""
    with pytest.raises(InvalidParameterError, match="include_contributions=True requires the 'shap' dependency"):
        with patch.object(check_funcs, "SHAP_AVAILABLE", False):
            has_no_row_anomalies(
                model_name="catalog.schema.model",
                registry_table="catalog.schema.table",
                include_contributions=True,
            )
