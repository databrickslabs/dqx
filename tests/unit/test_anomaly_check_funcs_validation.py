from types import SimpleNamespace

import pytest

from databricks.labs.dqx.anomaly.check_funcs import has_no_anomalies
from databricks.labs.dqx.errors import InvalidParameterError


def test_has_no_anomalies_rejects_threshold_type():
    with pytest.raises(InvalidParameterError):
        has_no_anomalies(
            model="catalog.schema.model",
            registry_table="catalog.schema.table",
            score_threshold="0.5",  # type: ignore[arg-type]
        )


def test_has_no_anomalies_rejects_threshold_range():
    with pytest.raises(InvalidParameterError):
        has_no_anomalies(
            model="catalog.schema.model",
            registry_table="catalog.schema.table",
            score_threshold=1.5,
        )


def test_has_no_anomalies_rejects_negative_drift_threshold():
    with pytest.raises(InvalidParameterError):
        has_no_anomalies(
            model="catalog.schema.model",
            registry_table="catalog.schema.table",
            drift_threshold=-0.1,
        )


def test_has_no_anomalies_rejects_drift_threshold_type():
    with pytest.raises(InvalidParameterError):
        has_no_anomalies(
            model="catalog.schema.model",
            registry_table="catalog.schema.table",
            drift_threshold=True,  # type: ignore[arg-type]
        )


def test_has_no_anomalies_rejects_row_filter_type():
    with pytest.raises(InvalidParameterError):
        has_no_anomalies(
            model="catalog.schema.model",
            registry_table="catalog.schema.table",
            row_filter=123,  # type: ignore[arg-type]
        )


def test_has_no_anomalies_rejects_include_contributions_type():
    with pytest.raises(InvalidParameterError):
        has_no_anomalies(
            model="catalog.schema.model",
            registry_table="catalog.schema.table",
            include_contributions="yes",  # type: ignore[arg-type]
        )


def test_has_no_anomalies_rejects_include_confidence_type():
    with pytest.raises(InvalidParameterError):
        has_no_anomalies(
            model="catalog.schema.model",
            registry_table="catalog.schema.table",
            include_confidence="yes",  # type: ignore[arg-type]
        )


def test_has_no_anomalies_requires_fully_qualified_model():
    with pytest.raises(InvalidParameterError):
        has_no_anomalies(
            model="schema.model",
            registry_table="catalog.schema.table",
        )


def test_has_no_anomalies_rejects_internal_row_id():
    df = SimpleNamespace(columns=["id", "value", "_dqx_row_id"])
    _, apply_fn = has_no_anomalies(
        model="catalog.schema.model",
        registry_table="catalog.schema.table",
        include_contributions=False,
    )
    with pytest.raises(InvalidParameterError):
        apply_fn(df)
