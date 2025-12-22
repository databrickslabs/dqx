import pytest

from databricks.labs.dqx.anomaly.check_funcs import has_no_anomalies
from databricks.labs.dqx.anomaly import model_registry
from databricks.labs.dqx.errors import InvalidParameterError


def test_has_no_anomalies_missing_model_raises(spark, monkeypatch):
    df = spark.createDataFrame([(1, 2)], "a int, b int")

    def fake_get_active_model(_self, _table, _model_name):  # noqa: D401
        return None

    monkeypatch.setattr(model_registry.AnomalyModelRegistry, "get_active_model", fake_get_active_model)

    _condition, apply_fn = has_no_anomalies(
        merge_columns=["a"],  # Required parameter
        columns=["a", "b"],
        model="orders_anomaly",
        registry_table="catalog.schema.dqx_anomaly_models",
    )

    with pytest.raises(InvalidParameterError, match="Model 'orders_anomaly' not found"):
        apply_fn(df)
