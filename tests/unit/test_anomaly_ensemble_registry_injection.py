"""An unusable injected registry stops ensemble training before any data is read.

This covers failure propagation and dependency isolation, not successful model registration.
The successful training and persistence path requires real Spark and belongs in integration tests.
"""

from unittest.mock import create_autospec

import pytest
from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.mlflow_registry import (
    ModelRegistryBase,
    get_default_registry,
    set_default_registry,
)
from databricks.labs.dqx.anomaly.training_strategies import IsolationForestTrainingStrategy
from databricks.labs.dqx.config import AnomalyParams


class RegistryUnavailableError(Exception):
    """A configured registry boundary that cannot accept a training run."""


@pytest.fixture(name="default_registry_spy")
def fixture_default_registry_spy():
    """Install a fake as the process default and restore the real one afterwards.

    Installed through the public *set_default_registry*, which the module documents as being for exactly
    this. It matters for more than tidiness: without it the real MLflow registry would run against the
    repository's local tracking store on the failing path and leave state behind.
    """
    original = get_default_registry()
    spy = create_autospec(ModelRegistryBase, instance=True)
    set_default_registry(spy)
    try:
        yield spy
    finally:
        set_default_registry(original)


@pytest.mark.parametrize("ensemble_size", [2, 3])
def test_unavailable_injected_registry_stops_training_before_data_access(
    default_registry_spy, ensemble_size: int
) -> None:
    injected = create_autospec(ModelRegistryBase, instance=True)
    injected.ensure_registry_configured.side_effect = RegistryUnavailableError("Registry is unavailable")
    train_df = create_autospec(DataFrame, instance=True)
    val_df = create_autospec(DataFrame, instance=True)

    with pytest.raises(RegistryUnavailableError, match="Registry is unavailable"):
        IsolationForestTrainingStrategy(registry=injected).train(
            train_df,
            val_df,
            ["amount", "quantity"],
            AnomalyParams(ensemble_size=ensemble_size),
            "catalog.schema.model",
            allow_ensemble=True,
        )

    injected.ensure_registry_configured.assert_called_once()
    assert not train_df.mock_calls
    assert not val_df.mock_calls
    assert not default_registry_spy.method_calls
