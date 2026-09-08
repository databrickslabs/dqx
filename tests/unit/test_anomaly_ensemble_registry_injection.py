"""A registry handed to a training strategy must be the one it uses (no Spark, no workspace).

The strategy accepts a ``ModelRegistryBase`` so a caller can substitute a backend and a test can
substitute a fake. That held for a single model and silently did not for an ensemble: a module-level
helper constructed ``EnsembleTrainer()`` with no argument, so the injected registry was dropped on the
path taken by default -- ``ensemble_size`` is 3 -- and a test's fake registry would have reached MLflow
instead of recording the call.

Reaching the assertion without Spark depends on ``EnsembleTrainer.train`` consulting the registry before
it engineers any features. That ordering is load-bearing for the test and not for the product, so it is
stated here rather than left implicit: if the two are ever swapped, this test fails for the wrong reason
and its docstring is where to look.
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


class _RegistryReached(Exception):
    """Raised by whichever registry is consulted first, to identify it without doing any real work."""


@pytest.fixture
def default_registry_spy():
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


def _train(strategy: IsolationForestTrainingStrategy, ensemble_size: int) -> None:
    train_df = create_autospec(DataFrame, instance=True)
    val_df = create_autospec(DataFrame, instance=True)
    strategy.train(
        train_df,
        val_df,
        ["amount", "quantity"],
        AnomalyParams(ensemble_size=ensemble_size),
        "cat.sch.model",
        allow_ensemble=True,
    )


def test_the_ensemble_path_uses_the_injected_registry(default_registry_spy):
    """The defect: an ensemble ignored the injected registry and used the default.

    Both halves are asserted. That the injected registry was consulted is the fix; that the default was
    not touched at all is what makes this a statement about injection rather than about which of two
    registries happens to be reached first.
    """
    injected = create_autospec(ModelRegistryBase, instance=True)
    injected.ensure_registry_configured.side_effect = _RegistryReached

    with pytest.raises(_RegistryReached):
        _train(IsolationForestTrainingStrategy(registry=injected), ensemble_size=3)

    injected.ensure_registry_configured.assert_called_once()
    assert not default_registry_spy.method_calls, "the default registry should never have been consulted"


def test_the_single_model_path_also_uses_the_injected_registry(default_registry_spy):
    """The path that already worked, pinned so the fix cannot be mistaken for the whole story.

    Reaches the registry later than the ensemble path -- after fitting -- so any Spark stand-in fails
    first; what matters here is only that the default registry is still never the one consulted.
    """
    injected = create_autospec(ModelRegistryBase, instance=True)
    injected.ensure_registry_configured.side_effect = _RegistryReached

    with pytest.raises(Exception):  # pylint: disable=broad-exception-caught
        _train(IsolationForestTrainingStrategy(registry=injected), ensemble_size=1)

    assert not default_registry_spy.method_calls, "the default registry should never have been consulted"
