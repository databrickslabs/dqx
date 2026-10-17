"""Attribution dependencies follow the stored detector, not check construction (no Spark)."""

import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest


_REPO_ROOT = Path(__file__).parents[2]


def _run_without_shap(script: str) -> None:
    """Run an import-isolated probe where importing the optional third-party package fails."""
    environment = os.environ.copy()
    existing_path = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = os.pathsep.join(
        path for path in (str(_REPO_ROOT / "src"), str(_REPO_ROOT), existing_path) if path
    )
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        cwd=_REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr


_BLOCK_SHAP_IMPORT = """
import importlib.abc
import sys


class BlockShap(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "shap" or fullname.startswith("shap."):
            raise ModuleNotFoundError("shap intentionally unavailable for this test")
        return None


sys.meta_path.insert(0, BlockShap())
"""


def test_correlation_attribution_works_when_shap_cannot_be_imported() -> None:
    """The correlation detector supplies exact evidence and must never load TreeSHAP."""
    _run_without_shap(
        _BLOCK_SHAP_IMPORT
        + """
import numpy as np
import pandas as pd

from databricks.labs.dqx.anomaly.correlation_detector import MahalanobisDetector
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.anomaly.explainability import SHAP_AVAILABLE, compute_row_attributions

has_no_row_anomalies(
    model_name="catalog.schema.correlation",
    registry_table="catalog.schema.registry",
    enable_contributions=True,
    enable_ai_explanation=False,
)
rng = np.random.default_rng(7)
training = rng.normal(size=(500, 2))
detector = MahalanobisDetector().fit(training)
probe = np.array([[5.0, 0.25], [0.1, -4.0]])
frame = pd.DataFrame(probe, columns=["amount", "quantity"])
attribution, valid, keys = compute_row_attributions(detector, frame, ["amount", "quantity"])

assert SHAP_AVAILABLE is False
assert keys == ["amount", "quantity"]
assert valid.tolist() == [True, True]
assert attribution.shape == (2, 2)
assert np.isfinite(attribution).all()
assert (attribution > 0.0).any(axis=1).all()
np.testing.assert_allclose(attribution, detector.feature_contributions(probe), rtol=1e-12, atol=1e-12)
"""
    )


@pytest.mark.parametrize("algorithm", ["IsolationForest", "IsolationForest_Ensemble_3"])
def test_distribution_contributions_fail_before_spark_when_shap_cannot_be_imported(algorithm: str) -> None:
    """IsolationForest needs SHAP, while disabling contributions still reaches normal scoring validation."""
    _run_without_shap(
        _BLOCK_SHAP_IMPORT
        + f"\nalgorithm = {algorithm!r}\n"
        + """
from datetime import datetime
from unittest.mock import create_autospec

from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.model_config import (
    AnomalyModelRecord,
    FeatureEngineering,
    GroupingConfig,
    ModelIdentity,
    TrainingMetadata,
    compute_config_hash,
)
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.anomaly.scoring_strategies import GlobalModelScoringStrategy
from databricks.labs.dqx.errors import InvalidParameterError

record = AnomalyModelRecord(
    identity=ModelIdentity("catalog.schema.model", "models:/catalog.schema.model/1", algorithm, "run"),
    training=TrainingMetadata(["amount"], {}, 100, datetime(2024, 1, 1)),
    features=FeatureEngineering(),
    grouping=GroupingConfig(config_hash=compute_config_hash(["amount"], None)),
)
with_contributions = ScoringConfig(
    columns=["amount"],
    model_name=record.identity.model_name,
    registry_table="catalog.schema.registry",
    threshold=95.0,
    merge_columns=["row_id"],
    enable_contributions=True,
    enable_ai_explanation=False,
)
frame = create_autospec(DataFrame, instance=True)
strategy = GlobalModelScoringStrategy()
try:
    strategy.score_global(frame, record, with_contributions)
except InvalidParameterError as error:
    assert "requires the 'shap' dependency" in str(error)
else:
    raise AssertionError("distribution contributions should require shap")
assert frame.mock_calls == []

without_contributions = ScoringConfig(
    columns=["amount"],
    model_name=record.identity.model_name,
    registry_table="catalog.schema.registry",
    threshold=95.0,
    merge_columns=["row_id"],
    enable_contributions=False,
    enable_ai_explanation=False,
)
frame.withColumn.return_value = frame
try:
    strategy.score_global(frame, record, without_contributions)
except InvalidParameterError as error:
    assert "missing feature_metadata" in str(error)
else:
    raise AssertionError("the existing scoring validation should reject incomplete metadata")
"""
    )
