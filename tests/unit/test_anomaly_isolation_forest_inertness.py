"""The IsolationForest path must not change when a second algorithm is added.

Written before any wiring, so "the tabular path is untouched" is an executable claim rather than a
review argument. Everything here describes behaviour that existed before the Mahalanobis detector and
must survive it: the pipeline's shape, the exact hyperparameters persisted into every registry row, the
positional field order of ``AnomalyParams``, the algorithm strings that resolve to the existing scoring
strategy, and — the literal claim — the scores themselves.

If a change makes one of these fail, the change is not additive, whatever else it looks like.
"""

import dataclasses

import numpy as np
import pandas as pd
import pytest

from databricks.labs.dqx.anomaly.core import fit_sklearn_model
from databricks.labs.dqx.anomaly.scoring_strategies import resolve_scoring_strategy
from databricks.labs.dqx.config import AnomalyParams, IsolationForestConfig

# Fixed seed, fixed shape: the reference scores below were generated from exactly this frame.
_TRAIN_SEED = 1234
_TRAIN_ROWS = 500
_TRAIN_COLUMNS = ["a", "b", "c"]

# A spread of ordinary and extreme points, scored against the reference model.
_SCORE_GRID = [
    [0.0, 0.0, 0.0],
    [1.0, -1.0, 0.5],
    [4.0, 4.0, 4.0],
    [-3.0, 2.0, -2.0],
    [0.5, 0.5, -0.5],
]

# Committed reference scores. Compared with pytest.approx rather than an exact hash: scikit-learn is
# pinned only as >=1.0,<2.0, and a patch release can legitimately shift the last bits of a float
# without changing the model. A hash would fail on noise; this fails on a behaviour change.
_REFERENCE_SCORES = [
    -0.3914786863,
    -0.45006446042,
    -0.764524071473,
    -0.672658509853,
    -0.411859435027,
]


def _reference_params() -> AnomalyParams:
    """Params as production actually presents them to ``fit_sklearn_model``.

    ``contamination`` is explicit because a bare ``AnomalyParams()`` cannot be fitted at all:
    ``IsolationForestConfig.contamination`` defaults to None and scikit-learn rejects that. Production
    fills it from *expected_anomaly_rate* (default 0.02) before training, so 0.02 is the real default.
    """
    return AnomalyParams(algorithm_config=IsolationForestConfig(contamination=0.02))


def _reference_training_frame() -> pd.DataFrame:
    rng = np.random.default_rng(_TRAIN_SEED)
    return pd.DataFrame(rng.normal(size=(_TRAIN_ROWS, len(_TRAIN_COLUMNS))), columns=_TRAIN_COLUMNS)


def test_scores_match_the_committed_reference():
    """The literal inertness claim: same data in, same scores out.

    This is the assertion that catches an accidentally inserted preprocessing step, a changed default,
    or a different seed — none of which would necessarily break any other test, and all of which would
    silently move every severity percentile for every deployed tabular model.
    """
    pipeline, _ = fit_sklearn_model(_reference_training_frame(), _reference_params())

    scores = pipeline.score_samples(pd.DataFrame(_SCORE_GRID, columns=_TRAIN_COLUMNS))

    assert list(scores) == pytest.approx(_REFERENCE_SCORES, rel=1e-9)


def test_pipeline_stays_a_single_model_step():
    """``explainability`` reaches the forest through ``named_steps["model"]``, and the absence of a
    scaler is a measured decision recorded in ``fit_sklearn_model``'s docstring, not an oversight."""
    pipeline, _ = fit_sklearn_model(_reference_training_frame(), _reference_params())

    assert list(pipeline.named_steps) == ["model"]


def test_persisted_hyperparameters_are_exactly_these_keys():
    """These land in ``training.hyperparameters`` on every registry row and in ``mlflow.log_params``.

    Adding a shared key — an "algorithm" discriminator, say — would change the persisted row for every
    existing IsolationForest model, which is why the discriminator lives in ``identity.algorithm``
    instead. Asserted as a whole dict, so an addition fails rather than passing unnoticed.
    """
    _, hyperparams = fit_sklearn_model(_reference_training_frame(), _reference_params())

    assert hyperparams == {
        "contamination": 0.02,
        "num_trees": 200,
        "max_samples": None,
        "random_seed": 42,
        "feature_scaling": "none",
    }


def test_anomaly_params_field_order_and_defaults_are_stable():
    """``AnomalyParams`` is a plain dataclass, so field order is part of its public API: anything
    constructing it positionally rebinds silently if a new field is inserted rather than appended."""
    assert [f.name for f in dataclasses.fields(AnomalyParams)] == [
        "sample_fraction",
        "max_rows",
        "train_ratio",
        "ensemble_size",
        "algorithm_config",
        "feature_engineering",
        "baseline_by",
    ]

    defaults = AnomalyParams()
    assert defaults.sample_fraction == 0.3
    assert defaults.max_rows == 1_000_000
    assert defaults.train_ratio == 0.8
    assert defaults.ensemble_size == 3
    assert defaults.baseline_by is None


@pytest.mark.parametrize("algorithm", ["IsolationForest", "IsolationForest_Ensemble_3"])
def test_existing_algorithm_strings_still_resolve(algorithm: str):
    """Every model already in a registry table carries one of these strings. Widening the resolver for
    a new algorithm must not stop it matching the old ones."""
    strategy = resolve_scoring_strategy(algorithm)

    assert strategy.supports(algorithm)
