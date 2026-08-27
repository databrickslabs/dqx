"""Training strategy pattern for row anomaly detection.

Enables different anomaly detection algorithms through a common interface.
Currently implements IsolationForest, but designed for extensibility.

Uses dependency injection for the model registry, enabling:
- Consistent registration path with EnsembleTrainer
- Easy mocking/testing
- Potential for alternative backends
"""

import dataclasses
import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from databricks.labs.dqx.anomaly.core import (
    compute_score_quantiles,
    compute_validation_metrics,
    fit_isolation_forest,
    prepare_engineered_pandas,
    prepare_training_features,
)
from databricks.labs.dqx.anomaly.ensemble_training import train_ensemble
from databricks.labs.dqx.anomaly.mlflow_registry import ModelRegistryBase, get_default_registry
from databricks.labs.dqx.anomaly.timeseries_detector import fit_mahalanobis_model
from databricks.labs.dqx.anomaly.types import TrainingResult
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.errors import InvalidParameterError

logger = logging.getLogger(__name__)

# Persisted in ModelIdentity.algorithm and matched by the scoring resolver, so it is a stored contract:
# changing it would orphan every model already trained with this algorithm.
MAHALANOBIS_ALGORITHM = "Mahalanobis"

# The public profile vocabulary. It describes the *data a user has*, not the algorithm DQX picks for it.
#
# There is deliberately no "auto": DQX never selects the algorithm on a user's behalf. Choosing
# correctly cannot be verified without labels, which an unsupervised tool does not have, and the one
# cheap signal for "this looks temporal" was measured and rejected -- lag-1 autocorrelation is
# confounded by any ordering correlated with the values, which sorted warehouse storage produces
# routinely (see benchmarks/anomaly_conditioning/profile_advisory_gate.py). A value named "auto" would
# therefore have promised a selection that never happens.
PROFILE_TABULAR = "tabular"
PROFILE_TIMESERIES = "timeseries"
SUPPORTED_PROFILES = (PROFILE_TABULAR, PROFILE_TIMESERIES)
# Unset means the tabular detector: exactly the behaviour that predates this option.
DEFAULT_PROFILE = PROFILE_TABULAR


class AnomalyTrainingStrategy(ABC):
    """Training strategy interface for row anomaly models.

    Implement this interface to add new anomaly detection algorithms.
    Uses dependency injection for the model registry.
    """

    name: str

    def __init__(self, registry: ModelRegistryBase | None = None) -> None:
        """Initialize strategy with optional registry.

        Args:
            registry: Model registry to use. Defaults to MLflow/Unity Catalog.
        """
        self._registry = registry or get_default_registry()

    @abstractmethod
    def train(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        columns: list[str],
        params: AnomalyParams,
        model_name: str,
        *,
        allow_ensemble: bool,
    ) -> TrainingResult:
        """Train an anomaly detection model.

        Args:
            train_df: Training DataFrame
            val_df: Validation DataFrame
            columns: Feature columns to use
            params: Training parameters
            model_name: Name for registered model
            allow_ensemble: Whether to allow ensemble training

        Returns:
            TrainingResult with model URI, metrics, and metadata
        """


class IsolationForestTrainingStrategy(AnomalyTrainingStrategy):
    """IsolationForest training strategy (default).

    Uses sklearn's IsolationForest algorithm with optional ensemble training.
    Both single-model and ensemble paths use the same ModelRegistryBase abstraction.
    """

    name = "isolation_forest"

    def train(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        columns: list[str],
        params: AnomalyParams,
        model_name: str,
        *,
        allow_ensemble: bool,
    ) -> TrainingResult:
        """Train IsolationForest model(s).

        If allow_ensemble and params.ensemble_size > 1, trains an ensemble.
        Otherwise trains a single model using the registry abstraction.
        """
        ensemble_size = params.ensemble_size if allow_ensemble and params.ensemble_size else 1

        if ensemble_size > 1:
            model_uris, hyperparams, validation_metrics, score_quantiles, feature_metadata = train_ensemble(
                train_df, val_df, columns, params, ensemble_size, model_name
            )
            model_uri = ",".join(model_uris)
            run_id = "ensemble"
        else:
            model, hyperparams, feature_metadata = fit_isolation_forest(train_df, columns, params)
            validation_metrics = compute_validation_metrics(model, val_df, columns, feature_metadata)
            score_quantiles = compute_score_quantiles(model, train_df, columns, feature_metadata)

            self._registry.ensure_registry_configured()
            train_pandas = prepare_engineered_pandas(train_df, feature_metadata)
            model_uri, run_id = self._registry.register_model_with_signature_inference(
                model, model_name, train_pandas, hyperparams, validation_metrics
            )

        algorithm = f"IsolationForest_Ensemble_{ensemble_size}" if ensemble_size > 1 else "IsolationForest"
        return TrainingResult(
            model_uri=model_uri,
            run_id=run_id,
            hyperparams=hyperparams,
            validation_metrics=validation_metrics,
            score_quantiles=score_quantiles,
            feature_metadata=feature_metadata,
            ensemble_size=ensemble_size,
            algorithm=algorithm,
        )


class MahalanobisTrainingStrategy(AnomalyTrainingStrategy):
    """Correlation-aware training strategy, for multivariate metrics such as time series.

    Same feature engineering, same registry, same metrics as the IsolationForest strategy — only the
    estimator differs. See ``timeseries_detector`` for why: IsolationForest splits one feature at a
    time, so anomalies that are broken *correlations* rather than extreme single values are close to
    invisible to it. Measured on SMD, incident coverage inside a 1%-of-rows alert budget is 0.359 for
    IsolationForest and 0.821 here on a clean training split, and 0.333 against 0.795 when the training
    data itself contains anomalies -- which is the case that matters, because DQX fits a random sample of
    the user's table rather than a curated one.
    """

    name = "mahalanobis"

    def train(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        columns: list[str],
        params: AnomalyParams,
        model_name: str,
        *,
        allow_ensemble: bool,
    ) -> TrainingResult:
        """Train a single correlation-aware model.

        *allow_ensemble* is accepted and ignored. The estimator is deterministic, so the ensemble --
        which exists to average away the randomness of differently-seeded forests and to report a
        confidence standard deviation from their disagreement -- would train N identical models, pay N
        times the cost, and report a spread of exactly zero. Declining it is the honest behaviour, and
        it is logged rather than silently dropped.
        """
        if allow_ensemble and params.ensemble_size and params.ensemble_size > 1:
            logger.info(
                f"Ignoring ensemble_size={params.ensemble_size} for the {self.name} algorithm: it is "
                "deterministic, so every ensemble member would be an identical model."
            )

        # Feature engineering is deliberately the shared implementation: this algorithm changes how
        # rows are scored, not how columns become features, so it inherits one-hot encoding, frequency
        # encoding, datetime cyclicals, null indicators and the group-relative baseline features as-is.
        train_pandas, feature_metadata = prepare_training_features(train_df, columns, params)
        model, hyperparams = fit_mahalanobis_model(train_pandas, params)
        validation_metrics = compute_validation_metrics(model, val_df, columns, feature_metadata)
        score_quantiles = compute_score_quantiles(model, train_df, columns, feature_metadata)

        self._registry.ensure_registry_configured()
        train_pandas = prepare_engineered_pandas(train_df, feature_metadata)
        model_uri, run_id = self._registry.register_model_with_signature_inference(
            model, model_name, train_pandas, hyperparams, validation_metrics
        )

        return TrainingResult(
            model_uri=model_uri,
            run_id=run_id,
            hyperparams=hyperparams,
            validation_metrics=validation_metrics,
            score_quantiles=score_quantiles,
            feature_metadata=feature_metadata,
            ensemble_size=1,
            algorithm=MAHALANOBIS_ALGORITHM,
        )


def resolve_training_profile(
    profile: str | None,
    params: AnomalyParams,
    strategy_override: AnomalyTrainingStrategy | None = None,
) -> tuple[AnomalyTrainingStrategy, AnomalyParams]:
    """Map a *profile* to the strategy that implements it, and any parameter defaults it implies.

    *strategy_override*, when given, replaces the resolved strategy — this is how an explicitly
    injected strategy (including a test double) stays authoritative over the profile. Parameters still
    follow the profile, because the parameter defaults are declared by the profile rather than by
    whichever object ends up doing the training. The precedence rule lives here, in a pure function,
    rather than inside the training service, so it can be asserted without a Spark session.

    Pure: it returns parameters rather than mutating the caller's. For the tabular profiles it returns
    the **same object**, so choosing a profile explicitly cannot perturb an existing configuration.

    The profiles describe the data a user has, not the algorithm DQX picks for it:

    * ``tabular`` (the default, and what an unset profile means) — IsolationForest, exactly the
      behaviour that predates this option.
    * ``timeseries`` — the correlation-aware detector, with the ensemble collapsed to a single model.
      Needs no time column: it models cross-metric correlation, not time.

    There is no automatic option. DQX will not switch algorithms on a user's behalf: the choice cannot
    be verified without labels, and a silent estimator change would move every score a user has
    calibrated thresholds against. The resolved profile is logged on every run so the default is visible
    rather than implicit.
    """
    requested = (profile or DEFAULT_PROFILE).strip().lower()

    if requested == PROFILE_TABULAR:
        strategy: AnomalyTrainingStrategy = IsolationForestTrainingStrategy()
        resolved_params = params
    elif requested == PROFILE_TIMESERIES:
        strategy = MahalanobisTrainingStrategy()
        resolved_params = dataclasses.replace(params, ensemble_size=1)
    else:
        raise InvalidParameterError(f"Unknown profile {profile!r}. Choose one of: {', '.join(SUPPORTED_PROFILES)}.")

    return (strategy_override or strategy), resolved_params
