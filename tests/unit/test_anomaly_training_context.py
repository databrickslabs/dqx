"""Action-free validation at the public training service boundary."""

from unittest.mock import create_autospec

import pytest

from databricks.labs.dqx.anomaly.anomaly_engine import AnomalyEngine
from databricks.labs.dqx.anomaly.training_service import AnomalyTrainingService
from databricks.labs.dqx.anomaly.training_strategies import AnomalyTrainingStrategy, normalize_training_profile
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.errors import InvalidParameterError


@pytest.mark.parametrize("injected", [False, True])
def test_unknown_profile_fails_before_data_access(training_inputs, injected):
    spark, df = training_inputs
    strategy = None
    if injected:
        strategy = create_autospec(AnomalyTrainingStrategy, instance=True)
    service = AnomalyTrainingService(spark, strategy=strategy)

    with pytest.raises(InvalidParameterError, match="Unknown profile"):
        service.build_context(
            df,
            "catalog.schema.model",
            "catalog.schema.registry",
            columns=None,
            params=None,
            exclude_columns=None,
            profile="not-a-profile",
        )

    assert not df.mock_calls
    assert not spark.mock_calls
    if strategy is not None:
        strategy.train.assert_not_called()


def test_public_train_rejects_unknown_profile_before_data_access(training_inputs, mock_workspace_client):
    spark, df = training_inputs
    engine = AnomalyEngine(mock_workspace_client, spark=spark)
    with pytest.raises(InvalidParameterError, match="Unknown profile"):
        engine.train(df, "catalog.schema.model", "catalog.schema.registry", profile="not-a-profile")
    assert not df.mock_calls
    assert not spark.mock_calls


@pytest.mark.parametrize("basis", ["region", "event_time"])
def test_explicit_feature_and_basis_overlap_is_rejected_before_data_access(training_inputs, basis):
    spark, df = training_inputs
    options = {"baseline_by": [basis]} if basis == "region" else {"baseline_over_time": basis}
    with pytest.raises(InvalidParameterError, match="used both as"):
        AnomalyTrainingService(spark).build_context(
            df,
            "catalog.schema.model",
            "catalog.schema.registry",
            columns=["amount", basis],
            params=None,
            exclude_columns=None,
            **options,
        )
    assert not df.mock_calls
    assert not spark.mock_calls


@pytest.mark.parametrize(
    "profile, expected",
    [
        (None, "distribution"),
        ("", "distribution"),
        (" DISTRIBUTION ", "distribution"),
        (" Correlation ", "correlation"),
    ],
)
def test_profile_normalization_preserves_supported_spellings(profile, expected):
    assert normalize_training_profile(profile) == expected


@pytest.mark.parametrize("basis", ["region", "event_time"])
@pytest.mark.parametrize("via_params", [False, True])
def test_excluded_comparison_basis_fails_before_discovery(training_inputs, basis, via_params):
    spark, df = training_inputs
    options = {"baseline_by": [basis]} if basis == "region" else {"baseline_over_time": basis}
    params = AnomalyParams(**options) if via_params else None
    with pytest.raises(InvalidParameterError, match="Comparison basis columns cannot also be in exclude_columns"):
        AnomalyTrainingService(spark).build_context(
            df,
            "catalog.schema.model",
            "catalog.schema.registry",
            columns=None,
            params=params,
            exclude_columns=[basis],
            **({} if via_params else options),
        )

    assert not df.mock_calls
    assert not spark.mock_calls
