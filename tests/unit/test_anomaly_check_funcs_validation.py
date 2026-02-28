# Tests target internal scoring/validation APIs by design.
# pylint: disable=protected-access

from unittest.mock import create_autospec, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.anomaly import check_funcs
from databricks.labs.dqx.anomaly.check_funcs import (
    ScoringConfig,
    has_no_row_anomalies,
    resolve_scoring_strategy,
)
from databricks.labs.dqx.anomaly.model_config import (
    AnomalyModelRecord,
    ModelIdentity,
    TrainingMetadata,
)
from databricks.labs.dqx.anomaly.model_registry import AnomalyModelRegistry
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


def test_resolve_scoring_strategy_raises_for_unknown_algorithm():
    """resolve_scoring_strategy raises InvalidParameterError for an unrecognised algorithm name."""
    with pytest.raises(InvalidParameterError, match="Unsupported model algorithm 'UnknownAlgorithm'"):
        resolve_scoring_strategy("UnknownAlgorithm")


def test_resolve_scoring_strategy_returns_strategy_for_isolation_forest():
    """resolve_scoring_strategy succeeds for any IsolationForest algorithm variant."""
    strategy = resolve_scoring_strategy("IsolationForestV1")
    assert strategy is not None
    assert strategy.supports("IsolationForestV1")


def test_run_anomaly_scoring_raises_when_model_not_found_and_no_fallback():
    """When get_active_model returns None and segmented fallback returns None, InvalidParameterError is raised."""
    mock_spark = create_autospec(SparkSession, instance=True)
    mock_df = create_autospec(DataFrame, instance=True)
    mock_df.sparkSession = mock_spark

    registry_table = "catalog.schema.registry"
    model_name = "catalog.schema.my_model"
    config = ScoringConfig(
        columns=["a", "b"],
        model_name=model_name,
        registry_table=registry_table,
        threshold=95.0,
        merge_columns=[],
    )

    with patch.object(check_funcs, "AnomalyModelRegistry") as mock_registry_cls:
        mock_registry = create_autospec(AnomalyModelRegistry, instance=True)
        mock_registry.get_active_model.return_value = None
        mock_registry.get_all_segment_models.return_value = []  # fallback returns None
        mock_registry_cls.return_value = mock_registry

        with pytest.raises(InvalidParameterError) as exc_info:
            check_funcs._run_anomaly_scoring(mock_df, config, registry_table, model_name)

    assert model_name in str(exc_info.value)
    assert registry_table in str(exc_info.value)
    assert "Train first" in str(exc_info.value)


def test_load_segment_models_raises_when_no_segments():
    """_load_segment_models raises when get_all_segment_models returns an empty list."""
    registry_table = "catalog.schema.registry"
    model_name = "catalog.schema.my_model"
    config = ScoringConfig(
        columns=["a", "b"],
        model_name=model_name,
        registry_table=registry_table,
        threshold=95.0,
        merge_columns=[],
    )
    mock_registry = create_autospec(AnomalyModelRegistry, instance=True)
    mock_registry.get_all_segment_models.return_value = []

    with pytest.raises(InvalidParameterError) as exc_info:
        check_funcs._load_segment_models(mock_registry, config)

    assert "No segment models found for base model" in str(exc_info.value)
    assert model_name in str(exc_info.value)
    assert "Train segmented models first" in str(exc_info.value)


def test_score_segmented_raises_when_no_segments():
    """_score_segmented raises when no segment models are found (all_segments empty)."""
    mock_df = create_autospec(DataFrame, instance=True)
    registry_table = "catalog.schema.registry"
    model_name = "catalog.schema.my_model"
    config = ScoringConfig(
        columns=["a", "b"],
        model_name=model_name,
        registry_table=registry_table,
        threshold=95.0,
        merge_columns=[],
    )
    mock_registry = create_autospec(AnomalyModelRegistry, instance=True)
    mock_registry.get_all_segment_models.return_value = []

    with pytest.raises(InvalidParameterError) as exc_info:
        check_funcs._score_segmented(mock_df, config, mock_registry)

    assert "No segment models found for base model" in str(exc_info.value)
    assert model_name in str(exc_info.value)
    assert "Train segmented models first" in str(exc_info.value)


def test_extract_quantile_points_raises_when_score_quantiles_missing():
    """_extract_quantile_points raises when record.training.score_quantiles is missing or empty."""
    record = create_autospec(AnomalyModelRecord, instance=True)
    record.identity = create_autospec(ModelIdentity, instance=True)
    record.identity.model_name = "catalog.schema.my_model"
    record.training = create_autospec(TrainingMetadata, instance=True)
    record.training.score_quantiles = None

    with pytest.raises(InvalidParameterError) as exc_info:
        check_funcs._extract_quantile_points(record)

    assert "missing score quantiles" in str(exc_info.value)
    assert "catalog.schema.my_model" in str(exc_info.value)
    assert "Retrain the model" in str(exc_info.value)


def test_extract_quantile_points_raises_when_score_quantiles_empty():
    """_extract_quantile_points raises when record.training.score_quantiles is empty dict."""
    record = create_autospec(AnomalyModelRecord, instance=True)
    record.identity = create_autospec(ModelIdentity, instance=True)
    record.identity.model_name = "catalog.schema.other_model"
    record.training = create_autospec(TrainingMetadata, instance=True)
    record.training.score_quantiles = {}

    with pytest.raises(InvalidParameterError) as exc_info:
        check_funcs._extract_quantile_points(record)

    assert "missing score quantiles" in str(exc_info.value)
    assert "catalog.schema.other_model" in str(exc_info.value)
