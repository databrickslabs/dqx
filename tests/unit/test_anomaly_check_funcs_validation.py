from datetime import datetime
from unittest.mock import create_autospec, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from databricks.labs.dqx.anomaly import check_funcs, model_discovery, scoring_orchestrator
from databricks.labs.dqx.anomaly.check_funcs import has_no_row_anomalies
from databricks.labs.dqx.anomaly.model_discovery import get_quantile_points_for_severity
from databricks.labs.dqx.anomaly.scoring_config import ScoringConfig
from databricks.labs.dqx.anomaly.scoring_strategies import resolve_scoring_strategy
from databricks.labs.dqx.anomaly.model_config import (
    AnomalyModelRecord,
    FeatureEngineering,
    ModelIdentity,
    SegmentationConfig,
    TrainingMetadata,
    compute_config_hash,
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


def test_has_no_row_anomalies_enable_contributions_requires_shap():
    """enable_contributions=True requires SHAP; raises when SHAP is not available."""
    with pytest.raises(InvalidParameterError, match="enable_contributions=True requires the 'shap' dependency"):
        with patch.object(check_funcs, "SHAP_AVAILABLE", False):
            has_no_row_anomalies(
                model_name="catalog.schema.model",
                registry_table="catalog.schema.table",
                enable_contributions=True,
            )


def test_has_no_row_anomalies_ai_explanation_requires_contributions():
    """enable_ai_explanation=True without enable_contributions=True raises InvalidParameterError."""
    with pytest.raises(InvalidParameterError, match="enable_ai_explanation=True requires enable_contributions=True"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            enable_ai_explanation=True,
            enable_contributions=False,
        )


def test_has_no_row_anomalies_ai_explanation_requires_dspy():
    """enable_ai_explanation=True raises when dspy is not importable."""
    from databricks.labs.dqx.anomaly import anomaly_llm_explainer

    with pytest.raises(InvalidParameterError, match="enable_ai_explanation=True requires the 'dspy' dependency"):
        with patch.object(check_funcs, "SHAP_AVAILABLE", True):
            with patch.object(anomaly_llm_explainer, "DSPY_AVAILABLE", False):
                import importlib

                importlib.reload(check_funcs)
                # Patch DSPY_AVAILABLE on the check_funcs module after reload
                with patch.object(check_funcs, "DSPY_AVAILABLE", False):
                    has_no_row_anomalies(
                        model_name="catalog.schema.model",
                        registry_table="catalog.schema.table",
                        enable_ai_explanation=True,
                        enable_contributions=True,
                    )


def test_has_no_row_anomalies_ai_explanation_requires_dspy_direct():
    """enable_ai_explanation=True raises when DSPY_AVAILABLE is False on check_funcs module."""
    from databricks.labs.dqx.anomaly import check_funcs as cf

    with pytest.raises(InvalidParameterError, match="enable_ai_explanation=True requires the 'dspy' dependency"):
        with patch.object(cf, "SHAP_AVAILABLE", True), patch.object(cf, "DSPY_AVAILABLE", False):
            has_no_row_anomalies(
                model_name="catalog.schema.model",
                registry_table="catalog.schema.table",
                enable_ai_explanation=True,
                enable_contributions=True,
            )


def test_has_no_row_anomalies_redact_columns_must_be_list():
    """redact_columns that is not a list raises InvalidParameterError."""
    with pytest.raises(InvalidParameterError, match="redact_columns must be a list"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            redact_columns="customer_id",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("bad_value", [0, -1, -100])
def test_has_no_row_anomalies_max_groups_must_be_positive(bad_value):
    """max_groups <= 0 raises InvalidParameterError."""
    with pytest.raises(InvalidParameterError, match="max_groups must be a positive integer"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            max_groups=bad_value,
        )


@pytest.mark.parametrize("bad_value", ["500", 500.0, None, [500]])
def test_has_no_row_anomalies_max_groups_must_be_int(bad_value):
    """max_groups non-int raises InvalidParameterError."""
    with pytest.raises(InvalidParameterError, match="max_groups must be a positive integer"):
        has_no_row_anomalies(
            model_name="catalog.schema.model",
            registry_table="catalog.schema.table",
            max_groups=bad_value,  # type: ignore[arg-type]
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
    mock_df.withColumn.return_value = mock_df
    mock_df.columns = ["a", "b"]

    registry_table = "catalog.schema.registry"
    model_name = "catalog.schema.my_model"

    # Minimal record so discovery (fetch_model_columns_and_segments) succeeds.
    record = create_autospec(AnomalyModelRecord, instance=True)
    record.training = create_autospec(TrainingMetadata, instance=True)
    record.training.columns = ["a", "b"]
    record.segmentation = create_autospec(SegmentationConfig, instance=True)
    record.segmentation.segment_by = None  # global model for discovery

    mock_registry = create_autospec(AnomalyModelRegistry, instance=True)
    # Discovery calls get_active_model once -> return record; orchestrator calls it -> None.
    mock_registry.get_active_model.side_effect = [record, None]
    mock_registry.get_all_segment_models.return_value = []  # fallback returns None
    with patch.object(model_discovery, "AnomalyModelRegistry") as mock_cls_disc:
        with patch.object(scoring_orchestrator, "AnomalyModelRegistry") as mock_cls_orch:
            mock_cls_disc.return_value = mock_registry
            mock_cls_orch.return_value = mock_registry

            _, apply_fn, _ = has_no_row_anomalies(
                model_name=model_name,
                registry_table=registry_table,
            )
            with pytest.raises(InvalidParameterError) as exc_info:
                apply_fn(mock_df)

    assert model_name in str(exc_info.value)
    assert registry_table in str(exc_info.value)
    assert "Train first" in str(exc_info.value)


def test_load_segment_models_raises_when_no_segments():
    """Loading segment models raises when get_all_segment_models returns empty on second call."""
    registry_table = "catalog.schema.registry"
    model_name = "catalog.schema.my_model"
    mock_spark = create_autospec(SparkSession, instance=True)
    mock_df = create_autospec(DataFrame, instance=True)
    mock_df.sparkSession = mock_spark
    mock_df.withColumn.return_value = mock_df
    mock_df.columns = ["a", "b"]

    segment = create_autospec(AnomalyModelRecord, instance=True)
    segment.identity = create_autospec(ModelIdentity, instance=True)
    segment.identity.model_name = model_name
    segment.identity.algorithm = "IsolationForestV1"
    segment.training = create_autospec(TrainingMetadata, instance=True)
    segment.training.columns = ["a", "b"]
    segment.training.training_time = datetime.min
    segment.segmentation = create_autospec(SegmentationConfig, instance=True)
    segment.segmentation.segment_by = ["region"]

    # Discovery and orchestrator both create a registry; patch both so the same mock is used.
    mock_registry = create_autospec(AnomalyModelRegistry, instance=True)
    mock_registry.get_active_model.return_value = None
    mock_registry.get_all_segment_models.side_effect = [[segment], []]
    with patch.object(model_discovery, "AnomalyModelRegistry") as mock_cls_disc:
        with patch.object(scoring_orchestrator, "AnomalyModelRegistry") as mock_cls_orch:
            mock_cls_disc.return_value = mock_registry
            mock_cls_orch.return_value = mock_registry

            _, apply_fn, _ = has_no_row_anomalies(
                model_name=model_name,
                registry_table=registry_table,
            )
            with pytest.raises(InvalidParameterError) as exc_info:
                apply_fn(mock_df)

    assert "No segment models found for base model" in str(exc_info.value)
    assert model_name in str(exc_info.value)
    assert "Train segmented models first" in str(exc_info.value)


def test_score_segmented_raises_when_no_segments():
    """Scoring strategy raises when no segment models are passed (all_segments empty)."""
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

    strategy = resolve_scoring_strategy("IsolationForestV1")
    with pytest.raises(InvalidParameterError) as exc_info:
        strategy.score_segmented(mock_df, config, mock_registry, all_segments=[])

    assert "No segment models found for base model" in str(exc_info.value)
    assert model_name in str(exc_info.value)
    assert "Train segmented models first" in str(exc_info.value)


def test_extract_quantile_points_raises_when_score_quantiles_missing():
    """get_quantile_points_for_severity raises when record.training.score_quantiles is missing or empty."""
    record = create_autospec(AnomalyModelRecord, instance=True)
    record.identity = create_autospec(ModelIdentity, instance=True)
    record.identity.model_name = "catalog.schema.my_model"
    record.training = create_autospec(TrainingMetadata, instance=True)
    record.training.score_quantiles = None

    with pytest.raises(InvalidParameterError) as exc_info:
        get_quantile_points_for_severity(record)

    assert "missing score quantiles" in str(exc_info.value)
    assert "catalog.schema.my_model" in str(exc_info.value)
    assert "Retrain the model" in str(exc_info.value)


def test_extract_quantile_points_raises_when_score_quantiles_empty():
    """get_quantile_points_for_severity raises when record.training.score_quantiles is empty dict."""
    record = create_autospec(AnomalyModelRecord, instance=True)
    record.identity = create_autospec(ModelIdentity, instance=True)
    record.identity.model_name = "catalog.schema.other_model"
    record.training = create_autospec(TrainingMetadata, instance=True)
    record.training.score_quantiles = {}

    with pytest.raises(InvalidParameterError) as exc_info:
        get_quantile_points_for_severity(record)

    assert "missing score quantiles" in str(exc_info.value)
    assert "catalog.schema.other_model" in str(exc_info.value)


def test_apply_fn_raises_when_global_model_has_no_feature_metadata():
    """When registry returns a global model with feature_metadata None, apply_fn raises InvalidParameterError."""
    mock_spark = create_autospec(SparkSession, instance=True)
    mock_df = create_autospec(DataFrame, instance=True)
    mock_df.sparkSession = mock_spark
    mock_df.withColumn.return_value = mock_df
    mock_df.columns = ["a", "b"]
    mock_df.filter.return_value = mock_df

    registry_table = "catalog.schema.registry"
    model_name = "catalog.schema.my_model"
    columns = ["a", "b"]
    config_hash = compute_config_hash(columns, None)

    record = create_autospec(AnomalyModelRecord, instance=True)
    record.identity = create_autospec(ModelIdentity, instance=True)
    record.identity.model_name = model_name
    record.identity.model_uri = "models:/my_model/1"
    record.identity.algorithm = "IsolationForestV1"
    record.training = create_autospec(TrainingMetadata, instance=True)
    record.training.columns = columns
    record.training.training_time = datetime(2024, 1, 1)
    record.features = FeatureEngineering(feature_metadata=None)
    record.segmentation = create_autospec(SegmentationConfig, instance=True)
    record.segmentation.segment_by = None
    record.segmentation.config_hash = config_hash

    mock_registry = create_autospec(AnomalyModelRegistry, instance=True)
    mock_registry.get_active_model.return_value = record
    mock_registry.get_all_segment_models.return_value = []

    with patch.object(model_discovery, "AnomalyModelRegistry") as mock_cls_disc:
        with patch.object(scoring_orchestrator, "AnomalyModelRegistry") as mock_cls_orch:
            mock_cls_disc.return_value = mock_registry
            mock_cls_orch.return_value = mock_registry

            _, apply_fn, _ = has_no_row_anomalies(
                model_name=model_name,
                registry_table=registry_table,
            )
            with pytest.raises(InvalidParameterError) as exc_info:
                apply_fn(mock_df)

    assert "missing feature_metadata" in str(exc_info.value)
    assert model_name in str(exc_info.value)


def test_apply_fn_raises_when_segment_model_has_no_segment_by():
    """When segment fallback returns a segment with segment_by None, apply_fn raises InvalidParameterError."""
    mock_spark = create_autospec(SparkSession, instance=True)
    mock_df = create_autospec(DataFrame, instance=True)
    mock_df.sparkSession = mock_spark
    mock_df.withColumn.return_value = mock_df
    mock_df.columns = ["a", "b"]

    registry_table = "catalog.schema.registry"
    model_name = "catalog.schema.my_model"

    segment = create_autospec(AnomalyModelRecord, instance=True)
    segment.identity = create_autospec(ModelIdentity, instance=True)
    segment.identity.model_name = model_name
    segment.identity.algorithm = "IsolationForestV1"
    segment.training = create_autospec(TrainingMetadata, instance=True)
    segment.training.columns = ["a", "b"]
    segment.training.training_time = datetime.min
    segment.segmentation = create_autospec(SegmentationConfig, instance=True)
    segment.segmentation.segment_by = None

    mock_registry = create_autospec(AnomalyModelRegistry, instance=True)
    mock_registry.get_active_model.return_value = None
    mock_registry.get_all_segment_models.return_value = [segment]

    with patch.object(model_discovery, "AnomalyModelRegistry") as mock_cls_disc:
        with patch.object(scoring_orchestrator, "AnomalyModelRegistry") as mock_cls_orch:
            mock_cls_disc.return_value = mock_registry
            mock_cls_orch.return_value = mock_registry

            _, apply_fn, _ = has_no_row_anomalies(
                model_name=model_name,
                registry_table=registry_table,
            )
            with pytest.raises(InvalidParameterError) as exc_info:
                apply_fn(mock_df)

    assert "Segment model must have segment_by" in str(exc_info.value)
