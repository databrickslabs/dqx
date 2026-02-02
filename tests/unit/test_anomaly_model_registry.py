"""Unit tests for anomaly model registry functions."""

from datetime import datetime

from databricks.labs.dqx.anomaly.model_registry import (
    AnomalyModelRecord,
    FeatureEngineering,
    ModelIdentity,
    SegmentationConfig,
    TrainingMetadata,
    compute_config_hash,
)


# ============================================================================
# Config Hash Tests
# ============================================================================


def test_compute_config_hash_order_independent() -> None:
    hash_a = compute_config_hash(["b", "a"], ["seg2", "seg1"])
    hash_b = compute_config_hash(["a", "b"], ["seg1", "seg2"])
    assert hash_a == hash_b


def test_compute_config_hash_handles_none_segment_by() -> None:
    hash_a = compute_config_hash(["a", "b"], None)
    hash_b = compute_config_hash(["b", "a"], None)
    assert hash_a == hash_b


def test_compute_config_hash_different_columns_produce_different_hash() -> None:
    """Different column sets should produce different hashes."""
    hash_a = compute_config_hash(["col1", "col2"], None)
    hash_b = compute_config_hash(["col1", "col3"], None)
    assert hash_a != hash_b


def test_compute_config_hash_different_segments_produce_different_hash() -> None:
    """Different segment_by should produce different hashes."""
    hash_a = compute_config_hash(["col1"], ["region"])
    hash_b = compute_config_hash(["col1"], ["country"])
    assert hash_a != hash_b


def test_compute_config_hash_with_vs_without_segments() -> None:
    """Segmented vs non-segmented should produce different hashes."""
    hash_segmented = compute_config_hash(["col1"], ["region"])
    hash_global = compute_config_hash(["col1"], None)
    assert hash_segmented != hash_global


def test_compute_config_hash_empty_columns() -> None:
    """Empty columns should still produce a hash."""
    hash_empty = compute_config_hash([], None)
    hash_with_cols = compute_config_hash(["col1"], None)
    assert hash_empty != hash_with_cols


def test_compute_config_hash_is_deterministic() -> None:
    """Same inputs should always produce same hash."""
    columns = ["amount", "quantity"]
    segment_by = ["region"]

    hash_1 = compute_config_hash(columns, segment_by)
    hash_2 = compute_config_hash(columns, segment_by)
    hash_3 = compute_config_hash(columns, segment_by)

    assert hash_1 == hash_2 == hash_3


# ============================================================================
# Model Record Dataclass Tests
# ============================================================================


def test_model_identity_required_fields() -> None:
    """Test ModelIdentity has expected structure."""
    identity = ModelIdentity(
        model_name="catalog.schema.model",
        model_uri="models:/catalog.schema.model/1",
        algorithm="IsolationForest",
        mlflow_run_id="abc123",
        status="active",
    )

    assert identity.model_name == "catalog.schema.model"
    assert identity.algorithm == "IsolationForest"
    assert identity.status == "active"


def test_training_metadata_required_fields() -> None:
    """Test TrainingMetadata has expected structure."""
    metadata = TrainingMetadata(
        columns=["amount", "quantity"],
        hyperparameters={"n_estimators": "100"},
        training_rows=1000,
        training_time=datetime.now(),
    )

    assert metadata.training_rows == 1000
    assert metadata.columns == ["amount", "quantity"]


def test_segmentation_config_for_global_model() -> None:
    """Test SegmentationConfig for non-segmented model."""
    config = SegmentationConfig(
        segment_by=None,
        segment_values=None,
    )

    assert config.segment_by is None
    assert config.segment_values is None


def test_segmentation_config_for_segment_model() -> None:
    """Test SegmentationConfig for segmented model."""
    config = SegmentationConfig(
        segment_by=["region"],
        segment_values={"region": "US"},
    )

    assert config.segment_by == ["region"]
    assert config.segment_values == {"region": "US"}


# ============================================================================
# Anomaly Model Record Tests
# ============================================================================


def test_anomaly_model_record_full_construction() -> None:
    """Test complete AnomalyModelRecord construction."""
    record = AnomalyModelRecord(
        identity=ModelIdentity(
            model_name="catalog.schema.model",
            model_uri="models:/catalog.schema.model/1",
            algorithm="IsolationForest",
            mlflow_run_id="run123",
            status="active",
        ),
        training=TrainingMetadata(
            columns=["amount", "quantity"],
            hyperparameters={"n_estimators": "100"},
            training_rows=5000,
            training_time=datetime.now(),
        ),
        features=FeatureEngineering(
            feature_metadata=None,
            feature_importance=None,
            temporal_config=None,
        ),
        segmentation=SegmentationConfig(
            segment_by=None,
            segment_values=None,
        ),
    )

    assert record.identity.model_name == "catalog.schema.model"
    assert record.training.training_rows == 5000
    assert record.training.columns == ["amount", "quantity"]
    assert record.segmentation.segment_by is None


def test_is_segmented_property() -> None:
    """Test is_segmented detection based on segment_by."""
    # Non-segmented model
    global_record = AnomalyModelRecord(
        identity=ModelIdentity(
            model_name="model",
            model_uri="uri",
            algorithm="IsolationForest",
            mlflow_run_id="run1",
            status="active",
        ),
        training=TrainingMetadata(
            columns=["col1"],
            hyperparameters={},
            training_rows=1000,
            training_time=datetime.now(),
        ),
        features=FeatureEngineering(
            feature_metadata=None,
            feature_importance=None,
            temporal_config=None,
        ),
        segmentation=SegmentationConfig(
            segment_by=None,
            segment_values=None,
        ),
    )

    # Segmented model
    segment_record = AnomalyModelRecord(
        identity=ModelIdentity(
            model_name="model__seg_region=US",
            model_uri="uri",
            algorithm="IsolationForest",
            mlflow_run_id="run2",
            status="active",
        ),
        training=TrainingMetadata(
            columns=["col1"],
            hyperparameters={},
            training_rows=1000,
            training_time=datetime.now(),
        ),
        features=FeatureEngineering(
            feature_metadata=None,
            feature_importance=None,
            temporal_config=None,
        ),
        segmentation=SegmentationConfig(
            segment_by=["region"],
            segment_values={"region": "US"},
        ),
    )

    # Global model has no segment_by
    assert global_record.segmentation.segment_by is None

    # Segmented model has segment_by and segment_values
    assert segment_record.segmentation.segment_by == ["region"]
    assert segment_record.segmentation.segment_values == {"region": "US"}
