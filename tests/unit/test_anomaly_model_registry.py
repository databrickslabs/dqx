"""Unit tests for anomaly model registry functions."""

from datetime import datetime


from databricks.labs.dqx.anomaly.model_config import compute_config_hash
from databricks.labs.dqx.anomaly.model_registry import (
    AnomalyModelRecord,
    FeatureEngineering,
    ModelIdentity,
    GroupingConfig,
    TrainingMetadata,
)


# ============================================================================
# Config Hash Tests
# ============================================================================


def test_compute_config_hash_order_independent() -> None:
    """Columns and baseline columns are sets: listing order must not change the hash."""
    hash_a = compute_config_hash(["b", "a"], ["g2", "g1"])
    hash_b = compute_config_hash(["a", "b"], ["g1", "g2"])
    assert hash_a == hash_b


def test_compute_config_hash_handles_none_baseline_by() -> None:
    hash_a = compute_config_hash(["a", "b"], None)
    hash_b = compute_config_hash(["b", "a"], None)
    assert hash_a == hash_b


def test_compute_config_hash_distinguishes_baseline_by() -> None:
    """The gap this closed: the same name retrained with a different grouping used to hash alike.

    Collision detection exists to catch "same model_name, different configuration", and the grouping
    is part of the configuration -- it changes the feature list and the persisted baselines.
    """
    ungrouped = compute_config_hash(["a", "b"], None)
    grouped = compute_config_hash(["a", "b"], ["region"])
    grouped_wider = compute_config_hash(["a", "b"], ["region", "product"])

    assert ungrouped != grouped
    assert grouped != grouped_wider


def test_compute_config_hash_treats_empty_baseline_as_ungrouped() -> None:
    """``baseline_by=[]`` is how a caller asks for whole-table comparison, which is the ungrouped case."""
    assert compute_config_hash(["a"], []) == compute_config_hash(["a"], None)


def test_compute_config_hash_distinguishes_baseline_over_time() -> None:
    """A time column changes the engineered feature list, so it has to change the hash.

    Without this, retraining under one name with the time column added or removed keeps the old hash, and
    scoring then hands the pipeline a feature vector of a different width than it was fitted on. The
    mismatch is the mechanism that turns that into a raised error instead of a wrong number.
    """
    without_time = compute_config_hash(["a", "b"], None)
    with_time = compute_config_hash(["a", "b"], None, "event_ts")
    other_time = compute_config_hash(["a", "b"], None, "created_at")

    assert without_time != with_time
    # Which column was used matters too: two timestamps in one table describe different histories.
    assert with_time != other_time


def test_compute_config_hash_treats_an_empty_time_column_as_no_time_column() -> None:
    """Empty and absent must agree, because the persisted metadata stores "" where a caller passed None."""
    assert compute_config_hash(["a"], None, "") == compute_config_hash(["a"], None, None)


def test_compute_config_hash_combines_both_bases_independently() -> None:
    """Grouping and time compose, so each has to move the hash on its own and together."""
    plain = compute_config_hash(["a"], None, None)
    grouped = compute_config_hash(["a"], ["region"], None)
    timed = compute_config_hash(["a"], None, "event_ts")
    both = compute_config_hash(["a"], ["region"], "event_ts")

    assert len({plain, grouped, timed, both}) == 4


def test_compute_config_hash_pins_the_current_formula() -> None:
    """A committed reference value, because changing this formula forces every user to retrain.

    Adding *baseline_over_time* to the hashed payload moved every hash, including for configurations that
    do not use it: the key is present as null rather than absent. That is acceptable here and only here,
    because this same unreleased release already moved the hash when *baseline_by* joined it, so no
    published model carries the intermediate value. It would not be acceptable again.

    Pinning the value is what makes the next such change deliberate rather than incidental.
    """
    assert compute_config_hash(["amount", "quantity"], ["region"]) == "63d569c1b4ec7ec5"
    assert compute_config_hash(["amount", "quantity"], ["region"], "event_ts") == compute_config_hash(
        ["quantity", "amount"], ["region"], "event_ts"
    )


def test_compute_config_hash_different_columns_produce_different_hash() -> None:
    """Different column sets should produce different hashes."""
    hash_a = compute_config_hash(["col1", "col2"], None)
    hash_b = compute_config_hash(["col1", "col3"], None)
    assert hash_a != hash_b


def test_compute_config_hash_empty_columns() -> None:
    """Empty columns should still produce a hash."""
    hash_empty = compute_config_hash([], None)
    hash_with_cols = compute_config_hash(["col1"], None)
    assert hash_empty != hash_with_cols


def test_compute_config_hash_is_deterministic() -> None:
    """Same inputs should always produce same hash."""
    columns = ["amount", "quantity"]
    baseline_by = ["region"]

    hash_1 = compute_config_hash(columns, baseline_by)
    hash_2 = compute_config_hash(columns, baseline_by)
    hash_3 = compute_config_hash(columns, baseline_by)

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


def test_grouping_config_for_ungrouped_model() -> None:
    """GroupingConfig for a model with no baseline conditioning."""
    config = GroupingConfig()
    assert config.baseline_by is None


def test_grouping_config_for_conditioned_model() -> None:
    """GroupingConfig for a model conditioned on a baseline grouping."""
    config = GroupingConfig(baseline_by=["region", "product"])
    assert config.baseline_by == ["region", "product"]


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
        grouping=GroupingConfig(baseline_by=None),
    )

    assert record.identity.model_name == "catalog.schema.model"
    assert record.training.training_rows == 5000
    assert record.training.columns == ["amount", "quantity"]
    assert record.grouping.baseline_by is None


def test_grouping_recorded_on_the_model() -> None:
    """A conditioned model carries its baseline grouping; an unconditioned one carries None."""
    ungrouped = AnomalyModelRecord(
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
        features=FeatureEngineering(feature_metadata=None, feature_importance=None, temporal_config=None),
        grouping=GroupingConfig(baseline_by=None),
    )

    conditioned = AnomalyModelRecord(
        identity=ModelIdentity(
            model_name="model",
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
        features=FeatureEngineering(feature_metadata=None, feature_importance=None, temporal_config=None),
        grouping=GroupingConfig(baseline_by=["region"]),
    )

    assert ungrouped.grouping.baseline_by is None
    assert conditioned.grouping.baseline_by == ["region"]


# ============================================================================
# save_model SQL safety tests (archive path)
# ============================================================================


def _minimal_record(model_name: str = "catalog.schema.model") -> AnomalyModelRecord:
    """Minimal AnomalyModelRecord for save_model tests."""
    return AnomalyModelRecord(
        identity=ModelIdentity(
            model_name=model_name,
            model_uri="models:/catalog.schema.model/1",
            algorithm="IsolationForest",
            mlflow_run_id="run1",
            status="active",
        ),
        training=TrainingMetadata(
            columns=["col1"],
            hyperparameters={},
            training_rows=100,
            training_time=datetime.now(),
        ),
        features=FeatureEngineering(
            feature_metadata=None,
            feature_importance=None,
            temporal_config=None,
        ),
        grouping=GroupingConfig(baseline_by=None),
    )
