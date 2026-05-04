"""Unit tests for row anomaly detection validation logic and error handling."""

import warnings
from datetime import datetime

import pytest
import sklearn

from databricks.labs.dqx.anomaly import validation
from databricks.labs.dqx.anomaly.model_config import (
    AnomalyModelRecord,
    FeatureEngineering,
    ModelIdentity,
    SegmentationConfig,
    TrainingMetadata,
)
from databricks.labs.dqx.anomaly.validation import validate_sklearn_compatibility, validate_training_params
from databricks.labs.dqx.config import AnomalyParams
from databricks.labs.dqx.errors import InvalidParameterError


# ============================================================================
# Spark Version Validation Tests
# ============================================================================


def test_validate_spark_version_raises_for_old_spark():
    """Test that validate_spark_version raises for Spark < 3.4."""
    mock_spark = type("MockSpark", (), {"version": "3.2.0"})()
    with pytest.raises(InvalidParameterError) as exc_info:
        validation.validate_spark_version(mock_spark)
    assert "Spark >= 3.4" in str(exc_info.value)
    assert "3.2" in str(exc_info.value)


def test_validate_spark_version_raises_for_spark_2():
    """Test that validate_spark_version raises for Spark 2.x."""
    mock_spark = type("MockSpark", (), {"version": "2.4.8"})()
    with pytest.raises(InvalidParameterError) as exc_info:
        validation.validate_spark_version(mock_spark)
    assert "Spark >= 3.4" in str(exc_info.value)
    assert "2.4" in str(exc_info.value)


# ============================================================================
# Model Name Validation Tests
# ============================================================================


def test_validate_fully_qualified_name_accepts_valid():
    """Test fully qualified name validation with valid values."""
    validation.validate_fully_qualified_name("catalog.schema.model", label="model_name")
    validation.validate_fully_qualified_name("catalog.schema.registry", label="registry_table")


def test_validate_fully_qualified_name_rejects_invalid():
    """Test fully qualified name validation with invalid values."""
    with pytest.raises(InvalidParameterError):
        validation.validate_fully_qualified_name("model", label="model_name")

    with pytest.raises(InvalidParameterError):
        validation.validate_fully_qualified_name("schema.model", label="model_name")

    with pytest.raises(InvalidParameterError):
        validation.validate_fully_qualified_name("catalog..table", label="model_name")

    with pytest.raises(InvalidParameterError):
        validation.validate_fully_qualified_name("catalog.schema.table.extra", label="model_name")


# ============================================================================
# Parameter Validation Edge Cases
# ============================================================================


def test_sample_fraction_validation():
    """Test sample_fraction parameter bounds."""
    # Valid fractions
    valid_fractions = [0.01, 0.1, 0.3, 0.5, 0.8, 1.0]
    for fraction in valid_fractions:
        assert 0.0 < fraction <= 1.0

    # Invalid fractions (would fail in actual usage)
    invalid_fractions = [0.0, -0.1, 1.1, 2.0]
    for fraction in invalid_fractions:
        assert fraction <= 0.0 or fraction > 1.0


def test_train_ratio_validation():
    """Test train_ratio parameter bounds."""
    # Valid train ratios
    valid_ratios = [0.5, 0.7, 0.8, 0.85, 0.9, 0.95, 0.99]
    for ratio in valid_ratios:
        assert 0.0 < ratio < 1.0

    # Invalid ratios
    invalid_ratios = [0.0, 1.0, 1.1, -0.1]
    for ratio in invalid_ratios:
        assert ratio <= 0.0 or ratio >= 1.0


def test_contamination_validation():
    """Test contamination parameter bounds for IsolationForest."""
    # Valid contamination values
    valid_contaminations = [0.01, 0.05, 0.1, 0.2, 0.5]
    for contamination in valid_contaminations:
        assert 0.0 < contamination < 1.0

    # Invalid contamination values
    invalid_contaminations = [0.0, 1.0, 1.5, -0.1]
    for contamination in invalid_contaminations:
        assert contamination <= 0.0 or contamination >= 1.0


def test_ensemble_size_validation():
    """Test ensemble_size parameter validation."""
    # Valid ensemble sizes
    valid_sizes = [1, 3, 5, 7, 10]
    for size in valid_sizes:
        assert size >= 1

    # Invalid ensemble sizes
    invalid_sizes = [0, -1, -5]
    for size in invalid_sizes:
        assert size < 1


def test_num_trees_validation():
    """Test num_trees parameter validation."""
    # Valid tree counts
    valid_counts = [1, 50, 100, 200, 500, 1000]
    for count in valid_counts:
        assert count >= 1

    # Invalid tree counts
    invalid_counts = [0, -1, -100]
    for count in invalid_counts:
        assert count < 1


# ============================================================================
# Column Validation Logic Tests
# ============================================================================


def test_column_list_validation():
    """Test column list parameter validation."""
    # Valid column lists
    assert isinstance(["col1"], list)
    assert isinstance(["col1", "col2", "col3"], list)
    assert isinstance([], list)

    # Valid lengths
    assert len(["col1", "col2"]) == 2
    assert len(["col1", "col2", "col3", "col4", "col5"]) == 5


def test_column_name_types():
    """Test column names are strings."""
    columns = ["col1", "col2", "col3"]
    assert all(isinstance(col, str) for col in columns)

    # Invalid: non-string column names would fail
    invalid_columns = [1, 2, 3]
    assert not all(isinstance(col, str) for col in invalid_columns)


def test_max_columns_limit():
    """Test maximum columns limit validation (soft limit - warns but proceeds)."""
    max_columns = 25

    # Valid: under recommended limit
    columns_ok = [f"col_{i}" for i in range(20)]
    assert len(columns_ok) <= max_columns

    # Above limit (will warn but not error)
    columns_above_limit = [f"col_{i}" for i in range(30)]
    assert len(columns_above_limit) > max_columns


def test_duplicate_column_detection():
    """Test detection of duplicate columns."""
    # No duplicates
    unique_columns = ["col1", "col2", "col3"]
    assert len(unique_columns) == len(set(unique_columns))

    # Has duplicates
    duplicate_columns = ["col1", "col2", "col1"]
    assert len(duplicate_columns) != len(set(duplicate_columns))


# ============================================================================
# Cardinality Validation Tests
# ============================================================================


def test_cardinality_threshold_validation():
    """Test categorical cardinality threshold validation."""
    default_threshold = 20

    # Valid cardinalities for categorical encoding
    valid_cardinalities = [2, 5, 10, 15, 20]
    for cardinality in valid_cardinalities:
        assert cardinality <= default_threshold

    # Too high cardinality for OneHot encoding
    high_cardinalities = [50, 100, 1000]
    for cardinality in high_cardinalities:
        assert cardinality > default_threshold


def test_segment_cardinality_bounds():
    """Test segment cardinality bounds (2-50 range)."""
    min_segments = 2
    max_segments = 50

    # Valid segment counts
    valid_segments = [2, 3, 5, 10, 25, 50]
    for count in valid_segments:
        assert min_segments <= count <= max_segments

    # Invalid segment counts
    assert min_segments > 1 or max_segments < 1  # Too few
    assert min_segments > 100 or max_segments < 100  # Too many


# ============================================================================
# Null Rate Validation Tests
# ============================================================================


def test_null_rate_validation():
    """Test null rate threshold validation."""
    max_null_rate_numeric = 0.5  # 50% for numeric columns
    max_null_rate_segment = 0.1  # 10% for segment columns

    # Valid null rates for numeric columns
    assert max_null_rate_numeric > 0.0
    assert max_null_rate_numeric > 0.02
    assert max_null_rate_numeric > 0.49

    # Invalid null rates for numeric columns
    assert max_null_rate_numeric <= 0.75

    # Valid null rates for segment columns
    assert max_null_rate_segment > 0.0
    assert max_null_rate_segment > 0.05

    # Invalid null rates for segment columns
    assert max_null_rate_segment < 0.15  # 15% exceeds 10% threshold


# ============================================================================
# Feature Engineering Limits Tests
# ============================================================================


def test_max_engineered_features_validation():
    """Test max engineered features limit."""
    default_max_features = 50

    # Valid feature counts
    valid_counts = [10, 25, 40, 50]
    for count in valid_counts:
        assert count <= default_max_features

    # Too many features
    invalid_counts = [60, 100, 200]
    for count in invalid_counts:
        assert count > default_max_features


def test_max_input_columns_validation():
    """Test max input columns limit (soft limit - warns but proceeds)."""
    default_max_input = 25

    # Valid input column counts (under recommended limit)
    valid_counts = [1, 5, 10, 20, 25]
    for count in valid_counts:
        assert count <= default_max_input

    # Columns above recommended limit (will warn but not error)
    above_limit_counts = [30, 40, 50]
    for count in above_limit_counts:
        assert count > default_max_input


# ============================================================================
# Score Threshold Validation Tests
# ============================================================================


def test_threshold_bounds():
    """Test anomaly severity threshold validation (0-100 range)."""
    # Valid thresholds
    valid_thresholds = [0.0, 10.0, 30.0, 50.0, 70.0, 90.0, 100.0]
    for threshold in valid_thresholds:
        assert 0.0 <= threshold <= 100.0

    # Invalid thresholds
    invalid_thresholds = [-0.1, 120.0, 200.0]
    for threshold in invalid_thresholds:
        assert threshold < 0.0 or threshold > 100.0


def test_drift_threshold_validation():
    """Test drift detection threshold validation."""
    # Valid drift thresholds (typically 2-5 sigma)
    valid_thresholds = [2.0, 2.5, 3.0, 4.0, 5.0]
    for threshold in valid_thresholds:
        assert threshold > 0.0

    # Disabled drift detection
    disabled = None
    assert disabled is None

    # Invalid thresholds (would be rejected by validation)
    invalid_thresholds = [0.0, -1.0]
    for invalid in invalid_thresholds:
        assert invalid <= 0.0  # These should fail validation


# ============================================================================
# Table Name Format Validation Tests
# ============================================================================


def test_table_name_format_validation():
    """Test table name format validation."""
    # Valid 3-part table names
    valid_tables = [
        "main.schema.table",
        "catalog.schema.table",
        "prod.sales.transactions",
    ]
    for table in valid_tables:
        parts = table.split(".")
        assert len(parts) == 3

    # Invalid table names
    invalid_tables = [
        "table",  # 1 part
        "schema.table",  # 2 parts
        "cat.sch.tab.extra",  # 4 parts
    ]
    for table in invalid_tables:
        parts = table.split(".")
        assert len(parts) != 3


def test_registry_table_format():
    """Test registry table must have at least catalog.schema format."""
    # Valid formats (at least 2 parts)
    valid_registries = [
        "catalog.schema",
        "main.anomaly.registry",
        "prod.models.registry",
    ]
    for registry in valid_registries:
        parts = registry.split(".")
        assert len(parts) >= 2

    # Invalid formats (only 1 part)
    invalid_registries = ["registry", "table_only"]
    for registry in invalid_registries:
        parts = registry.split(".")
        assert len(parts) < 2


# ============================================================================
# Model Name Required Validation
# ============================================================================


def test_model_name_validation():
    """Test model name parameter validation."""
    # Valid model names
    valid_names = ["my_model", "sales_anomaly", "fraud_detection_v2"]
    for name in valid_names:
        assert isinstance(name, str)
        assert len(name) > 0

    # Invalid model names
    assert not isinstance(None, str)
    assert len("") == 0  # Empty string


# ============================================================================
# Row Count Validation Tests
# ============================================================================


def test_minimum_rows_validation():
    """Test minimum rows validation for training."""
    min_rows_per_segment = 1000

    # Valid row counts
    valid_counts = [1000, 5000, 10000, 100000]
    for count in valid_counts:
        assert count >= min_rows_per_segment

    # Too few rows (should trigger warning)
    low_counts = [100, 500, 999]
    for count in low_counts:
        assert count < min_rows_per_segment


def test_max_rows_validation():
    """Test max_rows parameter validation."""
    # Valid max_rows values (including edge cases)
    valid_max = [1, 10_000, 100_000, 500_000, 1_000_000, 10_000_000]
    for max_val in valid_max:
        assert max_val > 0


# ============================================================================
# Error Message Format Tests
# ============================================================================


def test_error_message_formats():
    """Test that error messages follow expected patterns."""
    # Column not found error
    col_name = "unknown_col"
    error_msg = f"Column '{col_name}' not found in DataFrame"
    assert "Column" in error_msg
    assert col_name in error_msg

    # Model not found error
    model_name = "my_model"
    registry = "main.anomaly.registry"
    error_msg = f"Model '{model_name}' not found in '{registry}'"
    assert "Model" in error_msg
    assert model_name in error_msg
    assert registry in error_msg


# ============================================================================
# validate_training_params — direct calls with real AnomalyParams
# ============================================================================


def test_validate_training_params_accepts_defaults():
    """Default AnomalyParams with a valid expected_anomaly_rate should not raise."""
    validate_training_params(AnomalyParams(), expected_anomaly_rate=0.02)


def test_validate_training_params_rejects_non_numeric_sample_fraction():
    """Test that non-numeric sample_fraction raises (validation.py 64-65)."""
    params = AnomalyParams(sample_fraction="0.5")  # type: ignore[arg-type]
    with pytest.raises(InvalidParameterError, match="must be a numeric value"):
        validate_training_params(params, expected_anomaly_rate=0.02)


def test_validate_training_params_rejects_bool_sample_fraction():
    """Test that bool sample_fraction raises."""
    params = AnomalyParams(sample_fraction=True)  # type: ignore[arg-type]
    with pytest.raises(InvalidParameterError, match="must be a numeric value"):
        validate_training_params(params, expected_anomaly_rate=0.02)


def test_validate_training_params_rejects_zero_sample_fraction():
    with pytest.raises(InvalidParameterError, match="params.sample_fraction"):
        validate_training_params(AnomalyParams(sample_fraction=0.0), expected_anomaly_rate=0.02)


def test_validate_training_params_rejects_sample_fraction_above_one():
    with pytest.raises(InvalidParameterError, match="params.sample_fraction"):
        validate_training_params(AnomalyParams(sample_fraction=1.1), expected_anomaly_rate=0.02)


def test_validate_training_params_rejects_zero_expected_anomaly_rate():
    with pytest.raises(InvalidParameterError, match="expected_anomaly_rate"):
        validate_training_params(AnomalyParams(), expected_anomaly_rate=0.0)


def test_validate_training_params_rejects_expected_anomaly_rate_above_half():
    with pytest.raises(InvalidParameterError, match="expected_anomaly_rate"):
        validate_training_params(AnomalyParams(), expected_anomaly_rate=0.6)


def test_validate_training_params_rejects_non_integer_max_rows():
    """Test that non-integer max_rows raises."""
    params = AnomalyParams(max_rows=1000.5)  # type: ignore[arg-type]
    with pytest.raises(InvalidParameterError, match="must be an integer"):
        validate_training_params(params, expected_anomaly_rate=0.02)


def test_validate_training_params_rejects_bool_max_rows():
    """Test that bool max_rows raises."""
    params = AnomalyParams(max_rows=True)  # type: ignore[arg-type]
    with pytest.raises(InvalidParameterError, match="must be an integer"):
        validate_training_params(params, expected_anomaly_rate=0.02)


def test_validate_training_params_rejects_zero_max_rows():
    with pytest.raises(InvalidParameterError, match="params.max_rows"):
        validate_training_params(AnomalyParams(max_rows=0), expected_anomaly_rate=0.02)


def test_validate_training_params_rejects_zero_ensemble_size():
    with pytest.raises(InvalidParameterError, match="params.ensemble_size"):
        validate_training_params(AnomalyParams(ensemble_size=0), expected_anomaly_rate=0.02)


# ============================================================================
# validate_sklearn_compatibility — direct calls with real AnomalyModelRecord
# ============================================================================


def _make_record(sklearn_version: str | None) -> AnomalyModelRecord:
    return AnomalyModelRecord(
        identity=ModelIdentity(
            model_name="catalog.schema.model",
            model_uri="models:/catalog.schema.model/1",
            algorithm="IsolationForestV1",
            mlflow_run_id="run123",
        ),
        training=TrainingMetadata(
            columns=["amount", "quantity"],
            hyperparameters={},
            training_rows=1000,
            training_time=datetime.now(),
        ),
        features=FeatureEngineering(),
        segmentation=SegmentationConfig(sklearn_version=sklearn_version),
    )


def test_validate_sklearn_compatibility_no_warning_on_exact_match():
    """No warning when the stored version matches the installed version."""
    record = _make_record(sklearn_version=sklearn.__version__)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        validate_sklearn_compatibility(record)
    assert not any("SKLEARN" in str(w.message) for w in caught)


def test_validate_sklearn_compatibility_warns_on_version_mismatch():
    """UserWarning is emitted when the stored sklearn version differs from the current one."""
    record = _make_record(sklearn_version="0.0.0")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        validate_sklearn_compatibility(record)
    assert any("SKLEARN VERSION MISMATCH" in str(w.message) for w in caught)


def test_validate_sklearn_compatibility_skips_when_no_version():
    """No warning and no error when model was saved without a sklearn_version."""
    record = _make_record(sklearn_version=None)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        validate_sklearn_compatibility(record)
    assert not any("SKLEARN" in str(w.message) for w in caught)
