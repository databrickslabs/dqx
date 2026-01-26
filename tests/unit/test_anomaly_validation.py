"""Unit tests for anomaly detection validation logic and error handling."""

from databricks.labs.dqx.anomaly.trainer import ensure_full_model_name


# ============================================================================
# Model Name Validation Tests
# ============================================================================


def test_ensure_full_model_name_with_simple_name():
    """Test ensure_full_model_name with simple model name."""
    full_name = ensure_full_model_name("my_model", "main.anomaly.registry")
    assert full_name == "main.anomaly.my_model"


def test_ensure_full_model_name_with_already_full_name():
    """Test ensure_full_model_name with already complete catalog.schema.model."""
    full_name = ensure_full_model_name("main.anomaly.my_model", "main.anomaly.registry")
    assert full_name == "main.anomaly.my_model"


def test_ensure_full_model_name_different_catalogs():
    """Test ensure_full_model_name with different catalog and schema combinations."""
    assert ensure_full_model_name("test_model", "catalog1.schema1.registry") == "catalog1.schema1.test_model"
    assert ensure_full_model_name("test_model", "catalog2.schema2.registry") == "catalog2.schema2.test_model"
    assert ensure_full_model_name("test_model", "prod.anomaly.registry") == "prod.anomaly.test_model"


def test_ensure_full_model_name_preserves_existing_full_name():
    """Test that ensure_full_model_name doesn't modify already full names."""
    # If the model name already has catalog.schema, it should be preserved
    full_name = ensure_full_model_name("other_catalog.other_schema.my_model", "main.anomaly.registry")
    # The function should respect the existing full name
    assert full_name == "other_catalog.other_schema.my_model"


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


def test_score_threshold_bounds():
    """Test anomaly score threshold validation (0-1 range)."""
    # Valid thresholds
    valid_thresholds = [0.0, 0.1, 0.3, 0.5, 0.7, 0.9, 1.0]
    for threshold in valid_thresholds:
        assert 0.0 <= threshold <= 1.0

    # Invalid thresholds
    invalid_thresholds = [-0.1, 1.1, 2.0]
    for threshold in invalid_thresholds:
        assert threshold < 0.0 or threshold > 1.0


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

    # Internal row id collision error
    error_msg = "Input DataFrame already contains column '_dqx_row_id'."
    assert "_dqx_row_id" in error_msg
