"""Unit tests for AnomalyProfile and profiler data structures."""

from databricks.labs.dqx.anomaly.profiler import AnomalyProfile


def test_anomaly_profile_creation_with_minimal_data():
    """Test AnomalyProfile creation with minimal required fields."""
    profile = AnomalyProfile(
        recommended_columns=["amount", "quantity"],
        recommended_segments=["region"],
        segment_count=3,
        column_stats={},
        warnings=[],
    )

    assert profile.recommended_columns == ["amount", "quantity"]
    assert profile.recommended_segments == ["region"]
    assert profile.segment_count == 3
    assert not profile.column_stats
    assert not profile.warnings
    assert profile.column_types is None  # Optional field
    assert profile.unsupported_columns is None  # Optional field


def test_anomaly_profile_with_column_stats():
    """Test AnomalyProfile with detailed column statistics."""
    column_stats = {
        "amount": {
            "mean": 150.0,
            "stddev": 25.0,
            "min": 50.0,
            "max": 300.0,
            "null_rate": 0.02,
            "distinct_count": 1000,
        },
        "quantity": {
            "mean": 10.0,
            "stddev": 2.5,
            "min": 1.0,
            "max": 20.0,
            "null_rate": 0.0,
            "distinct_count": 20,
        },
        "region": {
            "distinct_count": 3,
            "null_rate": 0.0,
            "top_values": ["US", "EU", "APAC"],
        },
    }

    profile = AnomalyProfile(
        recommended_columns=["amount", "quantity"],
        recommended_segments=["region"],
        segment_count=3,
        column_stats=column_stats,
        warnings=[],
    )

    assert len(profile.column_stats) == 3
    assert profile.column_stats["amount"]["mean"] == 150.0
    assert profile.column_stats["amount"]["stddev"] == 25.0
    assert profile.column_stats["quantity"]["distinct_count"] == 20
    assert profile.column_stats["region"]["top_values"] == ["US", "EU", "APAC"]


def test_anomaly_profile_with_warnings():
    """Test AnomalyProfile with multiple warnings."""
    warnings = [
        "Column 'id' excluded: likely identifier (name pattern match)",
        "Column 'timestamp' excluded: temporal column",
        "Segment 'product_id' has high cardinality (1000 values)",
        "Small segment warning: region=APAC has only 500 rows (recommended: 1000+)",
    ]

    profile = AnomalyProfile(
        recommended_columns=["amount", "quantity"],
        recommended_segments=["region"],
        segment_count=3,
        column_stats={},
        warnings=warnings,
    )

    assert len(profile.warnings) == 4
    assert "id" in profile.warnings[0]
    assert "timestamp" in profile.warnings[1]
    assert "product_id" in profile.warnings[2]
    assert "APAC" in profile.warnings[3]
    assert "500 rows" in profile.warnings[3]


def test_anomaly_profile_with_column_types():
    """Test AnomalyProfile with column type mappings."""
    column_types = {
        "amount": "numeric",
        "quantity": "numeric",
        "discount": "numeric",
        "region": "categorical",
        "is_premium": "boolean",
        "order_date": "datetime",
        "metadata": "unsupported",
    }

    profile = AnomalyProfile(
        recommended_columns=["amount", "quantity", "discount"],
        recommended_segments=["region"],
        segment_count=3,
        column_stats={},
        warnings=[],
        column_types=column_types,
    )

    assert profile.column_types is not None
    assert len(profile.column_types) == 7
    assert profile.column_types["amount"] == "numeric"
    assert profile.column_types["region"] == "categorical"
    assert profile.column_types["is_premium"] == "boolean"
    assert profile.column_types["order_date"] == "datetime"
    assert profile.column_types["metadata"] == "unsupported"


def test_anomaly_profile_with_unsupported_columns():
    """Test AnomalyProfile with unsupported column list."""
    unsupported_columns = [
        "nested_struct",
        "array_column",
        "map_column",
        "binary_data",
    ]

    profile = AnomalyProfile(
        recommended_columns=["amount", "quantity"],
        recommended_segments=["region"],
        segment_count=3,
        column_stats={},
        warnings=["Some columns are unsupported"],
        column_types={"amount": "numeric", "quantity": "numeric"},
        unsupported_columns=unsupported_columns,
    )

    assert profile.unsupported_columns is not None
    assert len(profile.unsupported_columns) == 4
    assert "nested_struct" in profile.unsupported_columns
    assert "array_column" in profile.unsupported_columns
    assert "map_column" in profile.unsupported_columns
    assert "binary_data" in profile.unsupported_columns


def test_anomaly_profile_no_recommended_segments():
    """Test AnomalyProfile when no segments are recommended (global model)."""
    profile = AnomalyProfile(
        recommended_columns=["amount", "quantity", "discount"],
        recommended_segments=[],  # Empty - global model
        segment_count=1,  # Single global model
        column_stats={
            "amount": {"mean": 150.0, "stddev": 25.0},
            "quantity": {"mean": 10.0, "stddev": 2.5},
            "discount": {"mean": 0.15, "stddev": 0.05},
        },
        warnings=["No suitable segmentation columns found"],
    )

    assert profile.recommended_columns == ["amount", "quantity", "discount"]
    assert not profile.recommended_segments
    assert profile.segment_count == 1
    assert len(profile.warnings) == 1
    assert "No suitable segmentation" in profile.warnings[0]


def test_anomaly_profile_empty_recommendations():
    """Test AnomalyProfile when no columns or segments are suitable."""
    profile = AnomalyProfile(
        recommended_columns=[],
        recommended_segments=[],
        segment_count=0,
        column_stats={},
        warnings=[
            "No numeric columns with sufficient variance found",
            "All categorical columns have too high cardinality",
        ],
        column_types={},
        unsupported_columns=["col1", "col2", "col3"],
    )

    assert not profile.recommended_columns
    assert not profile.recommended_segments
    assert profile.segment_count == 0
    assert len(profile.warnings) == 2
    assert len(profile.unsupported_columns) == 3


def test_anomaly_profile_multi_segment_configuration():
    """Test AnomalyProfile with multiple segmentation columns."""
    profile = AnomalyProfile(
        recommended_columns=["amount", "quantity", "discount", "weight"],
        recommended_segments=["region", "product_category"],  # Two segment columns
        segment_count=12,  # 3 regions × 4 categories
        column_stats={
            "amount": {"mean": 150.0, "stddev": 25.0},
            "region": {"distinct_count": 3},
            "product_category": {"distinct_count": 4},
        },
        warnings=[],
        column_types={
            "amount": "numeric",
            "quantity": "numeric",
            "discount": "numeric",
            "weight": "numeric",
            "region": "categorical",
            "product_category": "categorical",
        },
    )

    assert len(profile.recommended_columns) == 4
    assert len(profile.recommended_segments) == 2
    assert profile.segment_count == 12
    assert profile.column_types["region"] == "categorical"
    assert profile.column_types["product_category"] == "categorical"


def test_anomaly_profile_column_stats_with_edge_cases():
    """Test AnomalyProfile with edge case statistics."""
    column_stats = {
        "zero_variance": {
            "mean": 100.0,
            "stddev": 0.0,  # Zero variance - should be excluded
            "min": 100.0,
            "max": 100.0,
        },
        "high_nulls": {
            "mean": 50.0,
            "stddev": 10.0,
            "null_rate": 0.75,  # High null rate - should be excluded
        },
        "good_column": {
            "mean": 150.0,
            "stddev": 25.0,
            "null_rate": 0.02,
        },
    }

    profile = AnomalyProfile(
        recommended_columns=["good_column"],  # Only the good one
        recommended_segments=[],
        segment_count=1,
        column_stats=column_stats,
        warnings=[
            "Column 'zero_variance' excluded: stddev = 0",
            "Column 'high_nulls' excluded: null_rate = 0.75",
        ],
    )

    assert profile.recommended_columns == ["good_column"]
    assert len(profile.column_stats) == 3
    assert profile.column_stats["zero_variance"]["stddev"] == 0.0
    assert profile.column_stats["high_nulls"]["null_rate"] == 0.75
    assert len(profile.warnings) == 2


def test_anomaly_profile_preserves_column_order():
    """Test that AnomalyProfile preserves column order."""
    columns = ["col_z", "col_a", "col_m", "col_b"]

    profile = AnomalyProfile(
        recommended_columns=columns,
        recommended_segments=[],
        segment_count=1,
        column_stats={},
        warnings=[],
    )

    # Order should be preserved
    assert profile.recommended_columns == ["col_z", "col_a", "col_m", "col_b"]
    assert profile.recommended_columns[0] == "col_z"
    assert profile.recommended_columns[3] == "col_b"
