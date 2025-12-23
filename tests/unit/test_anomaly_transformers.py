"""Unit tests for feature engineering data structures and metadata."""

import json

from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeInfo,
    SparkFeatureMetadata,
    reconstruct_column_infos,
)

from tests.unit.test_anomaly_test_helpers import STANDARD_REGION_PRODUCT_FEATURES


# ============================================================================
# Column Type Info Tests
# ============================================================================


def test_column_type_info_creation():
    """Test ColumnTypeInfo dataclass creation."""
    col_info = ColumnTypeInfo(
        name="amount",
        spark_type=T.DoubleType(),
        category="numeric",
        cardinality=None,
        null_count=10,
    )

    assert col_info.name == "amount"
    assert isinstance(col_info.spark_type, T.DoubleType)
    assert col_info.category == "numeric"
    assert col_info.cardinality is None
    assert col_info.null_count == 10
    assert col_info.encoding_strategy is None  # Default


def test_column_type_info_with_encoding_strategy():
    """Test ColumnTypeInfo with encoding strategy."""
    col_info = ColumnTypeInfo(
        name="region",
        spark_type=T.StringType(),
        category="categorical",
        cardinality=5,
        null_count=0,
        encoding_strategy="onehot",
    )

    assert col_info.name == "region"
    assert col_info.category == "categorical"
    assert col_info.cardinality == 5
    assert col_info.encoding_strategy == "onehot"


def test_column_type_info_categories():
    """Test ColumnTypeInfo with different category types."""
    categories = ["numeric", "categorical", "datetime", "boolean", "unsupported"]

    for category in categories:
        col_info = ColumnTypeInfo(
            name=f"{category}_col",
            spark_type=T.StringType(),
            category=category,
        )
        assert col_info.category == category


def test_column_type_info_encoding_strategies():
    """Test ColumnTypeInfo with different encoding strategies."""
    strategies = ["onehot", "frequency", "cyclical", "binary", "none"]

    for strategy in strategies:
        col_info = ColumnTypeInfo(
            name="col",
            spark_type=T.StringType(),
            category="categorical",
            encoding_strategy=strategy,
        )
        assert col_info.encoding_strategy == strategy


def test_spark_feature_metadata_creation():
    """Test SparkFeatureMetadata creation."""
    column_infos = [
        {"name": "amount", "category": "numeric", "cardinality": None, "null_count": 0},
        {"name": "region", "category": "categorical", "cardinality": 3, "null_count": 0},
    ]

    metadata = SparkFeatureMetadata(
        column_infos=column_infos,
        categorical_frequency_maps={"region": {"US": 0.5, "EU": 0.3, "APAC": 0.2}},
        onehot_categories={"region": ["US", "EU", "APAC"]},
        engineered_feature_names=["amount_scaled", "region_US", "region_EU", "region_APAC"],
    )

    assert len(metadata.column_infos) == 2
    assert metadata.column_infos[0]["name"] == "amount"
    assert metadata.column_infos[1]["name"] == "region"
    assert "US" in metadata.categorical_frequency_maps["region"]
    assert metadata.categorical_frequency_maps["region"]["US"] == 0.5
    assert len(metadata.onehot_categories["region"]) == 3
    assert len(metadata.engineered_feature_names) == 4


def test_spark_feature_metadata_json_serialization():
    """Test SparkFeatureMetadata JSON serialization."""
    column_infos = [
        {"name": "amount", "category": "numeric"},
        {"name": "quantity", "category": "numeric"},
    ]

    metadata = SparkFeatureMetadata(
        column_infos=column_infos,
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["amount_scaled", "quantity_scaled"],
    )

    json_str = metadata.to_json()

    assert isinstance(json_str, str)
    assert "amount" in json_str
    assert "quantity" in json_str
    assert "engineered_feature_names" in json_str
    assert "categorical_frequency_maps" in json_str

    # Verify it's valid JSON
    parsed = json.loads(json_str)
    assert "column_infos" in parsed
    assert "engineered_feature_names" in parsed


def test_spark_feature_metadata_json_deserialization():
    """Test SparkFeatureMetadata JSON deserialization."""
    original_metadata = SparkFeatureMetadata(
        column_infos=[
            {"name": "amount", "category": "numeric", "cardinality": None},
            {"name": "region", "category": "categorical", "cardinality": 3},
        ],
        categorical_frequency_maps={"region": {"US": 0.6, "EU": 0.4}},
        onehot_categories={"region": ["US", "EU"]},
        engineered_feature_names=["amount_scaled", "region_US", "region_EU"],
    )

    json_str = original_metadata.to_json()
    restored_metadata = SparkFeatureMetadata.from_json(json_str)

    assert len(restored_metadata.column_infos) == 2
    assert restored_metadata.column_infos[0]["name"] == "amount"
    assert restored_metadata.column_infos[1]["name"] == "region"
    assert restored_metadata.categorical_frequency_maps["region"]["US"] == 0.6
    assert len(restored_metadata.onehot_categories["region"]) == 2
    assert len(restored_metadata.engineered_feature_names) == 3


def test_spark_feature_metadata_roundtrip():
    """Test full roundtrip: create → serialize → deserialize."""
    original = SparkFeatureMetadata(
        column_infos=[
            {"name": "col1", "category": "numeric", "null_count": 5},
            {"name": "col2", "category": "categorical", "cardinality": 10},
            {"name": "col3", "category": "boolean"},
        ],
        categorical_frequency_maps={
            "col2": {"A": 0.3, "B": 0.25, "C": 0.2, "D": 0.15, "E": 0.1},
        },
        onehot_categories={"col2": ["A", "B", "C", "D", "E"]},
        engineered_feature_names=["col1_scaled", "col2_A", "col2_B", "col2_C", "col2_D", "col2_E", "col3_binary"],
    )

    # Serialize and deserialize
    json_str = original.to_json()
    restored = SparkFeatureMetadata.from_json(json_str)

    # Verify all fields match
    assert restored.column_infos == original.column_infos
    assert restored.categorical_frequency_maps == original.categorical_frequency_maps
    assert restored.onehot_categories == original.onehot_categories
    assert restored.engineered_feature_names == original.engineered_feature_names


def test_spark_feature_metadata_backwards_compatibility():
    """Test backwards compatibility with old models without onehot_categories."""
    # Simulate old model metadata (before onehot_categories was added)
    old_json = json.dumps(
        {
            "column_infos": [
                {"name": "amount", "category": "numeric"},
            ],
            "categorical_frequency_maps": {"region": {"US": 0.5, "EU": 0.5}},
            "engineered_feature_names": ["amount_scaled", "region_freq"],
            # Note: no onehot_categories field
        }
    )

    # Should handle missing onehot_categories gracefully
    metadata = SparkFeatureMetadata.from_json(old_json)

    assert len(metadata.column_infos) == 1
    assert len(metadata.categorical_frequency_maps) == 1
    assert not metadata.onehot_categories  # Should default to empty dict
    assert len(metadata.engineered_feature_names) == 2


def test_reconstruct_column_infos():
    """Test reconstruct_column_infos helper function."""
    metadata = SparkFeatureMetadata(
        column_infos=[
            {"name": "amount", "category": "numeric", "cardinality": None, "null_count": 5},
            {"name": "region", "category": "categorical", "cardinality": 3, "null_count": 0},
            {"name": "is_premium", "category": "boolean", "cardinality": 2, "null_count": 0},
        ],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
    )

    column_infos = reconstruct_column_infos(metadata)

    assert len(column_infos) == 3
    assert all(isinstance(info, ColumnTypeInfo) for info in column_infos)
    assert column_infos[0].name == "amount"
    assert column_infos[0].category == "numeric"
    assert column_infos[0].cardinality is None
    assert column_infos[0].null_count == 5
    assert column_infos[1].name == "region"
    assert column_infos[1].category == "categorical"
    assert column_infos[1].cardinality == 3
    assert column_infos[2].name == "is_premium"
    assert column_infos[2].category == "boolean"


def test_reconstruct_column_infos_handles_optional_fields():
    """Test reconstruct_column_infos with missing optional fields."""
    metadata = SparkFeatureMetadata(
        column_infos=[
            {"name": "col1", "category": "numeric"},  # Missing cardinality and null_count
            {"name": "col2", "category": "categorical", "null_count": 10},  # Missing cardinality
        ],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
    )

    column_infos = reconstruct_column_infos(metadata)

    assert len(column_infos) == 2
    assert column_infos[0].cardinality is None
    assert column_infos[0].null_count is None
    assert column_infos[1].cardinality is None
    assert column_infos[1].null_count == 10


def test_spark_feature_metadata_with_complex_frequency_maps():
    """Test SparkFeatureMetadata with multiple categorical columns and frequency maps."""
    metadata = SparkFeatureMetadata(
        column_infos=[
            {"name": "region", "category": "categorical", "cardinality": 3},
            {"name": "product", "category": "categorical", "cardinality": 5},
        ],
        categorical_frequency_maps={
            "region": {"US": 0.5, "EU": 0.3, "APAC": 0.2},
            "product": {"A": 0.3, "B": 0.25, "C": 0.2, "D": 0.15, "E": 0.1},
        },
        onehot_categories={
            "region": ["US", "EU", "APAC"],
            "product": ["A", "B", "C", "D", "E"],
        },
        engineered_feature_names=STANDARD_REGION_PRODUCT_FEATURES,
    )

    assert len(metadata.categorical_frequency_maps) == 2
    assert sum(metadata.categorical_frequency_maps["region"].values()) == 1.0
    assert sum(metadata.categorical_frequency_maps["product"].values()) == 1.0


def test_spark_feature_metadata_empty_collections():
    """Test SparkFeatureMetadata with empty collections."""
    metadata = SparkFeatureMetadata(
        column_infos=[],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
    )

    assert not metadata.column_infos
    assert not metadata.categorical_frequency_maps
    assert not metadata.onehot_categories
    assert not metadata.engineered_feature_names

    # Should serialize and deserialize correctly
    json_str = metadata.to_json()
    restored = SparkFeatureMetadata.from_json(json_str)

    assert not restored.column_infos
    assert not restored.categorical_frequency_maps
    assert not restored.engineered_feature_names


def test_spark_feature_metadata_with_special_characters_in_names():
    """Test SparkFeatureMetadata with special characters in column names."""
    metadata = SparkFeatureMetadata(
        column_infos=[
            {"name": "col with spaces", "category": "numeric"},
            {"name": "col-with-dashes", "category": "numeric"},
            {"name": "col.with.dots", "category": "numeric"},
        ],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["col_with_spaces_scaled", "col_with_dashes_scaled", "col_with_dots_scaled"],
    )

    json_str = metadata.to_json()
    restored = SparkFeatureMetadata.from_json(json_str)

    assert len(restored.column_infos) == 3
    assert restored.column_infos[0]["name"] == "col with spaces"
    assert restored.column_infos[1]["name"] == "col-with-dashes"
    assert restored.column_infos[2]["name"] == "col.with.dots"


def test_column_type_info_numeric_types():
    """Test ColumnTypeInfo with different numeric Spark types."""
    numeric_types = [
        T.IntegerType(),
        T.LongType(),
        T.FloatType(),
        T.DoubleType(),
        T.DecimalType(10, 2),
    ]

    for spark_type in numeric_types:
        col_info = ColumnTypeInfo(
            name="numeric_col",
            spark_type=spark_type,
            category="numeric",
        )
        assert col_info.category == "numeric"
        assert isinstance(col_info.spark_type, type(spark_type))


def test_spark_feature_metadata_preserves_order():
    """Test that SparkFeatureMetadata preserves column and feature order."""
    column_infos = [
        {"name": "z_col", "category": "numeric"},
        {"name": "a_col", "category": "numeric"},
        {"name": "m_col", "category": "numeric"},
    ]

    metadata = SparkFeatureMetadata(
        column_infos=column_infos,
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=["z_col_scaled", "a_col_scaled", "m_col_scaled"],
    )

    # Order should be preserved
    assert metadata.column_infos[0]["name"] == "z_col"
    assert metadata.column_infos[1]["name"] == "a_col"
    assert metadata.column_infos[2]["name"] == "m_col"
    assert metadata.engineered_feature_names[0] == "z_col_scaled"
    assert metadata.engineered_feature_names[2] == "m_col_scaled"

    # Roundtrip should preserve order
    restored = SparkFeatureMetadata.from_json(metadata.to_json())
    assert restored.column_infos[0]["name"] == "z_col"
    assert restored.engineered_feature_names[0] == "z_col_scaled"
