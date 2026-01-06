"""Unit tests for anomaly detection utility functions."""

from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.trainer import ensure_full_model_name
from databricks.labs.dqx.anomaly.transformers import (
    ColumnTypeInfo,
    SparkFeatureMetadata,
    reconstruct_column_infos,
)

from tests.unit.test_anomaly_test_helpers import STANDARD_REGION_PRODUCT_FEATURES


# ============================================================================
# Model Name Utility Tests
# ============================================================================


def test_ensure_full_model_name_simple_model():
    """Test model name completion with simple name."""
    result = ensure_full_model_name("my_model", "catalog.schema.registry")
    assert result == "catalog.schema.my_model"


def test_ensure_full_model_name_two_part_name():
    """Test model name completion with schema.model format."""
    result = ensure_full_model_name("schema.my_model", "catalog.schema.registry")
    # Should prepend catalog
    assert result.startswith("catalog.")
    assert "my_model" in result


def test_ensure_full_model_name_already_complete():
    """Test that complete model names are not modified."""
    complete_name = "catalog.schema.my_model"
    result = ensure_full_model_name(complete_name, "other_catalog.other_schema.registry")
    # Should return as-is (already has 2+ dots)
    assert result == complete_name


def test_ensure_full_model_name_different_registries():
    """Test model name completion with different registry tables."""
    # Test with main catalog
    result1 = ensure_full_model_name("model1", "main.anomaly.registry")
    assert result1 == "main.anomaly.model1"

    # Test with prod catalog
    result2 = ensure_full_model_name("model2", "prod.ml_models.registry")
    assert result2 == "prod.ml_models.model2"

    # Test with dev catalog
    result3 = ensure_full_model_name("model3", "dev.test_schema.registry")
    assert result3 == "dev.test_schema.model3"


def test_ensure_full_model_name_extracts_catalog_schema():
    """Test that catalog and schema are correctly extracted from registry table."""
    model_name = "simple_model"

    # 3-part registry table
    result = ensure_full_model_name(model_name, "cat.sch.registry")
    assert result.startswith("cat.sch.")

    # Complex names
    result2 = ensure_full_model_name(model_name, "my_catalog.my_schema.my_registry")
    assert result2.startswith("my_catalog.my_schema.")


# ============================================================================
# Column Type Info Reconstruction Tests
# ============================================================================


def test_reconstruct_column_infos_basic():
    """Test basic reconstruction of ColumnTypeInfo objects."""
    metadata = SparkFeatureMetadata(
        column_infos=[
            {"name": "col1", "category": "numeric"},
            {"name": "col2", "category": "categorical"},
        ],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
    )

    result = reconstruct_column_infos(metadata)

    assert len(result) == 2
    assert all(isinstance(info, ColumnTypeInfo) for info in result)
    assert result[0].name == "col1"
    assert result[0].category == "numeric"
    assert result[1].name == "col2"
    assert result[1].category == "categorical"


def test_reconstruct_column_infos_with_all_fields():
    """Test reconstruction with all optional fields."""
    metadata = SparkFeatureMetadata(
        column_infos=[
            {
                "name": "amount",
                "category": "numeric",
                "cardinality": None,
                "null_count": 5,
            },
            {
                "name": "region",
                "category": "categorical",
                "cardinality": 3,
                "null_count": 0,
            },
        ],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
    )

    result = reconstruct_column_infos(metadata)

    assert result[0].name == "amount"
    assert result[0].cardinality is None
    assert result[0].null_count == 5
    assert result[1].name == "region"
    assert result[1].cardinality == 3
    assert result[1].null_count == 0


def test_reconstruct_column_infos_preserves_order():
    """Test that column order is preserved during reconstruction."""
    column_names = ["z_col", "a_col", "m_col", "b_col"]
    metadata = SparkFeatureMetadata(
        column_infos=[{"name": name, "category": "numeric"} for name in column_names],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
    )

    result = reconstruct_column_infos(metadata)

    assert [info.name for info in result] == column_names


def test_reconstruct_column_infos_handles_missing_optional_fields():
    """Test reconstruction with missing optional fields."""
    metadata = SparkFeatureMetadata(
        column_infos=[
            {"name": "col1", "category": "numeric"},  # Missing cardinality, null_count
        ],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
    )

    result = reconstruct_column_infos(metadata)

    assert len(result) == 1
    assert result[0].name == "col1"
    assert result[0].cardinality is None
    assert result[0].null_count is None


def test_reconstruct_column_infos_empty_list():
    """Test reconstruction with empty column list."""
    metadata = SparkFeatureMetadata(
        column_infos=[],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
    )

    result = reconstruct_column_infos(metadata)

    assert result == []


def test_reconstruct_column_infos_all_categories():
    """Test reconstruction with all category types."""
    categories = ["numeric", "categorical", "datetime", "boolean", "unsupported"]

    metadata = SparkFeatureMetadata(
        column_infos=[{"name": f"col_{cat}", "category": cat} for cat in categories],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
    )

    result = reconstruct_column_infos(metadata)

    assert len(result) == len(categories)
    for i, category in enumerate(categories):
        assert result[i].category == category


# ============================================================================
# Feature Metadata JSON Serialization Tests
# ============================================================================
# Note: Full roundtrip test with complex metadata is in test_anomaly_transformers.py


def test_feature_metadata_json_handles_empty_maps():
    """Test JSON serialization with empty maps and lists."""
    metadata = SparkFeatureMetadata(
        column_infos=[],
        categorical_frequency_maps={},
        onehot_categories={},
        engineered_feature_names=[],
    )

    json_str = metadata.to_json()
    restored = SparkFeatureMetadata.from_json(json_str)

    assert not restored.column_infos
    assert not restored.categorical_frequency_maps
    assert not restored.onehot_categories
    assert not restored.engineered_feature_names


def test_feature_metadata_json_handles_complex_frequency_maps():
    """Test JSON serialization with complex frequency maps."""
    metadata = SparkFeatureMetadata(
        column_infos=[
            {"name": "region", "category": "categorical"},
            {"name": "product", "category": "categorical"},
        ],
        categorical_frequency_maps={
            "region": {"US": 0.5, "EU": 0.3, "APAC": 0.2},
            "product": {"A": 0.4, "B": 0.3, "C": 0.2, "D": 0.1},
        },
        onehot_categories={
            "region": ["US", "EU", "APAC"],
            "product": ["A", "B", "C", "D"],
        },
        engineered_feature_names=STANDARD_REGION_PRODUCT_FEATURES[:7],  # Exclude "product_E"
    )

    json_str = metadata.to_json()
    restored = SparkFeatureMetadata.from_json(json_str)

    # Verify frequency maps are preserved with correct values
    assert restored.categorical_frequency_maps["region"]["US"] == 0.5
    assert restored.categorical_frequency_maps["product"]["A"] == 0.4
    assert len(restored.onehot_categories["region"]) == 3
    assert len(restored.onehot_categories["product"]) == 4


# ============================================================================
# Column Type Info Edge Cases
# ============================================================================


def test_column_type_info_with_none_values():
    """Test ColumnTypeInfo with None for optional fields."""
    info = ColumnTypeInfo(
        name="test_col",
        spark_type=T.StringType(),
        category="numeric",
        cardinality=None,
        null_count=None,
        encoding_strategy=None,
    )

    assert info.name == "test_col"
    assert info.cardinality is None
    assert info.null_count is None
    assert info.encoding_strategy is None


def test_column_type_info_with_zero_values():
    """Test ColumnTypeInfo with zero for counts."""
    info = ColumnTypeInfo(
        name="perfect_col",
        spark_type=T.DoubleType(),
        category="numeric",
        cardinality=0,
        null_count=0,
    )

    assert info.cardinality == 0
    assert info.null_count == 0


def test_column_type_info_different_spark_types():
    """Test ColumnTypeInfo with various Spark types."""
    types_to_test = [
        T.IntegerType(),
        T.LongType(),
        T.FloatType(),
        T.DoubleType(),
        T.StringType(),
        T.BooleanType(),
        T.TimestampType(),
        T.DateType(),
    ]

    for spark_type in types_to_test:
        info = ColumnTypeInfo(
            name="test_col",
            spark_type=spark_type,
            category="numeric",
        )
        assert isinstance(info.spark_type, type(spark_type))


# ============================================================================
# Integration: Full Workflow Tests
# ============================================================================


def test_full_metadata_workflow():
    """Test complete workflow: create → to_json → from_json → reconstruct."""
    # Step 1: Create metadata
    original_metadata = SparkFeatureMetadata(
        column_infos=[
            {"name": "amount", "category": "numeric", "null_count": 10},
            {"name": "region", "category": "categorical", "cardinality": 3},
        ],
        categorical_frequency_maps={"region": {"US": 0.6, "EU": 0.4}},
        onehot_categories={"region": ["US", "EU"]},
        engineered_feature_names=["amount_scaled", "region_US", "region_EU"],
    )

    # Step 2: Serialize
    json_str = original_metadata.to_json()

    # Step 3: Deserialize
    restored_metadata = SparkFeatureMetadata.from_json(json_str)

    # Step 4: Reconstruct column infos
    column_infos = reconstruct_column_infos(restored_metadata)

    # Verify end-to-end
    assert len(column_infos) == 2
    assert column_infos[0].name == "amount"
    assert column_infos[0].category == "numeric"
    assert column_infos[0].null_count == 10
    assert column_infos[1].name == "region"
    assert column_infos[1].category == "categorical"
    assert column_infos[1].cardinality == 3


def test_model_name_workflow():
    """Test model name workflow with various inputs."""
    registry_table = "main.anomaly.model_registry"

    # Simple name
    result1 = ensure_full_model_name("fraud_model", registry_table)
    assert result1 == "main.anomaly.fraud_model"

    # Already full name
    result2 = ensure_full_model_name("prod.ml.existing_model", registry_table)
    assert result2 == "prod.ml.existing_model"

    # Verify catalog/schema extraction is consistent
    assert result1.split(".", maxsplit=1)[0] == "main"
    assert result1.split(".", maxsplit=2)[1] == "anomaly"
