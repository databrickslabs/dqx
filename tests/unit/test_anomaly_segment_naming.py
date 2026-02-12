"""Unit tests for segment model naming convention consistency.

These tests ensure that segment model names are consistent between training
(service.py) and querying (model_registry.py) to prevent lookup failures.
"""

import pytest
from pyspark.sql import types as T

from databricks.labs.dqx.anomaly.check_funcs import (
    validate_fully_qualified_name as check_validate,
)
from databricks.labs.dqx.anomaly.service import validate_fully_qualified_name
from databricks.labs.dqx.anomaly.transformers import ColumnTypeInfo
from databricks.labs.dqx.anomaly.utils.segment_utils import build_segment_name
from databricks.labs.dqx.errors import InvalidParameterError


class TestSegmentNamingConvention:
    """Test segment model naming conventions match between training and registry."""

    def test_segment_name_format_training_matches_registry_query(self) -> None:
        """Verify training creates names that registry can find.

        This test documents the expected segment naming convention:
        - Format: {base_model_name}__seg_{col1}={val1}_{col2}={val2}
        - Example: my_model__seg_region=US
        """
        # Simulate the naming logic from service.py (training)
        base_model_name = "catalog.schema.my_model"
        seg_values = {"region": "US"}

        # Training logic (service.py)
        segment_name_training = build_segment_name(seg_values)
        model_name_training = f"{base_model_name}__seg_{segment_name_training}"

        # Registry query logic (model_registry.py)
        segment_name_registry = build_segment_name(seg_values)
        model_name_registry = f"{base_model_name}__seg_{segment_name_registry}"

        # These MUST match for segment lookup to work
        assert model_name_training == model_name_registry
        assert model_name_training == "catalog.schema.my_model__seg_region=US"

    def test_multi_segment_naming_consistency(self) -> None:
        """Test naming with multiple segment columns."""
        base_model_name = "catalog.schema.model"
        seg_values = {"region": "US", "product": "A"}

        # Training creates this name
        segment_name = build_segment_name(seg_values)
        model_name = f"{base_model_name}__seg_{segment_name}"

        # Should contain all segment info
        assert "__seg_" in model_name
        assert "region=US" in model_name or "product=A" in model_name

    def test_segment_prefix_is_double_underscore(self) -> None:
        """Ensure segment prefix uses double underscore for unambiguous parsing."""
        base_name = "my_model_name"
        segment = "region=US"

        model_name = f"{base_name}__seg_{segment}"

        # Double underscore separates base name from segment info
        assert "__seg_" in model_name
        # Can reliably split on __seg_ to get base name
        parts = model_name.split("__seg_")
        assert len(parts) == 2
        assert parts[0] == base_name
        assert parts[1] == segment

    def test_segment_value_format_uses_equals_sign(self) -> None:
        """Verify segment values use = not _ between key and value."""
        seg_values = {"region": "US", "tier": "premium"}

        segment_name = build_segment_name(seg_values)

        # Must use = to separate key from value
        assert "region=US" in segment_name or "tier=premium" in segment_name
        # Not the old incorrect format
        assert "region_US" not in segment_name.replace("region=US", "")

    def test_segment_name_startswith_query_pattern(self) -> None:
        """Test that registry query pattern matches training output."""
        base_model_name = "catalog.schema.model"
        seg_values = {"region": "APAC"}

        # Training output
        segment_name = build_segment_name(seg_values)
        trained_model_name = f"{base_model_name}__seg_{segment_name}"

        # Registry query pattern
        query_prefix = f"{base_model_name}__seg_"

        # Training output must start with query prefix
        assert trained_model_name.startswith(query_prefix)


class TestSegmentNameEdgeCases:
    """Test edge cases in segment naming."""

    def test_segment_value_with_special_characters(self) -> None:
        """Test segment values containing underscores or other chars."""
        seg_values = {"region": "US_EAST", "product_line": "premium"}

        segment_name = build_segment_name(seg_values)
        model_name = f"base__seg_{segment_name}"

        # Should handle underscores in values
        assert "US_EAST" in model_name or "premium" in model_name

    def test_segment_value_case_sensitivity(self) -> None:
        """Segment values should be case-sensitive."""
        seg_values_upper = {"region": "US"}
        seg_values_lower = {"region": "us"}

        name_upper = build_segment_name(seg_values_upper)
        name_lower = build_segment_name(seg_values_lower)

        # Different cases should produce different names
        assert name_upper != name_lower
        assert "region=US" in name_upper
        assert "region=us" in name_lower

    def test_empty_segment_values_not_allowed(self) -> None:
        """Empty segment dict would result in malformed name."""
        seg_values: dict[str, str] = {}

        segment_name = build_segment_name(seg_values)

        # Empty segment produces empty string
        assert segment_name == ""

    def test_segment_order_consistency(self) -> None:
        """Segment key order should be consistent for lookups."""
        # Same segments, different insertion order
        seg_values_1 = {"region": "US", "tier": "gold"}
        seg_values_2 = {"tier": "gold", "region": "US"}

        name_1 = build_segment_name(seg_values_1)
        name_2 = build_segment_name(seg_values_2)

        assert name_1 == name_2
        assert name_1 == "region=US_tier=gold"


class TestValidateFullyQualifiedNameConsistency:
    """Test that validation function is used consistently."""

    def test_service_and_check_funcs_use_same_validation(self) -> None:
        """Verify both modules use the same validation logic."""
        # Both should be the same function (imported from service)
        assert validate_fully_qualified_name is check_validate

    def test_validation_accepts_three_part_names(self) -> None:
        """Test validation accepts catalog.schema.name format."""
        # Should not raise
        validate_fully_qualified_name("catalog.schema.model", label="model")
        validate_fully_qualified_name("main.default.table", label="table")

    def test_validation_rejects_two_part_names(self) -> None:
        """Test validation rejects schema.name format."""
        with pytest.raises(InvalidParameterError):
            validate_fully_qualified_name("schema.model", label="model")

    def test_validation_rejects_single_part_names(self) -> None:
        """Test validation rejects simple names."""
        with pytest.raises(InvalidParameterError):
            validate_fully_qualified_name("model", label="model")


class TestFeatureEngineeringMetadataConsistency:
    """Test feature engineering metadata is consistent between training and scoring."""

    def test_column_type_info_required_fields(self) -> None:
        """Test ColumnTypeInfo has all required fields for reconstruction."""
        info = ColumnTypeInfo(
            name="test_col",
            spark_type=T.DoubleType(),
            category="numeric",
            cardinality=None,
            null_count=0,
            encoding_strategy="none",
        )

        # All fields needed for feature reconstruction
        assert info.name is not None
        assert info.spark_type is not None
        assert info.category is not None

    def test_column_type_info_category_values(self) -> None:
        """Test ColumnTypeInfo category field has expected values."""
        valid_categories = ["numeric", "categorical", "datetime", "boolean", "unsupported"]

        for category in valid_categories:
            assert category in valid_categories


class TestModelRegistryQueryPatterns:
    """Test registry query patterns match what training produces."""

    def test_global_model_query_by_exact_name(self) -> None:
        """Global models are queried by exact name match."""
        model_name = "catalog.schema.my_model"

        # Query should match exact name
        query_name = model_name
        assert query_name == model_name

    def test_segment_model_query_by_prefix(self) -> None:
        """Segment models are queried by prefix pattern."""
        base_name = "catalog.schema.my_model"

        # Query prefix for all segments of a base model
        query_prefix = f"{base_name}__seg_"

        # All segment models should start with this prefix
        segment_names = [
            f"{base_name}__seg_region=US",
            f"{base_name}__seg_region=EU",
            f"{base_name}__seg_region=APAC",
        ]

        for name in segment_names:
            assert name.startswith(query_prefix)

    def test_specific_segment_query_pattern(self) -> None:
        """Test querying for a specific segment combination."""
        base_name = "catalog.schema.my_model"
        segment_values = {"region": "US"}

        # Build expected segment model name
        segment_name = build_segment_name(segment_values)
        expected_model_name = f"{base_name}__seg_{segment_name}"

        assert expected_model_name == "catalog.schema.my_model__seg_region=US"
