"""
Integration tests for Data Contract to DQX rules generation.
"""

import os

import pytest
from datacontract.data_contract import DataContract
from pyspark.sql import types as T

from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
from tests.unit.datacontract_test_helpers import (
    assert_rules_have_valid_metadata,
    assert_rules_have_valid_structure,
    create_test_contract_file,
)


class TestDataContractIntegration:
    """Integration tests for data contract processing."""

    @pytest.fixture
    def sample_contract_path(self):
        """Path to sample data contract."""
        tests_dir = os.path.dirname(os.path.dirname(__file__))
        return os.path.join(tests_dir, "resources", "sample_datacontract.yaml")

    def test_generate_rules_from_contract_file(self, ws, spark, sample_contract_path):
        """Test generating rules from data contract file."""
        generator = DQGenerator(workspace_client=ws, spark=spark)

        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_implicit_rules=True, process_text_rules=False
        )

        # Verify generated rules
        assert_rules_have_valid_structure(rules)
        assert_rules_have_valid_metadata(rules)

    def test_generate_rules_from_datacontract_object(self, ws, spark, sample_contract_path):
        """Test generating rules from DataContract object."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        contract = DataContract(data_contract_file=sample_contract_path)

        rules = generator.generate_rules_from_contract(
            contract=contract, generate_implicit_rules=True, process_text_rules=False
        )

        # Verify rules were generated
        assert len(rules) > 0
        assert_rules_have_valid_structure(rules)
        assert_rules_have_valid_metadata(rules)

    def test_apply_generated_rules_to_dataframe(self, ws, spark, sample_contract_path):
        """Test applying generated rules to actual DataFrame."""
        generator = DQGenerator(workspace_client=ws)

        # Generate rules (only implicit rules to avoid Spark validation issues)
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_implicit_rules=True, process_text_rules=False
        )

        # Filter out rules with float arguments (is_in_range with decimal values)
        # These cause validation warnings but work correctly when applied
        valid_rules = []
        for rule in rules:
            args = rule.get("check", {}).get("arguments", {})
            has_float = any(isinstance(v, float) for v in args.values())
            if not has_float:
                valid_rules.append(rule)

        # Create sample DataFrame matching the contract schema (subset of fields)
        from datetime import datetime
        
        schema = T.StructType(
            [
                T.StructField("sensor_id", T.StringType(), True),
                T.StructField("reading_timestamp", T.TimestampType(), True),
                T.StructField("temperature_celsius", T.DoubleType(), True),
                T.StructField("sensor_status", T.StringType(), True),
            ]
        )

        data = [
            ("SENS-001", datetime(2024, 1, 15, 10, 30, 0), 25.5, "active"),
            ("SENS-002", datetime(2024, 1, 16, 14, 20, 0), 30.2, "maintenance"),
        ]

        df = spark.createDataFrame(data, schema)

        # Apply generated rules
        engine = DQEngine(workspace_client=ws)
        result_df = engine.apply_checks_by_metadata(df, valid_rules)

        # Verify result DataFrame
        assert result_df is not None
        assert result_df.count() == 2

        # Check that result columns were added
        result_columns = result_df.columns
        assert len(result_columns) > len(df.columns)

    def test_contract_metadata_preserved(self, ws, spark, sample_contract_path):
        """Test that contract metadata is preserved in generated rules."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(contract_file=sample_contract_path, process_text_rules=False)
        assert_rules_have_valid_metadata(rules)

    def test_default_criticality_applied(self, ws, spark, sample_contract_path):
        """Test that default_criticality is correctly applied."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(contract_file=sample_contract_path, default_criticality="warn")

        # All implicit rules should have the default criticality
        implicit_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "implicit"]
        assert all(r["criticality"] == "warn" for r in implicit_rules)

    def test_skip_implicit_rules_flag(self, ws, spark, sample_contract_path):
        """Test that generate_implicit_rules=False skips implicit rules."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_implicit_rules=False, process_text_rules=False
        )

        # Should only have explicit rules (no implicit rules)
        # Sample contract has 5 explicit DQX rules
        assert len(rules) == 5
        
        # Verify all rules are explicit
        for rule in rules:
            assert rule["user_metadata"]["rule_type"] == "explicit"

    def test_multiple_fields_generate_multiple_rules(self, ws, spark, sample_contract_path):
        """Test that multiple fields with constraints generate appropriate rules."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(contract_file=sample_contract_path)

        # Verify we have multiple rules generated
        assert len(rules) >= 10

        # Verify different rule types are present
        functions = {r["check"]["function"] for r in rules}
        assert "is_not_null" in functions
        assert "is_unique" in functions
        assert "regex_match" in functions
        assert "is_in_list" in functions

    def test_apply_rules_with_validation_failures(self, ws, spark):
        """Test applying generated rules to DataFrame with invalid data."""
        # Create temporary contract file
        temp_path = create_test_contract_file()

        try:
            # Generate rules from contract
            rules = self._generate_test_rules(ws, spark, temp_path)

            # Create test DataFrame with invalid data
            df = self._create_test_dataframe_with_invalid_data(spark)

            # Apply rules and verify results
            self._verify_validation_failures(ws, df, rules)
        finally:
            os.unlink(temp_path)

    def _generate_test_rules(self, ws, spark, contract_path: str) -> list[dict]:
        """Generate rules from test contract."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        return generator.generate_rules_from_contract(
            contract_file=contract_path, generate_implicit_rules=True, process_text_rules=False
        )

    def _create_test_dataframe_with_invalid_data(self, spark):
        """Create test DataFrame with mix of valid and invalid data."""
        schema = T.StructType(
            [
                T.StructField("user_id", T.StringType(), True),
                T.StructField("age", T.IntegerType(), True),
                T.StructField("status", T.StringType(), True),
            ]
        )

        data = [
            ("USER-0001", 25, "active"),  # Valid
            (None, 30, "active"),  # Invalid: null user_id
            ("USER-0002", 150, "active"),  # Invalid: age > 120
            ("USER-0003", 40, "unknown"),  # Invalid: status not in valid values
            ("INVALID", 35, "inactive"),  # Invalid: pattern mismatch
        ]

        return spark.createDataFrame(data, schema)

    def _verify_validation_failures(self, ws, df, rules):
        """Verify that validation failures are correctly detected."""
        engine = DQEngine(workspace_client=ws)
        result_df = engine.apply_checks_by_metadata(df, rules)

        # Verify errors were detected
        assert result_df.count() == 5

        # Split into good and bad
        good_df, bad_df = engine.apply_checks_by_metadata_and_split(df, rules)

        # Only the first row should be good
        assert good_df.count() == 1
        assert bad_df.count() == 4
