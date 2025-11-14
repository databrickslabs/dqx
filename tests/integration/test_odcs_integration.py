"""
Integration tests for ODCS data contract to DQX rules generation.
"""

import os
import yaml

import pytest
from pyspark.sql import types as T

from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine


class TestODCSIntegration:
    """Integration tests for ODCS contract processing."""

    @pytest.fixture
    def sample_contract_path(self):
        """Path to sample ODCS contract."""
        tests_dir = os.path.dirname(os.path.dirname(__file__))
        return os.path.join(tests_dir, "resources", "sample_odcs_contract.yaml")

    @pytest.fixture
    def sample_contract(self, sample_contract_path):
        """Load sample ODCS contract."""
        with open(sample_contract_path, encoding='utf-8') as f:
            return yaml.safe_load(f)

    def test_generate_rules_from_sample_contract(self, ws, spark, sample_contract):
        """Test generating rules from sample ODCS contract."""
        generator = DQGenerator(workspace_client=ws, spark=spark)

        rules = generator.generate_rules_from_contract(
            contract=sample_contract, generate_implicit_rules=True, process_text_rules=False
        )

        # Should generate multiple implicit rules
        assert len(rules) > 0

        # Verify rule structure
        for rule in rules:
            assert "check" in rule
            assert "function" in rule["check"]
            assert "arguments" in rule["check"]
            assert "criticality" in rule
            assert "user_metadata" in rule
            assert "odcs_contract_name" in rule["user_metadata"]
            assert "odcs_contract_version" in rule["user_metadata"]

        # Verify rules validate successfully
        status = DQEngine.validate_checks(rules)
        assert not status.has_errors

    def test_apply_generated_rules_to_dataframe(self, ws, spark, sample_contract):
        """Test applying generated rules to actual DataFrame."""
        generator = DQGenerator(workspace_client=ws, spark=spark)

        # Generate rules (without text rules to avoid LLM dependency)
        rules = generator.generate_rules_from_contract(
            contract=sample_contract, generate_implicit_rules=True, process_text_rules=False
        )

        # Create sample DataFrame matching the contract schema
        schema = T.StructType(
            [
                T.StructField("order_id", T.StringType(), True),
                T.StructField("customer_id", T.StringType(), True),
                T.StructField("customer_email", T.StringType(), True),
                T.StructField("order_date", T.DateType(), True),
                T.StructField("order_timestamp", T.TimestampType(), True),
                T.StructField("order_total", T.DecimalType(10, 2), True),
                T.StructField("order_status", T.StringType(), True),
                T.StructField("quantity", T.IntegerType(), True),
                T.StructField("discount_percentage", T.DecimalType(5, 2), True),
                T.StructField("shipping_address", T.StringType(), True),
                T.StructField("product_category", T.StringType(), True),
                T.StructField("priority_order", T.BooleanType(), True),
                T.StructField("customer_notes", T.StringType(), True),
                T.StructField("delivery_date", T.DateType(), True),
            ]
        )

        data = [
            (
                "ORD-12345678",
                "CUST-123456",
                "customer@example.com",
                "2024-01-15",
                "2024-01-15 10:30:00",
                99.99,
                "confirmed",
                2,
                10.0,
                "123 Main St",
                "electronics",
                False,
                "Please deliver before noon",
                "2024-01-20",
            ),
            (
                "ORD-87654321",
                "CUST-654321",
                "another@example.com",
                "2024-01-16",
                "2024-01-16 14:20:00",
                150.50,
                "shipped",
                1,
                5.0,
                "456 Oak Ave",
                "home",
                True,
                None,
                "2024-01-22",
            ),
        ]

        df = spark.createDataFrame(data, schema)

        # Apply generated rules
        engine = DQEngine(spark)
        result_df = engine.apply_checks_by_metadata(df, rules)

        # Verify result DataFrame
        assert result_df is not None
        assert result_df.count() == 2

        # Check that result columns were added
        result_columns = result_df.columns
        assert len(result_columns) > len(df.columns)

    def test_apply_rules_with_validation_failures(self, ws, spark):
        """Test applying generated rules to DataFrame with invalid data."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "name": "test_contract",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "user_id": {"logicalType": "string", "required": True, "pattern": "^USER-[0-9]{4}$"},
                    "age": {"logicalType": "integer", "minValue": 0, "maxValue": 120},
                    "status": {"logicalType": "string", "validValues": ["active", "inactive"]},
                }
            },
        }

        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract=contract_dict, generate_implicit_rules=True, process_text_rules=False
        )

        # Create DataFrame with some invalid data
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

        df = spark.createDataFrame(data, schema)

        # Apply rules
        engine = DQEngine(spark)
        result_df = engine.apply_checks_by_metadata(df, rules)

        # Verify errors were detected
        assert result_df.count() == 5

        # Split into good and bad
        good_df, bad_df = engine.apply_checks_by_metadata_and_split(df, rules)

        # Only the first row should be good
        assert good_df.count() == 1
        assert bad_df.count() == 4

    def test_multiple_properties_generate_multiple_rules(self, ws, spark):
        """Test that each property with constraints generates appropriate rules."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "id": {"required": True, "unique": True},
                    "email": {"required": True, "pattern": "^.+@.+$"},
                    "age": {"minValue": 0, "maxValue": 150},
                    "status": {"validValues": ["A", "B", "C"]},
                }
            },
        }

        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(contract=contract_dict)

        # Verify we have at least 6 rules generated
        assert len(rules) >= 6

        # Verify different rule types are present
        functions = [r["check"]["function"] for r in rules]
        assert "is_not_null" in functions
        assert "is_unique" in functions
        assert "regex_match" in functions
        assert "is_in_range" in functions
        assert "is_in_list" in functions

    def test_default_criticality_applied(self, ws, spark):
        """Test that default_criticality is correctly applied to all implicit rules."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "id": {"required": True, "unique": True},
                    "email": {"pattern": "^.+@.+$"},
                    "age": {"minValue": 0, "maxValue": 120},
                }
            },
        }

        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(contract=contract_dict, default_criticality="warn")

        # All implicit rules should have the default criticality
        assert len(rules) == 4  # is_not_null, is_unique, regex_match, is_in_range
        assert all(r["criticality"] == "warn" for r in rules)

    def test_contract_metadata_preserved(self, ws, spark, sample_contract):
        """Test that contract metadata is preserved in generated rules."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(contract=sample_contract, process_text_rules=False)

        contract_name = sample_contract["name"]
        contract_version = sample_contract["version"]

        for rule in rules:
            metadata = rule["user_metadata"]
            assert metadata["odcs_contract_name"] == contract_name
            assert metadata["odcs_contract_version"] == contract_version
            assert "odcs_property" in metadata
            assert "odcs_rule_type" in metadata
            assert metadata["odcs_rule_type"] in {"implicit", "explicit", "text_llm"}

    def test_skip_implicit_rules_flag(self, ws, spark):
        """Test that generate_implicit_rules=False skips implicit rules."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {"properties": {"id": {"required": True}, "email": {"pattern": "^.+@.+$"}}},
        }

        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract=contract_dict, generate_implicit_rules=False, process_text_rules=False
        )

        # Should have no rules since implicit generation is disabled
        assert len(rules) == 0

    def test_date_and_timestamp_format_validation(self, ws, spark):
        """Test that date and timestamp format validations are generated correctly."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "event_date": {"logicalType": "date", "format": "yyyy-MM-dd", "required": True},
                    "event_timestamp": {
                        "logicalType": "timestamp",
                        "format": "yyyy-MM-dd HH:mm:ss",
                        "required": True,
                    },
                }
            },
        }

        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(contract=contract_dict)

        # Should have date and timestamp validation rules
        date_rules = [r for r in rules if r["check"]["function"] == "is_valid_date"]
        timestamp_rules = [r for r in rules if r["check"]["function"] == "is_valid_timestamp"]

        assert len(date_rules) == 1
        assert len(timestamp_rules) == 1
        assert date_rules[0]["check"]["arguments"]["date_format"] == "yyyy-MM-dd"
        assert timestamp_rules[0]["check"]["arguments"]["timestamp_format"] == "yyyy-MM-dd HH:mm:ss"
