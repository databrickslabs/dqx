"""
Integration tests for Data Contract to DQX rules generation.
"""

import os

import pytest
from datacontract.data_contract import DataContract

from databricks.labs.dqx.profiler.generator import DQGenerator
from tests.unit.test_datacontract_utils import (
    assert_rules_have_valid_metadata,
    assert_rules_have_valid_structure,
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

        actual_rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=False
        )

        # Verify generated rules have valid structure and metadata
        assert_rules_have_valid_structure(actual_rules)
        assert_rules_have_valid_metadata(actual_rules)

        # Get predefined and explicit rules
        predefined_rules = [r for r in actual_rules if r["user_metadata"]["rule_type"] == "predefined"]
        explicit_rules = [r for r in actual_rules if r["user_metadata"]["rule_type"] == "explicit"]

        # Verify we have both types
        assert len(predefined_rules) > 0, "Expected predefined rules to be generated"
        assert len(explicit_rules) > 0, "Expected explicit rules to be generated"

        # Verify key field rules are present (sampling approach)
        # Test sensor_id (has required, unique, pattern, length)
        sensor_id_rules = {r["check"]["function"] for r in predefined_rules if "sensor_id" in r["name"]}
        assert sensor_id_rules >= {"is_not_null", "is_unique", "regex_match"}, "sensor_id missing expected rules"

        # Test temperature_celsius (float range uses sql_expression)
        temp_rules = [r for r in predefined_rules if "temperature_celsius" in r["name"]]
        temp_functions = [r["check"]["function"] for r in temp_rules]
        assert "is_not_null" in temp_functions, "temperature_celsius should have is_not_null"
        assert "sql_expression" in temp_functions, "temperature_celsius float range should use sql_expression"

        # Test vibration_level (integer range uses is_in_range)
        vib_rules = [r for r in predefined_rules if "vibration_level" in r["name"]]
        vib_range_rule = next((r for r in vib_rules if r["check"]["function"] == "is_in_range"), None)
        assert vib_range_rule is not None, "vibration_level should use is_in_range for integer range"
        assert vib_range_rule["check"]["arguments"] == {
            "column": "vibration_level",
            "min_limit": 0,
            "max_limit": 10,
        }

        # Verify explicit rule types are present
        explicit_functions = {r["check"]["function"] for r in explicit_rules}
        assert "is_not_null_and_not_empty" in explicit_functions, "Missing explicit is_not_null_and_not_empty"
        assert "is_valid_timestamp" in explicit_functions, "Missing explicit is_valid_timestamp"
        assert "is_data_fresh_per_time_window" in explicit_functions, "Missing dataset-level freshness check"
        assert "is_aggr_not_less_than" in explicit_functions, "Missing dataset-level count check"

    def test_generate_rules_from_datacontract_object(self, ws, spark, sample_contract_path):
        """Test generating rules from DataContract object - should produce same output as from file."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        contract = DataContract(data_contract_file=sample_contract_path)

        rules_from_object = generator.generate_rules_from_contract(
            contract=contract, generate_predefined_rules=True, process_text_rules=False
        )

        # Also generate from file for comparison
        rules_from_file = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=False
        )

        # Verify rules were generated
        assert len(rules_from_object) > 0
        assert_rules_have_valid_structure(rules_from_object)
        assert_rules_have_valid_metadata(rules_from_object)

        # Sort both for consistent comparison
        rules_from_object_sorted = sorted(rules_from_object, key=lambda r: r.get("name", ""))
        rules_from_file_sorted = sorted(rules_from_file, key=lambda r: r.get("name", ""))

        # Rules generated from object should match rules from file
        assert len(rules_from_object_sorted) == len(
            rules_from_file_sorted
        ), f"Expected {len(rules_from_file_sorted)} rules from object, got {len(rules_from_object_sorted)}"

        # Compare each rule
        for obj_rule, file_rule in zip(rules_from_object_sorted, rules_from_file_sorted):
            assert obj_rule["check"] == file_rule["check"], f"Check mismatch for rule {obj_rule.get('name')}"
            assert (
                obj_rule["criticality"] == file_rule["criticality"]
            ), f"Criticality mismatch for rule {obj_rule.get('name')}"
            assert (
                obj_rule["user_metadata"]["rule_type"] == file_rule["user_metadata"]["rule_type"]
            ), f"Rule type mismatch for rule {obj_rule.get('name')}"

    def test_contract_metadata_preserved(self, ws, spark, sample_contract_path):
        """Test that contract metadata is preserved in generated rules."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(contract_file=sample_contract_path, process_text_rules=False)
        assert_rules_have_valid_metadata(rules)

    def test_default_criticality_applied(self, ws, spark, sample_contract_path):
        """Test that default_criticality is correctly applied."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(contract_file=sample_contract_path, default_criticality="warn")

        # All predefined rules should have the default criticality
        predefined_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "predefined"]
        assert all(r["criticality"] == "warn" for r in predefined_rules)

    def test_skip_predefined_rules_flag(self, ws, spark, sample_contract_path):
        """Test that generate_predefined_rules=False skips predefined rules."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=False, process_text_rules=False
        )

        # Should only have explicit rules (no predefined rules)
        # Sample ODCS v3.x contract has 7 explicit DQX rules (5 property-level + 2 schema-level)
        assert len(rules) == 7

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

    def test_generate_rules_with_text_processing(self, ws, spark, sample_contract_path):
        """Test generating rules with process_text_rules=True for LLM-based rule generation."""
        generator = DQGenerator(workspace_client=ws, spark=spark)

        # Generate rules with text processing enabled
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=True
        )

        # Verify rules were generated
        assert len(rules) > 0
        assert_rules_have_valid_structure(rules)
        assert_rules_have_valid_metadata(rules)

        # Verify that text-based rules were processed by LLM
        # The sample contract has a text expectation about duplicate sensor readings
        text_llm_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "text_llm"]
        assert len(text_llm_rules) > 0, "Expected at least one text_llm rule from text expectations"

        # Verify that we still have predefined and explicit rules too
        predefined_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "predefined"]
        explicit_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "explicit"]
        assert len(predefined_rules) > 0, "Expected predefined rules"
        assert len(explicit_rules) > 0, "Expected explicit DQX rules"
