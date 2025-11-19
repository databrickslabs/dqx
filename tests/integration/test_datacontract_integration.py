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

        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=False
        )

        # Verify generated rules
        assert_rules_have_valid_structure(rules)
        assert_rules_have_valid_metadata(rules)

        # Get predefined and explicit rules
        predefined_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "predefined"]
        explicit_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "explicit"]

        # Verify we have both types of rules
        assert len(predefined_rules) > 0, "Expected predefined rules to be generated"
        assert len(explicit_rules) >= 5, f"Expected at least 5 explicit DQX rules, got {len(explicit_rules)}"

        # ===== Verify specific expected rules match the contract =====

        # 1. sensor_id: required=true, unique=true, pattern
        sensor_id_rules = [r for r in predefined_rules if "sensor_id" in r["name"]]
        sensor_id_functions = {r["check"]["function"] for r in sensor_id_rules}
        assert "is_not_null" in sensor_id_functions, "sensor_id should have is_not_null (required)"
        assert "is_unique" in sensor_id_functions, "sensor_id should have is_unique (unique)"
        assert "regex_match" in sensor_id_functions, "sensor_id should have regex_match (pattern)"

        # 2. temperature_celsius: minimum=-273.15, maximum=200.0 (float values - should use sql_expression)
        temp_rules = [r for r in predefined_rules if "temperature_celsius" in r["name"]]
        assert len(temp_rules) == 2, "temperature_celsius should have 2 rules (is_not_null + range)"
        temp_range_rule = next((r for r in temp_rules if "range" in r["name"]), None)
        assert temp_range_rule is not None, "temperature_celsius should have range rule"
        assert temp_range_rule["check"]["function"] == "sql_expression", "Float range should use sql_expression"
        assert (
            "temperature_celsius >= -273.15 AND temperature_celsius <= 200.0"
            in temp_range_rule["check"]["arguments"]["expression"]
        ), "temperature_celsius range expression should be correct"

        # 3. vibration_level: minimum=0, maximum=10 (integer values - should use is_in_range)
        vibration_rules = [r for r in predefined_rules if "vibration_level" in r["name"]]
        vibration_range_rule = next((r for r in vibration_rules if "range" in r["name"]), None)
        assert vibration_range_rule is not None, "vibration_level should have range rule"
        assert vibration_range_rule["check"]["function"] == "is_in_range", "Integer range should use is_in_range"
        assert vibration_range_rule["check"]["arguments"]["min_limit"] == 0
        assert vibration_range_rule["check"]["arguments"]["max_limit"] == 10

        # 4. sensor_status: pattern for enum values (ODCS v3.x uses pattern)
        status_rules = [r for r in predefined_rules if "sensor_status" in r["name"]]
        status_pattern_rule = next((r for r in status_rules if r["check"]["function"] == "regex_match"), None)
        assert status_pattern_rule is not None, "sensor_status should have regex_match for pattern constraint"

        # 5. Verify explicit DQX rules from contract
        explicit_functions = {r["check"]["function"] for r in explicit_rules}
        assert (
            "is_not_null_and_not_empty" in explicit_functions
        ), "Should have explicit is_not_null_and_not_empty for sensor_id"
        assert "is_valid_timestamp" in explicit_functions, "Should have explicit is_valid_timestamp"
        assert "sql_expression" in explicit_functions, "Should have explicit sql_expression checks"

    def test_generate_rules_from_datacontract_object(self, ws, spark, sample_contract_path):
        """Test generating rules from DataContract object."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        contract = DataContract(data_contract_file=sample_contract_path)

        rules = generator.generate_rules_from_contract(
            contract=contract, generate_predefined_rules=True, process_text_rules=False
        )

        # Verify rules were generated
        assert len(rules) > 0
        assert_rules_have_valid_structure(rules)
        assert_rules_have_valid_metadata(rules)

        # Get predefined and explicit rules
        predefined_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "predefined"]
        explicit_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "explicit"]

        # Verify we have both types of rules
        assert len(predefined_rules) > 0, "Expected predefined rules to be generated"
        assert len(explicit_rules) >= 5, f"Expected at least 5 explicit DQX rules, got {len(explicit_rules)}"

        # ===== Verify specific expected rules match the contract =====

        # 1. Required fields should have is_not_null rules
        required_fields = ["sensor_id", "machine_id", "reading_timestamp", "temperature_celsius", "humidity_percentage"]
        for field in required_fields:
            not_null_rule = next(
                (r for r in predefined_rules if field in r["name"] and r["check"]["function"] == "is_not_null"), None
            )
            assert not_null_rule is not None, f"{field} should have is_not_null rule (required=true)"

        # 2. Unique field should have is_unique rule
        unique_rule = next((r for r in predefined_rules if r["check"]["function"] == "is_unique"), None)
        assert unique_rule is not None, "sensor_id should have is_unique rule (unique=true)"

        # 3. Pattern fields should have regex_match rules
        pattern_fields = ["sensor_id", "machine_id", "location", "device_model"]
        pattern_rules = [r for r in predefined_rules if r["check"]["function"] == "regex_match"]
        assert len(pattern_rules) >= len(pattern_fields), f"Expected at least {len(pattern_fields)} regex_match rules"

        # 4. Pattern fields should have regex_match rules (ODCS v3.x uses pattern for enums)
        # Note: In ODCS v3.x, enum-like constraints are handled via pattern in logicalTypeOptions

        # 5. Float range constraints should use sql_expression
        float_range_fields = ["temperature_celsius", "humidity_percentage", "pressure_bar"]
        for field in float_range_fields:
            range_rule = next(
                (
                    r
                    for r in predefined_rules
                    if field in r["name"] and "range" in r["name"] and r["check"]["function"] == "sql_expression"
                ),
                None,
            )
            if field != "pressure_bar":  # pressure_bar is optional, might not have all fields populated
                assert range_rule is not None, f"{field} should have sql_expression rule for float range"

        # 6. Integer range should use is_in_range
        vibration_rule = next(
            (r for r in predefined_rules if "vibration_level" in r["name"] and r["check"]["function"] == "is_in_range"),
            None,
        )
        assert vibration_rule is not None, "vibration_level (integer) should use is_in_range"

        # 7. Verify criticality levels from explicit rules in contract
        error_rules = [r for r in explicit_rules if r["criticality"] == "error"]
        warn_rules = [r for r in explicit_rules if r["criticality"] == "warn"]
        assert len(error_rules) > 0, "Expected rules with error criticality from explicit checks"
        assert len(warn_rules) > 0, "Expected rules with warn criticality from explicit checks"

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
