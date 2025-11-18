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

        # Verify specific expected rules based on sample_datacontract.yaml
        # The contract has sensor_id field with: required=true, unique=true, pattern
        rule_functions = {r["check"]["function"] for r in rules}
        assert "is_not_null" in rule_functions, "Expected is_not_null for required field sensor_id"
        assert "is_unique" in rule_functions, "Expected is_unique for unique field sensor_id"
        assert "regex_match" in rule_functions, "Expected regex_match for pattern constraint"

        # Verify we have explicit DQX rules from the contract
        explicit_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "explicit"]
        assert len(explicit_rules) >= 5, f"Expected at least 5 explicit DQX rules, got {len(explicit_rules)}"

        # Verify predefined rules exist
        predefined_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "predefined"]
        assert len(predefined_rules) > 0, "Expected predefined rules to be generated"

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

        # Verify specific expected rules
        rule_functions = {r["check"]["function"] for r in rules}
        assert "is_not_null" in rule_functions, "Expected is_not_null for required fields"
        assert "is_unique" in rule_functions, "Expected is_unique for unique fields"

        # Verify criticality levels from contract
        error_rules = [r for r in rules if r["criticality"] == "error"]
        assert len(error_rules) > 0, "Expected rules with error criticality"

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
        # The sample contract has a text expectation (line 60-63) about duplicate sensor readings
        text_llm_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "text_llm"]
        assert len(text_llm_rules) > 0, "Expected at least one text_llm rule from text expectations"

        # Verify that we still have predefined and explicit rules too
        predefined_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "predefined"]
        explicit_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "explicit"]
        assert len(predefined_rules) > 0, "Expected predefined rules"
        assert len(explicit_rules) > 0, "Expected explicit DQX rules"
