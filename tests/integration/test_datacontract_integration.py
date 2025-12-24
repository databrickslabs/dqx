import os
import pytest
from databricks.labs.dqx.profiler.generator import DQGenerator


class TestDataContractIntegration:
    """Integration tests for data contract processing."""

    @pytest.fixture
    def sample_contract_path(self):
        """Path to sample data contract."""
        tests_dir = os.path.dirname(os.path.dirname(__file__))
        return os.path.join(tests_dir, "resources", "sample_datacontract.yaml")

    def test_generate_rules_with_text_processing(self, ws, spark, sample_contract_path):
        """Test generating rules with process_text_rules=True for LLM-based rule generation."""
        generator = DQGenerator(workspace_client=ws, spark=spark)

        # Generate rules with text processing enabled
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=True
        )

        # Verify rules were generated
        assert len(rules) > 36  # More than predefined + explicit rules due to text-based rules

        # Verify that text-based rules were processed by LLM
        # The sample contract has a text expectation about duplicate sensor readings
        text_llm_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "text_llm"]
        assert len(text_llm_rules) > 0, "Expected at least one text_llm rule from text expectations"

        # Verify that we still have predefined and explicit rules too
        predefined_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "predefined"]
        explicit_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "explicit"]
        assert len(predefined_rules) > 0, "Expected predefined rules"
        assert len(explicit_rules) > 0, "Expected explicit DQX rules"
