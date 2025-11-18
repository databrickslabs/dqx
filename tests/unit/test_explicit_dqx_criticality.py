"""
Test that explicit DQX rules preserve criticality from ODCS contracts.
"""

import os
from unittest.mock import Mock

import pytest

from databricks.labs.dqx.profiler.generator import DQGenerator
from tests.unit.test_datacontract_utils import (
    create_basic_contract,
    create_contract_with_quality,
    create_test_contract_file,
)


class TestExplicitDQXCriticality:
    """Test criticality handling for explicit DQX rules in ODCS contracts."""

    @pytest.fixture
    def mock_workspace_client(self):
        """Create mock WorkspaceClient."""
        mock_ws = Mock()
        mock_config = Mock()
        setattr(mock_config, "_product_info", ("dqx", "0.0.0"))
        mock_ws.config = mock_config
        mock_ws.clusters.select_spark_version = Mock()
        return mock_ws

    @pytest.fixture
    def mock_spark(self):
        """Create mock SparkSession."""
        return Mock()

    @pytest.fixture
    def generator(self, mock_workspace_client, mock_spark):
        """Create DQGenerator instance."""
        return DQGenerator(workspace_client=mock_workspace_client, spark=mock_spark)

    def test_explicit_rule_with_criticality_warn(self, generator):
        """Test that explicit DQX rules preserve 'warn' criticality from contract."""
        contract_dict = create_contract_with_quality(
            field_name="email",
            field_type="string",
            quality_checks=[
                {
                    "type": "custom",
                    "engine": "dqx",
                    "specification": {
                        "criticality": "warn",  # Explicitly set to warn
                        "check": {
                            "function": "regex_match",
                            "arguments": {
                                "column": "email",
                                "regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
                            },
                        },
                    },
                }
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=False
            )

            assert len(rules) == 1
            assert rules[0]["criticality"] == "warn"  # Should preserve 'warn' from contract
            assert rules[0]["check"]["function"] == "regex_match"
        finally:
            os.unlink(temp_path)

    def test_explicit_rule_with_criticality_error(self, generator):
        """Test that explicit DQX rules preserve 'error' criticality from contract."""
        contract_dict = create_contract_with_quality(
            field_name="user_id",
            field_type="string",
            quality_checks=[
                {
                    "type": "custom",
                    "engine": "dqx",
                    "specification": {
                        "criticality": "error",  # Explicitly set to error
                        "check": {
                            "function": "is_not_null_and_not_empty",
                            "arguments": {"column": "user_id", "trim_strings": True},
                        },
                    },
                }
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=False
            )

            assert len(rules) == 1
            assert rules[0]["criticality"] == "error"  # Should preserve 'error' from contract
            assert rules[0]["check"]["function"] == "is_not_null_and_not_empty"
        finally:
            os.unlink(temp_path)

    def test_explicit_rule_without_criticality_uses_dqx_default(self, generator):
        """Test that explicit DQX rules without criticality use DQX's default (error)."""
        contract_dict = create_contract_with_quality(
            field_name="user_id",
            field_type="string",
            quality_checks=[
                {
                    "type": "custom",
                    "engine": "dqx",
                    "specification": {
                        # No criticality specified
                        "check": {
                            "function": "is_not_null",
                            "arguments": {"column": "user_id"},
                        },
                    },
                }
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=False
            )

            assert len(rules) == 1
            # When criticality is not specified in contract, it won't be in the rule
            # DQX engine will use its default 'error' when applying the rule
            assert "criticality" not in rules[0] or rules[0].get("criticality") == "error"
            assert rules[0]["check"]["function"] == "is_not_null"
        finally:
            os.unlink(temp_path)

    def test_predefined_rules_use_default_criticality(self, generator):
        """Test that predefined rules use the default_criticality parameter."""
        contract_dict = create_basic_contract(fields={"user_id": {"type": "string", "required": True}})

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            # Generate with warn as default
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=True,
                process_text_rules=False,
                default_criticality="warn",
            )

            assert len(rules) == 1
            assert rules[0]["criticality"] == "warn"  # Predefined rule should use default_criticality
            assert rules[0]["check"]["function"] == "is_not_null"
        finally:
            os.unlink(temp_path)
