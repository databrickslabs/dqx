"""
Unit tests for Data Contract to DQX rules generation.
"""

import os
from unittest.mock import Mock

import pytest
from datacontract.data_contract import DataContract

from databricks.labs.dqx.profiler.generator import DQGenerator
from tests.unit.datacontract_test_helpers import (
    assert_rules_have_valid_metadata,
    assert_rules_have_valid_structure,
    create_basic_contract,
    create_test_contract_file,
)


class DataContractGeneratorTestBase:
    """Base class with shared fixtures for data contract generator tests."""

    @pytest.fixture
    def mock_workspace_client(self):
        """Create mock WorkspaceClient."""
        mock_ws = Mock()
        # Configure mock config with product_info for telemetry
        mock_config = Mock()
        setattr(mock_config, '_product_info', ("dqx", "0.0.0"))
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
        gen = DQGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
        gen.llm_engine = None
        return gen

    @pytest.fixture
    def sample_contract_path(self):
        """Path to sample data contract."""
        tests_dir = os.path.dirname(os.path.dirname(__file__))
        return os.path.join(tests_dir, "resources", "sample_datacontract.yaml")


class TestDataContractGeneratorBasic(DataContractGeneratorTestBase):
    """Test basic data contract generator functionality."""

    def test_import_error_without_datacontract_cli(self, generator):
        """Test that appropriate error is raised if datacontract-cli not installed."""
        # This test documents the expected behavior when neither contract nor file is provided
        # In dev env with datacontract-cli, it will raise ValueError
        with pytest.raises((ImportError, ValueError)):
            generator.generate_rules_from_contract(contract_file=None, contract=None)

    def test_requires_either_contract_or_file(self, generator):
        """Test that either contract or contract_file must be provided."""
        with pytest.raises(ValueError, match="Either 'contract' or 'contract_file' must be provided"):
            generator.generate_rules_from_contract()

    def test_cannot_provide_both_contract_and_file(self, generator, sample_contract_path):
        """Test that both contract and contract_file cannot be provided."""
        contract = DataContract(data_contract_file=sample_contract_path)
        with pytest.raises(ValueError, match="Cannot provide both"):
            generator.generate_rules_from_contract(contract=contract, contract_file=sample_contract_path)

    def test_generate_rules_from_file(self, generator, sample_contract_path):
        """Test generating rules from a contract file path."""
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_implicit_rules=True, process_text_rules=False
        )

        assert isinstance(rules, list)
        assert_rules_have_valid_structure(rules)

    def test_generate_rules_from_datacontract_object(self, generator, sample_contract_path):
        """Test generating rules from a DataContract object."""
        contract = DataContract(data_contract_file=sample_contract_path)
        rules = generator.generate_rules_from_contract(
            contract=contract, generate_implicit_rules=True, process_text_rules=False
        )

        assert isinstance(rules, list)
        assert len(rules) > 0

    def test_unsupported_contract_format(self, generator, sample_contract_path):
        """Test error for unsupported contract format."""
        with pytest.raises(ValueError, match="Contract format 'unknown' not supported"):
            generator.generate_rules_from_contract(contract_file=sample_contract_path, contract_format="unknown")

    def test_skip_implicit_rules(self, generator, sample_contract_path):
        """Test that generate_implicit_rules=False works."""
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_implicit_rules=False, process_text_rules=False
        )

        # Should have no rules since we disabled implicit and text
        assert len(rules) == 0

    def test_default_criticality(self, generator, sample_contract_path):
        """Test that default_criticality is applied."""
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path,
            default_criticality="warn",
            generate_implicit_rules=True,
            process_text_rules=False,
        )

        # All implicit rules should have warn criticality
        assert all(r["criticality"] == "warn" for r in rules if r["user_metadata"]["rule_type"] == "implicit")


class TestDataContractGeneratorImplicitRules(DataContractGeneratorTestBase):
    """Test implicit rule generation from field constraints."""

    def test_required_field_generates_is_not_null(self, generator):
        """Test that required fields generate is_not_null rules."""
        # Create a simple contract with a required field
        contract_dict = create_basic_contract(fields={"user_id": {"type": "string", "required": True}})

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
            )

            assert len(rules) == 1
            assert rules[0]["check"]["function"] == "is_not_null"
            assert rules[0]["check"]["arguments"]["column"] == "user_id"
            assert rules[0]["user_metadata"]["rule_type"] == "implicit"
            assert rules[0]["user_metadata"]["dimension"] == "completeness"
        finally:
            os.unlink(temp_path)

    def test_unique_field_generates_is_unique(self, generator):
        """Test that unique fields generate is_unique rules."""
        contract_dict = create_basic_contract(fields={"user_id": {"type": "string", "unique": True}})

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(contract_file=temp_path)

            unique_rules = [r for r in rules if r["check"]["function"] == "is_unique"]
            assert len(unique_rules) == 1
            assert unique_rules[0]["check"]["arguments"]["columns"] == ["user_id"]
        finally:
            os.unlink(temp_path)

    def test_enum_generates_is_in_list(self, generator):
        """Test that enum fields generate is_in_list rules."""
        contract_dict = create_basic_contract(
            fields={"status": {"type": "string", "enum": ["active", "inactive", "pending"]}}
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(contract_file=temp_path)

            enum_rules = [r for r in rules if r["check"]["function"] == "is_in_list"]
            assert len(enum_rules) == 1
            assert set(enum_rules[0]["check"]["arguments"]["allowed"]) == {"active", "inactive", "pending"}
        finally:
            os.unlink(temp_path)

    def test_pattern_generates_regex_match(self, generator):
        """Test that pattern fields generate regex_match rules."""
        contract_dict = create_basic_contract(fields={"code": {"type": "string", "pattern": "^[A-Z]{3}-[0-9]{4}$"}})

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(contract_file=temp_path)

            pattern_rules = [r for r in rules if r["check"]["function"] == "regex_match"]
            assert len(pattern_rules) == 1
            assert pattern_rules[0]["check"]["arguments"]["regex"] == "^[A-Z]{3}-[0-9]{4}$"
        finally:
            os.unlink(temp_path)

    def test_range_generates_is_in_range(self, generator):
        """Test that minimum/maximum generate is_in_range rules."""
        contract_dict = create_basic_contract(fields={"age": {"type": "integer", "minimum": 0, "maximum": 120}})

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(contract_file=temp_path)

            range_rules = [r for r in rules if r["check"]["function"] == "is_in_range"]
            assert len(range_rules) == 1
            assert range_rules[0]["check"]["arguments"]["min_limit"] == 0
            assert range_rules[0]["check"]["arguments"]["max_limit"] == 120
        finally:
            os.unlink(temp_path)

    def test_format_generates_date_validation(self, generator):
        """Test that format generates date/timestamp validation rules."""
        contract_dict = create_basic_contract(
            fields={
                "event_date": {"type": "date", "format": "%Y-%m-%d"},
                "event_time": {"type": "timestamp", "format": "%Y-%m-%d %H:%M:%S"},
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(contract_file=temp_path)

            date_rules = [r for r in rules if r["check"]["function"] == "is_valid_date"]
            timestamp_rules = [r for r in rules if r["check"]["function"] == "is_valid_timestamp"]

            assert len(date_rules) == 1
            assert len(timestamp_rules) == 1
        finally:
            os.unlink(temp_path)

    def test_metadata_contains_contract_info(self, generator, sample_contract_path):
        """Test that generated rules contain contract metadata."""
        rules = generator.generate_rules_from_contract(contract_file=sample_contract_path, process_text_rules=False)
        assert_rules_have_valid_metadata(rules)
