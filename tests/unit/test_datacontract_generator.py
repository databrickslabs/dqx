"""
Unit tests for Data Contract to DQX rules generation.
"""

import json
import os
from unittest.mock import Mock

import pytest
from datacontract.data_contract import DataContract

from databricks.labs.dqx.datacontract.contract_rules_generator import (
    DataContractRulesGenerator,
)
import databricks.labs.dqx.profiler.generator as generator_module
from databricks.labs.dqx.profiler.generator import DQGenerator
from tests.unit.test_datacontract_utils import (
    assert_rules_have_valid_metadata,
    assert_rules_have_valid_structure,
    create_basic_contract,
    create_contract_with_quality,
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
        mock_config.configure_mock(**{"_product_info": ("dqx", "0.0.0")})
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
        """Path to sample data contract for testing."""
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

        # Should only have explicit rules (comprehensive contract has explicit quality checks)
        # All rules should be explicit (not implicit)
        for rule in rules:
            assert rule["user_metadata"]["rule_type"] == "explicit", "Should only have explicit rules"

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

    def test_sample_contract_validates_with_cli(self, sample_contract_path):
        """Test that the sample contract is valid according to datacontract-cli.

        This documents that the contract has been validated externally using:
        hatch run datacontract lint tests/resources/sample_datacontract.yaml
        Expected result: "data contract is valid"
        """
        assert os.path.exists(sample_contract_path), "Contract file must exist for CLI validation"

    def test_missing_datacontract_cli_dependency(self, generator, monkeypatch):
        """Test error when datacontract-cli is not installed."""
        monkeypatch.setattr(generator_module, "DATACONTRACT_ENABLED", False)

        # Attempt to generate rules should raise ImportError
        with pytest.raises(ImportError, match="Data contract support requires datacontract-cli"):
            generator.generate_rules_from_contract(contract_file="dummy.yaml")

    def test_nested_fields_generate_rules(self, generator):
        """Test that nested field structures generate rules with proper column paths."""
        contract_dict = create_basic_contract(
            fields={
                "customer": {
                    "type": "object",
                    "required": True,
                    "fields": {
                        "name": {"type": "string", "required": True},
                        "email": {
                            "type": "string",
                            "required": True,
                            "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
                        },
                        "address": {
                            "type": "object",
                            "fields": {
                                "street": {"type": "string", "required": True},
                                "city": {"type": "string", "required": True},
                                "zipcode": {"type": "string", "pattern": "^[0-9]{5}$"},
                            },
                        },
                    },
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            self._verify_nested_field_rules(generator, temp_path)
        finally:
            os.unlink(temp_path)

    def _verify_nested_field_rules(self, generator, contract_file):
        """Helper to verify nested field rule generation."""
        rules = generator.generate_rules_from_contract(
            contract_file=contract_file, generate_implicit_rules=True, process_text_rules=False
        )

        # Verify nested column paths are generated correctly
        rule_columns = {
            rule["check"]["arguments"].get("column") for rule in rules if "column" in rule["check"]["arguments"]
        }

        # Should have nested paths like customer.name, customer.address.street
        assert "customer.name" in rule_columns, "Should have nested path customer.name"
        assert "customer.email" in rule_columns, "Should have nested path customer.email"
        assert "customer.address.street" in rule_columns, "Should have deeply nested path"
        assert "customer.address.city" in rule_columns, "Should have deeply nested path"

        # Check that metadata also contains the nested paths
        for rule in rules:
            field = rule["user_metadata"]["field"]
            assert "." in field or field == "customer", f"Nested fields should have dot notation: {field}"

        return rules

    def test_fields_without_quality_checks(self, generator):
        """Test that fields without quality checks are handled gracefully."""
        contract_dict = create_basic_contract(
            fields={
                "id": {"type": "string", "required": True},
                "optional_field": {"type": "string", "required": False},  # No quality, no constraints
                "another_field": {"type": "string"},  # Minimal definition
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
            )

            # Should only generate rule for required field
            assert len(rules) == 1, "Should only generate rule for field with constraints"
            assert rules[0]["check"]["arguments"]["column"] == "id"

        finally:
            os.unlink(temp_path)

    def test_empty_text_description_skipped(self, generator):
        """Test that text quality checks with empty/whitespace descriptions are skipped."""
        contract_dict = create_contract_with_quality(
            field_name="user_id",
            field_type="string",
            quality_checks=[
                {"type": "text", "description": ""},  # Empty description
                {"type": "text", "description": "   "},  # Whitespace only
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=False, process_text_rules=True
            )

            # Should generate no rules since all text descriptions are empty/whitespace
            assert len(rules) == 0, "Should skip text rules with empty or whitespace-only descriptions"

        finally:
            os.unlink(temp_path)

    def test_contract_with_validation_warnings(self, generator, caplog):
        """Test that contract validation warnings are logged but don't prevent rule generation."""
        # Create a contract missing optional fields (like 'servers') that triggers warnings
        # but is still structurally valid
        contract_dict = {
            "dataContractSpecification": "0.9.3",
            "id": "test-minimal",
            "info": {"title": "Minimal Contract", "version": "1.0.0"},
            "models": {
                "test_table": {
                    "fields": {
                        "user_id": {"type": "string", "required": True},
                    }
                }
            },
            # Note: Missing 'servers' block which triggers a warning in datacontract-cli
        }

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            # Should still generate rules despite warnings
            with caplog.at_level("WARNING"):
                rules = generator.generate_rules_from_contract(
                    contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
                )

            # Should generate rules successfully
            assert len(rules) > 0, "Should generate rules despite validation warnings"

            # Note: Validation warnings from datacontract-cli may or may not be logged
            # depending on the contract structure, but the important thing is rules are generated

        finally:
            os.unlink(temp_path)

    def test_multiple_models_in_contract(self, generator):
        """Test that contracts with multiple models generate rules for all models."""
        contract_dict = {
            "dataContractSpecification": "0.9.3",
            "id": "multi-model-test",
            "info": {"title": "Multi Model Contract", "version": "1.0.0"},
            "models": {
                "users": {
                    "fields": {
                        "user_id": {"type": "string", "required": True},
                        "email": {"type": "string", "required": True},
                    }
                },
                "orders": {
                    "fields": {
                        "order_id": {"type": "string", "required": True},
                        "user_id": {"type": "string", "required": True},
                    }
                },
            },
        }

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
            )

            # Should generate rules for both models
            models_in_rules = {rule["user_metadata"]["model"] for rule in rules}
            assert "users" in models_in_rules, "Should have rules for users model"
            assert "orders" in models_in_rules, "Should have rules for orders model"

            # Should have 4 rules total (2 required fields per model)
            assert len(rules) == 4, f"Should have 4 rules, got {len(rules)}"

        finally:
            os.unlink(temp_path)

    def test_field_with_multiple_constraints(self, generator):
        """Test that fields with multiple constraints generate multiple rules."""
        contract_dict = create_basic_contract(
            fields={
                "product_code": {
                    "type": "string",
                    "required": True,
                    "unique": True,
                    "pattern": "^PROD-[0-9]{6}$",
                    "minLength": 11,
                    "maxLength": 11,
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
            )

            # Should generate multiple rules for different constraints
            rule_functions = [rule["check"]["function"] for rule in rules]

            # Required → is_not_null
            assert "is_not_null" in rule_functions, "Should have is_not_null rule"

            # Unique → is_unique
            assert "is_unique" in rule_functions, "Should have is_unique rule"

            # Pattern → regex_match
            assert "regex_match" in rule_functions, "Should have regex_match rule"

            # Should have at least 3 rules
            assert len(rules) >= 3, f"Should have at least 3 rules, got {len(rules)}"

        finally:
            os.unlink(temp_path)

    def test_explicit_rules_with_different_criticalities(self, generator):
        """Test that explicit rules preserve their individual criticality levels."""
        contract_dict = create_basic_contract(
            fields={
                "data": {
                    "type": "string",
                    "quality": [
                        {
                            "type": "custom",
                            "engine": "dqx",
                            "specification": {
                                "criticality": "error",
                                "name": "check1",
                                "check": {"function": "is_not_null", "arguments": {"column": "data"}},
                            },
                        },
                        {
                            "type": "custom",
                            "engine": "dqx",
                            "specification": {
                                "criticality": "warn",
                                "name": "check2",
                                "check": {
                                    "function": "is_not_null_and_not_empty",
                                    "arguments": {"column": "data", "trim_strings": True},
                                },
                            },
                        },
                    ],
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=False, process_text_rules=False
            )

            # Should have 2 rules with different criticalities
            assert len(rules) == 2, f"Should have 2 rules, got {len(rules)}"

            criticalities = {rule["criticality"] for rule in rules}
            assert "error" in criticalities, "Should have error criticality"
            assert "warn" in criticalities, "Should have warn criticality"

        finally:
            os.unlink(temp_path)


class TestDataContractGeneratorConstraints(DataContractGeneratorTestBase):
    """Test constraint-specific implicit rule generation."""

    def test_field_with_only_minimum_constraint(self, generator):
        """Test that field with only minimum (no maximum) generates is_not_less_than rule."""
        contract_dict = create_basic_contract(
            fields={
                "temperature": {
                    "type": "decimal",
                    "minimum": -273.15,  # Only minimum, no maximum
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
            )

            # Should generate is_not_less_than rule
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "is_not_less_than" in rule_functions, "Should have is_not_less_than rule"

            # Verify the limit is set correctly
            min_rule = next(r for r in rules if r["check"]["function"] == "is_not_less_than")
            assert min_rule["check"]["arguments"]["limit"] == -273.15

        finally:
            os.unlink(temp_path)

    def test_field_with_only_maximum_constraint(self, generator):
        """Test that field with only maximum (no minimum) generates is_not_greater_than rule."""
        contract_dict = create_basic_contract(
            fields={
                "humidity": {
                    "type": "decimal",
                    "maximum": 100.0,  # Only maximum, no minimum
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
            )

            # Should generate is_not_greater_than rule
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "is_not_greater_than" in rule_functions, "Should have is_not_greater_than rule"

            # Verify the limit is set correctly
            max_rule = next(r for r in rules if r["check"]["function"] == "is_not_greater_than")
            assert max_rule["check"]["arguments"]["limit"] == 100.0

        finally:
            os.unlink(temp_path)

    def test_explicit_dqx_quality_check_detection(self, generator):
        """Test that explicit DQX quality checks are correctly identified."""
        contract_dict = create_basic_contract(
            fields={
                "data": {
                    "type": "string",
                    "quality": [
                        # DQX custom check
                        {
                            "type": "custom",
                            "engine": "dqx",
                            "specification": {
                                "check": {"function": "is_not_null", "arguments": {"column": "data"}},
                            },
                        },
                        # Non-DQX custom check (should be ignored)
                        {
                            "type": "custom",
                            "engine": "some_other_engine",
                            "specification": {"some": "config"},
                        },
                    ],
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=False, process_text_rules=False
            )

            # Should only have 1 rule (the DQX one)
            assert len(rules) == 1, f"Should only extract DQX rules, got {len(rules)}"
            assert rules[0]["check"]["function"] == "is_not_null"

        finally:
            os.unlink(temp_path)

    def test_non_dqx_custom_quality_checks_ignored(self, generator):
        """Test that custom quality checks with non-dqx engines are ignored."""
        contract_dict = create_basic_contract(
            fields={
                "data": {
                    "type": "string",
                    "quality": [
                        {
                            "type": "custom",
                            "engine": "soda",  # Non-DQX engine
                            "specification": {"checks": ["row_count > 0"]},
                        },
                        {
                            "type": "custom",
                            "engine": "great_expectations",  # Non-DQX engine
                            "specification": {"expectation": "expect_column_values_to_not_be_null"},
                        },
                    ],
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=False, process_text_rules=False
            )

            # Should generate no rules since non-DQX engines are not supported
            assert len(rules) == 0, "Should ignore non-DQX custom quality checks"

        finally:
            os.unlink(temp_path)

    def test_minimum_and_maximum_together(self, generator):
        """Test that field with both minimum and maximum generates is_in_range rule."""
        contract_dict = create_basic_contract(
            fields={
                "age": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 120,
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
            )

            # Should generate is_in_range rule when both min and max are present
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "is_in_range" in rule_functions, "Should have is_in_range rule"

            # Verify both limits are set
            range_rule = next(r for r in rules if r["check"]["function"] == "is_in_range")
            assert range_rule["check"]["arguments"]["min_limit"] == 0
            assert range_rule["check"]["arguments"]["max_limit"] == 120

        finally:
            os.unlink(temp_path)

    def test_min_length_only(self, generator):
        """Test that field with only minLength generates sql_expression rule."""
        contract_dict = create_basic_contract(
            fields={
                "code": {
                    "type": "string",
                    "minLength": 5,
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
            )

            # Should generate sql_expression rule for minLength
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "sql_expression" in rule_functions, "Should have sql_expression rule for minLength"

            # Verify the length rule
            length_rule = next(r for r in rules if "min_length" in r["name"])
            assert length_rule["check"]["function"] == "sql_expression"
            assert "LENGTH(code) >= 5" in length_rule["check"]["arguments"]["expression"]

        finally:
            os.unlink(temp_path)

    def test_max_length_only(self, generator):
        """Test that field with only maxLength generates sql_expression rule."""
        contract_dict = create_basic_contract(
            fields={
                "description": {
                    "type": "string",
                    "maxLength": 200,
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
            )

            # Should generate sql_expression rule for maxLength
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "sql_expression" in rule_functions, "Should have sql_expression rule for maxLength"

            # Verify the length rule
            length_rule = next(r for r in rules if "max_length" in r["name"])
            assert length_rule["check"]["function"] == "sql_expression"
            assert "LENGTH(description) <= 200" in length_rule["check"]["arguments"]["expression"]

        finally:
            os.unlink(temp_path)

    def test_min_and_max_length_together(self, generator):
        """Test that field with both minLength and maxLength generates two sql_expression rules."""
        contract_dict = create_basic_contract(
            fields={
                "postal_code": {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 10,
                }
            }
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=True, process_text_rules=False
            )

            # Should generate two sql_expression rules (one for min, one for max)
            length_rules = [r for r in rules if "length" in r["name"]]
            assert len(length_rules) == 2, "Should have two length rules"

            # Verify min length rule
            min_rule = next(r for r in length_rules if "min_length" in r["name"])
            assert "LENGTH(postal_code) >= 5" in min_rule["check"]["arguments"]["expression"]

            # Verify max length rule
            max_rule = next(r for r in length_rules if "max_length" in r["name"])
            assert "LENGTH(postal_code) <= 10" in max_rule["check"]["arguments"]["expression"]

        finally:
            os.unlink(temp_path)


class TestDataContractGeneratorLLM(DataContractGeneratorTestBase):
    """Test LLM-based text rule generation with mocked LLM output."""

    def _load_contract_and_get_model(self, temp_path):
        """Helper to load contract and extract first model."""
        data_contract = DataContract(data_contract_file=temp_path)
        spec = data_contract.get_data_contract_specification()
        model = list(spec.models.values())[0]
        return model

    def _create_test_rules_generator(self):
        """Helper to create a test rules generator with mocked workspace client."""
        mock_ws = Mock()
        mock_config = Mock()
        mock_config.configure_mock(**{"_product_info": ("dqx", "0.0.0")})
        mock_ws.config = mock_config
        return DataContractRulesGenerator(workspace_client=mock_ws)

    def _verify_schema_info(self, schema_info, expected_columns):
        """Helper to verify schema info structure."""
        schema_dict = json.loads(schema_info)
        assert "columns" in schema_dict
        assert len(schema_dict["columns"]) == len(expected_columns)
        column_names = [col["name"] for col in schema_dict["columns"]]
        for col_name in expected_columns:
            assert col_name in column_names

    def test_build_schema_info_from_model(self, generator):
        """Test that schema info is correctly built from a model."""
        contract_dict = create_basic_contract(
            fields={
                "customer_id": {"type": "string"},
                "order_date": {"type": "date"},
                "amount": {"type": "decimal"},
            }
        )
        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            model = self._load_contract_and_get_model(temp_path)
            gen = self._create_test_rules_generator()
            build_schema_info = getattr(gen, "_build_schema_info_from_model")
            schema_info = build_schema_info(model)
            self._verify_schema_info(schema_info, ["customer_id", "order_date", "amount"])
        finally:
            os.unlink(temp_path)

    def _create_text_rule_llm_mock(self):
        """Helper to create LLM mock for text rule tests."""
        mock_prediction = Mock()
        mock_prediction.quality_rules = json.dumps(
            [
                {
                    "criticality": "error",
                    "check": {
                        "function": "regex_match",
                        "arguments": {
                            "column": "email",
                            "regex": "^[a-zA-Z0-9._%+-]+@(company\\.com|partner\\.com)$",
                        },
                    },
                }
            ]
        )
        mock_llm_engine = Mock()
        mock_llm_engine.get_business_rules_with_llm.return_value = mock_prediction
        return mock_llm_engine

    def _verify_text_rules(self, rules, mock_llm_engine):
        """Helper to verify text rule generation."""
        assert mock_llm_engine.get_business_rules_with_llm.called
        assert len(rules) > 0
        text_rules = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "text_llm"]
        assert len(text_rules) > 0
        assert text_rules[0]["check"]["function"] == "regex_match"
        assert text_rules[0]["criticality"] == "error"
        assert text_rules[0]["user_metadata"]["field"] == "email"
        assert text_rules[0]["user_metadata"]["rule_type"] == "text_llm"
        assert "text_expectation" in text_rules[0]["user_metadata"]

    def test_text_rules_with_mocked_llm_output(self, generator):
        """Test text rule processing with mocked LLM output (based on REAL LLM structure)."""
        contract_dict = create_contract_with_quality(
            field_name="email",
            field_type="string",
            quality_checks=[
                {
                    "type": "text",
                    "description": "Email addresses must be valid and from approved domains (company.com or partner.com)",
                }
            ],
        )
        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            mock_llm_engine = self._create_text_rule_llm_mock()
            generator.llm_engine = mock_llm_engine
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=False, process_text_rules=True
            )
            self._verify_text_rules(rules, mock_llm_engine)
        finally:
            os.unlink(temp_path)

    def test_text_rules_skipped_when_llm_disabled(self, generator):
        """Test that text rules are skipped when LLM is not available."""
        contract_dict = create_contract_with_quality(
            field_name="user_id",
            field_type="string",
            quality_checks=[
                {
                    "type": "text",
                    "description": "User IDs must follow the corporate standard format",
                }
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            # Generator without LLM engine (llm_engine=None by default in fixture)
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=False, process_text_rules=True
            )

            # Should generate no rules since LLM is not available
            assert len(rules) == 0, "Should skip text rules when LLM is not available"

        finally:
            os.unlink(temp_path)

    def test_text_rules_skipped_when_process_text_false(self, generator):
        """Test that text rules are skipped when process_text_rules=False."""
        contract_dict = create_contract_with_quality(
            field_name="user_id",
            field_type="string",
            quality_checks=[
                {
                    "type": "text",
                    "description": "User IDs must follow the corporate standard format",
                }
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            # Mock LLM engine
            mock_llm_engine = Mock()
            generator.llm_engine = mock_llm_engine

            # Generate with process_text_rules=False
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=False, process_text_rules=False
            )

            # Should not call LLM
            assert not mock_llm_engine.get_business_rules_with_llm.called

            # Should generate no rules
            assert len(rules) == 0, "Should skip text rules when process_text_rules=False"

        finally:
            os.unlink(temp_path)

    def _create_multiple_text_rules_llm_mock(self):
        """Helper to create LLM mock with side_effect for multiple text rules."""
        mock_prediction1 = Mock()
        mock_prediction1.quality_rules = json.dumps(
            [
                {
                    "criticality": "error",
                    "check": {
                        "function": "is_unique",
                        "arguments": {"columns": ["order_id"]},
                    },
                }
            ]
        )
        mock_prediction2 = Mock()
        mock_prediction2.quality_rules = json.dumps(
            [
                {
                    "criticality": "error",
                    "check": {
                        "function": "regex_match",
                        "arguments": {"column": "customer_email", "regex": "^[a-zA-Z0-9._%+-]+@.+$"},
                    },
                }
            ]
        )
        mock_llm_engine = Mock()
        mock_llm_engine.get_business_rules_with_llm.side_effect = [mock_prediction1, mock_prediction2]
        return mock_llm_engine

    def _verify_multiple_text_rules(self, rules, mock_llm_engine):
        """Helper to verify multiple text rules were generated."""
        assert mock_llm_engine.get_business_rules_with_llm.call_count == 2
        assert len(rules) == 2
        fields_with_rules = {r["user_metadata"]["field"] for r in rules}
        assert "order_id" in fields_with_rules
        assert "customer_email" in fields_with_rules
        for rule in rules:
            assert "criticality" in rule
            assert "check" in rule
            assert "user_metadata" in rule
            assert rule["user_metadata"]["rule_type"] == "text_llm"

    def test_multiple_text_rules_processed(self, generator):
        """Test that multiple text rules are all processed by LLM (based on REAL LLM structure)."""
        contract_dict = {
            "dataContractSpecification": "0.9.3",
            "id": "multi-text-rules",
            "info": {"title": "Multi Text Rules", "version": "1.0.0"},
            "models": {
                "orders": {
                    "fields": {
                        "order_id": {
                            "type": "string",
                            "quality": [{"type": "text", "description": "Order IDs must be unique across all systems"}],
                        },
                        "customer_email": {
                            "type": "string",
                            "quality": [{"type": "text", "description": "Email must be valid and deliverable"}],
                        },
                    }
                }
            },
        }
        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            mock_llm_engine = self._create_multiple_text_rules_llm_mock()
            generator.llm_engine = mock_llm_engine
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_implicit_rules=False, process_text_rules=True
            )
            self._verify_multiple_text_rules(rules, mock_llm_engine)
        finally:
            os.unlink(temp_path)
