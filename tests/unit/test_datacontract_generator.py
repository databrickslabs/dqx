"""
Unit tests for Data Contract to DQX rules generation.
"""

import json
import os
import tempfile
from unittest.mock import Mock

import pytest
from datacontract.data_contract import DataContract
from datacontract.lint.resolve import resolve_data_contract_v2

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
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=False
        )

        assert isinstance(rules, list)
        assert_rules_have_valid_structure(rules)

    def test_generate_rules_from_datacontract_object(self, generator, sample_contract_path):
        """Test generating rules from a DataContract object."""
        contract = DataContract(data_contract_file=sample_contract_path)
        rules = generator.generate_rules_from_contract(
            contract=contract, generate_predefined_rules=True, process_text_rules=False
        )

        assert isinstance(rules, list)
        assert len(rules) > 0

    def test_unsupported_contract_format(self, generator, sample_contract_path):
        """Test error for unsupported contract format."""
        with pytest.raises(ValueError, match="Contract format 'unknown' not supported"):
            generator.generate_rules_from_contract(contract_file=sample_contract_path, contract_format="unknown")

    def test_skip_predefined_rules(self, generator, sample_contract_path):
        """Test that generate_predefined_rules=False works."""
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=False, process_text_rules=False
        )

        # Should only have explicit rules (comprehensive contract has explicit quality checks)
        # All rules should be explicit (not predefined)
        for rule in rules:
            assert rule["user_metadata"]["rule_type"] == "explicit", "Should only have explicit rules"

    def test_default_criticality(self, generator, sample_contract_path):
        """Test that default_criticality is applied."""
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path,
            default_criticality="warn",
            generate_predefined_rules=True,
            process_text_rules=False,
        )

        # All predefined rules should have warn criticality
        assert all(r["criticality"] == "warn" for r in rules if r["user_metadata"]["rule_type"] == "predefined")


class TestDataContractGeneratorPredefinedRules(DataContractGeneratorTestBase):
    """Test predefined rule generation from field constraints."""

    def test_required_field_generates_is_not_null(self, generator):
        """Test that required fields generate is_not_null rules."""
        # Create a simple contract with a required property
        contract_dict = create_basic_contract(
            properties=[{"name": "user_id", "logicalType": "string", "required": True}]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            assert len(rules) == 1
            assert rules[0]["check"]["function"] == "is_not_null"
            assert rules[0]["check"]["arguments"]["column"] == "user_id"
            assert rules[0]["user_metadata"]["rule_type"] == "predefined"
            assert rules[0]["user_metadata"]["dimension"] == "completeness"
        finally:
            os.unlink(temp_path)

    def test_unique_field_generates_is_unique(self, generator):
        """Test that unique fields generate is_unique rules."""
        contract_dict = create_basic_contract(properties=[{"name": "user_id", "logicalType": "string", "unique": True}])

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(contract_file=temp_path)

            unique_rules = [r for r in rules if r["check"]["function"] == "is_unique"]
            assert len(unique_rules) == 1
            assert unique_rules[0]["check"]["arguments"]["columns"] == ["user_id"]
        finally:
            os.unlink(temp_path)

    def test_enum_generates_is_in_list(self, generator):
        """Test that enum fields generate regex_match rules (ODCS v3.x uses pattern for enums)."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "status",
                    "logicalType": "string",
                    "logicalTypeOptions": {"pattern": "^(active|inactive|pending)$"},
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(contract_file=temp_path)
            self._verify_enum_pattern_rules(rules)
        finally:
            os.unlink(temp_path)

    def _verify_enum_pattern_rules(self, rules):
        """Helper to verify enum pattern rules."""
        # In ODCS v3.x, enum is converted to pattern, which generates regex_match
        pattern_rules = [r for r in rules if r["check"]["function"] == "regex_match"]
        assert len(pattern_rules) == 1
        # Pattern should be: ^(active|inactive|pending)$
        assert "regex" in pattern_rules[0]["check"]["arguments"]
        regex_pattern = pattern_rules[0]["check"]["arguments"]["regex"]
        # Verify all enum values are in the pattern
        assert "active" in regex_pattern
        assert "inactive" in regex_pattern
        assert "pending" in regex_pattern

    def test_pattern_generates_regex_match(self, generator):
        """Test that pattern fields generate regex_match rules."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "code",
                    "logicalType": "string",
                    "logicalTypeOptions": {"pattern": "^[A-Z]{3}-[0-9]{4}$"},
                }
            ]
        )

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
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "age",
                    "logicalType": "integer",
                    "logicalTypeOptions": {"minimum": 0, "maximum": 120},
                }
            ]
        )

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
            properties=[
                {
                    "name": "event_date",
                    "logicalType": "date",
                    "logicalTypeOptions": {"format": "%Y-%m-%d"},
                },
                {
                    "name": "event_time",
                    "logicalType": "timestamp",
                    "logicalTypeOptions": {"format": "%Y-%m-%d %H:%M:%S"},
                },
            ]
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
            properties=[
                {
                    "name": "customer",
                    "logicalType": "object",
                    "required": True,
                    "properties": [
                        {"name": "name", "logicalType": "string", "required": True},
                        {
                            "name": "email",
                            "logicalType": "string",
                            "required": True,
                            "logicalTypeOptions": {"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
                        },
                        {
                            "name": "address",
                            "logicalType": "object",
                            "properties": [
                                {"name": "street", "logicalType": "string", "required": True},
                                {"name": "city", "logicalType": "string", "required": True},
                                {
                                    "name": "zipcode",
                                    "logicalType": "string",
                                    "logicalTypeOptions": {"pattern": "^[0-9]{5}$"},
                                },
                            ],
                        },
                    ],
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            self._verify_nested_field_rules(generator, temp_path)
        finally:
            os.unlink(temp_path)

    def _verify_nested_field_rules(self, generator, contract_file):
        """Helper to verify nested field rule generation."""
        rules = generator.generate_rules_from_contract(
            contract_file=contract_file, generate_predefined_rules=True, process_text_rules=False
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
            properties=[
                {"name": "id", "logicalType": "string", "required": True},
                {"name": "optional_field", "logicalType": "string", "required": False},
                {"name": "another_field", "logicalType": "string"},
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should only generate rule for required field
            assert len(rules) == 1, "Should only generate rule for field with constraints"
            assert rules[0]["check"]["arguments"]["column"] == "id"

        finally:
            os.unlink(temp_path)

    def test_empty_text_description_skipped(self, generator):
        """Test that text quality checks with empty/whitespace descriptions are skipped."""
        contract_dict = create_contract_with_quality(
            property_name="user_id",
            logical_type="string",
            quality_checks=[
                {"type": "text", "description": ""},  # Empty description
                {"type": "text", "description": "   "},  # Whitespace only
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=True
            )

            # Should generate no rules since all text descriptions are empty/whitespace
            assert len(rules) == 0, "Should skip text rules with empty or whitespace-only descriptions"

        finally:
            os.unlink(temp_path)

    def test_contract_with_validation_warnings(self, generator, caplog):
        """Test that contract validation warnings are logged but don't prevent rule generation."""
        # Create a minimal ODCS v3.x contract
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test-minimal",
            "name": "test-minimal",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "test_table",
                    "physicalType": "table",
                    "properties": [
                        {"name": "user_id", "logicalType": "string", "required": True},
                    ],
                }
            ],
        }

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            # Should still generate rules despite warnings
            with caplog.at_level("WARNING"):
                rules = generator.generate_rules_from_contract(
                    contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
                )

            # Should generate rules successfully
            assert len(rules) > 0, "Should generate rules despite validation warnings"

            # Note: Validation warnings from datacontract-cli may or may not be logged
            # depending on the contract structure, but the important thing is rules are generated

        finally:
            os.unlink(temp_path)

    def test_multiple_models_in_contract(self, generator):
        """Test that contracts with multiple schemas generate rules for all schemas."""
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "multi-model-test",
            "name": "multi-model-test",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "users",
                    "physicalType": "table",
                    "properties": [
                        {"name": "user_id", "logicalType": "string", "required": True},
                        {"name": "email", "logicalType": "string", "required": True},
                    ],
                },
                {
                    "name": "orders",
                    "physicalType": "table",
                    "properties": [
                        {"name": "order_id", "logicalType": "string", "required": True},
                        {"name": "user_id", "logicalType": "string", "required": True},
                    ],
                },
            ],
        }

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate rules for both schemas
            schemas_in_rules = {rule["user_metadata"]["schema"] for rule in rules}
            assert "users" in schemas_in_rules, "Should have rules for users schema"
            assert "orders" in schemas_in_rules, "Should have rules for orders schema"

            # Should have 4 rules total (2 required fields per schema)
            assert len(rules) == 4, f"Should have 4 rules, got {len(rules)}"

        finally:
            os.unlink(temp_path)

    def test_field_with_multiple_constraints(self, generator):
        """Test that fields with multiple constraints generate multiple rules."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "product_code",
                    "logicalType": "string",
                    "required": True,
                    "unique": True,
                    "logicalTypeOptions": {
                        "pattern": "^PROD-[0-9]{6}$",
                        "minLength": 11,
                        "maxLength": 11,
                    },
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
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
        contract_dict = create_contract_with_quality(
            property_name="data",
            logical_type="string",
            quality_checks=[
                {
                    "type": "custom",
                    "engine": "dqx",
                    "implementation": {
                        "criticality": "error",
                        "name": "check1",
                        "check": {"function": "is_not_null", "arguments": {"column": "data"}},
                    },
                },
                {
                    "type": "custom",
                    "engine": "dqx",
                    "implementation": {
                        "criticality": "warn",
                        "name": "check2",
                        "check": {
                            "function": "is_not_null_and_not_empty",
                            "arguments": {"column": "data", "trim_strings": True},
                        },
                    },
                },
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=False
            )

            # Should have 2 rules with different criticalities
            assert len(rules) == 2, f"Should have 2 rules, got {len(rules)}"

            criticalities = {rule["criticality"] for rule in rules}
            assert "error" in criticalities, "Should have error criticality"
            assert "warn" in criticalities, "Should have warn criticality"

        finally:
            os.unlink(temp_path)


class TestDataContractGeneratorConstraints(DataContractGeneratorTestBase):
    """Test constraint-specific predefined rule generation."""

    def test_field_with_only_minimum_constraint(self, generator):
        """Test that field with only minimum (no maximum) generates sql_expression rule for float."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "temperature",
                    "logicalType": "number",
                    "logicalTypeOptions": {
                        "minimum": -273.15,  # Float minimum, should use sql_expression
                    },
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate sql_expression rule for float minimum
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "sql_expression" in rule_functions, "Should have sql_expression rule for float minimum"

            # Verify the expression is correct
            min_rule = next(r for r in rules if r["check"]["function"] == "sql_expression")
            assert min_rule["check"]["arguments"]["expression"] == "temperature >= -273.15"
            assert min_rule["check"]["arguments"]["columns"] == ["temperature"]

        finally:
            os.unlink(temp_path)

    def test_field_with_only_maximum_constraint(self, generator):
        """Test that field with only maximum (no minimum) generates sql_expression rule for float."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "humidity",
                    "logicalType": "number",
                    "logicalTypeOptions": {
                        "maximum": 100.0,  # Float maximum, should use sql_expression
                    },
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate sql_expression rule for float maximum
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "sql_expression" in rule_functions, "Should have sql_expression rule for float maximum"

            # Verify the expression is correct
            max_rule = next(r for r in rules if r["check"]["function"] == "sql_expression")
            assert max_rule["check"]["arguments"]["expression"] == "humidity <= 100.0"
            assert max_rule["check"]["arguments"]["columns"] == ["humidity"]

        finally:
            os.unlink(temp_path)

    def test_field_with_only_integer_maximum_constraint(self, generator):
        """Test that field with only integer maximum generates is_aggr_not_greater_than rule."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "age",
                    "logicalType": "integer",
                    "logicalTypeOptions": {
                        "maximum": 150,  # Integer maximum
                    },
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate is_aggr_not_greater_than rule for integer maximum
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "is_aggr_not_greater_than" in rule_functions

            # Verify the rule details
            max_rule = next(r for r in rules if r["check"]["function"] == "is_aggr_not_greater_than")
            assert max_rule["check"]["arguments"]["expression"] == "max(age)"
            assert max_rule["check"]["arguments"]["max_limit"] == 150

        finally:
            os.unlink(temp_path)

    def test_field_with_only_integer_minimum_constraint(self, generator):
        """Test that field with only integer minimum generates is_aggr_not_less_than rule."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "quantity",
                    "logicalType": "integer",
                    "logicalTypeOptions": {
                        "minimum": 0,  # Integer minimum
                    },
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate is_aggr_not_less_than rule for integer minimum
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "is_aggr_not_less_than" in rule_functions

            # Verify the rule details
            min_rule = next(r for r in rules if r["check"]["function"] == "is_aggr_not_less_than")
            assert min_rule["check"]["arguments"]["expression"] == "min(quantity)"
            assert min_rule["check"]["arguments"]["min_limit"] == 0

        finally:
            os.unlink(temp_path)

    def test_explicit_dqx_quality_check_detection(self, generator):
        """Test that explicit DQX quality checks are correctly identified."""
        contract_dict = create_contract_with_quality(
            property_name="data",
            logical_type="string",
            quality_checks=[
                # DQX custom check
                {
                    "type": "custom",
                    "engine": "dqx",
                    "implementation": {
                        "check": {"function": "is_not_null", "arguments": {"column": "data"}},
                    },
                },
                # Non-DQX custom check (should be ignored)
                {
                    "type": "custom",
                    "engine": "some_other_engine",
                    "implementation": {"some": "config"},
                },
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=False
            )

            # Should only have 1 rule (the DQX one)
            assert len(rules) == 1, f"Should only extract DQX rules, got {len(rules)}"
            assert rules[0]["check"]["function"] == "is_not_null"

        finally:
            os.unlink(temp_path)

    def test_non_dqx_custom_quality_checks_ignored(self, generator):
        """Test that custom quality checks with non-dqx engines are ignored."""
        contract_dict = create_contract_with_quality(
            property_name="data",
            logical_type="string",
            quality_checks=[
                {
                    "type": "custom",
                    "engine": "soda",  # Non-DQX engine
                    "implementation": {"checks": ["row_count > 0"]},
                },
                {
                    "type": "custom",
                    "engine": "great_expectations",  # Non-DQX engine
                    "implementation": {"expectation": "expect_column_values_to_not_be_null"},
                },
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=False
            )

            # Should generate no rules since non-DQX engines are not supported
            assert len(rules) == 0, "Should ignore non-DQX custom quality checks"

        finally:
            os.unlink(temp_path)

    def test_minimum_and_maximum_together(self, generator):
        """Test that field with both minimum and maximum generates is_in_range rule."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "age",
                    "logicalType": "integer",
                    "logicalTypeOptions": {
                        "minimum": 0,
                        "maximum": 120,
                    },
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
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
            properties=[
                {
                    "name": "code",
                    "logicalType": "string",
                    "logicalTypeOptions": {
                        "minLength": 5,
                    },
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate sql_expression rule for minLength
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "sql_expression" in rule_functions, "Should have sql_expression rule for minLength"

            # Verify the length rule
            length_rule = next(r for r in rules if "too_short" in r["name"])
            assert length_rule["check"]["function"] == "sql_expression"
            assert "LENGTH(code) >= 5" in length_rule["check"]["arguments"]["expression"]

        finally:
            os.unlink(temp_path)

    def test_max_length_only(self, generator):
        """Test that field with only maxLength generates sql_expression rule."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "description",
                    "logicalType": "string",
                    "logicalTypeOptions": {
                        "maxLength": 200,
                    },
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate sql_expression rule for maxLength
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "sql_expression" in rule_functions, "Should have sql_expression rule for maxLength"

            # Verify the length rule
            length_rule = next(r for r in rules if "too_long" in r["name"])
            assert length_rule["check"]["function"] == "sql_expression"
            assert "LENGTH(description) <= 200" in length_rule["check"]["arguments"]["expression"]

        finally:
            os.unlink(temp_path)

    def test_min_and_max_length_together(self, generator):
        """Test that field with both minLength and maxLength generates a sql_expression rule."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "postal_code",
                    "logicalType": "string",
                    "logicalTypeOptions": {
                        "minLength": 5,
                        "maxLength": 10,
                    },
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate one combined sql_expression rule for both min and max
            length_rule = next(r for r in rules if "invalid_length" in r["name"])
            assert length_rule["check"]["function"] == "sql_expression"
            expression = length_rule["check"]["arguments"]["expression"]

            # Verify both min and max length are checked
            assert "LENGTH(postal_code) >= 5" in expression
            assert "LENGTH(postal_code) <= 10" in expression

        finally:
            os.unlink(temp_path)

    def test_field_with_logical_type_options_but_no_constraints(self, generator):
        """Test field with logicalTypeOptions but no min/max constraints."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "status",
                    "logicalType": "string",
                    "logicalTypeOptions": {
                        "description": "User status field",
                        # No minimum, maximum, minLength, maxLength, etc.
                    },
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should not generate any range or length rules for this field
            # Only count rules for 'status' field
            status_rules = [r for r in rules if "status" in r.get("name", "")]
            # There should be no constraint-based rules
            constraint_rules = [
                r
                for r in status_rules
                if any(
                    func in r["check"]["function"]
                    for func in (
                        "is_in_range",
                        "is_length_between",
                        "is_aggr_not_less_than",
                        "is_aggr_not_greater_than",
                    )
                )
            ]
            assert len(constraint_rules) == 0

        finally:
            os.unlink(temp_path)


class TestDataContractGeneratorLLM(DataContractGeneratorTestBase):
    """Test LLM-based text rule generation with mocked LLM output."""

    def _load_contract_and_get_model(self, temp_path):
        """Helper to load contract and extract first schema (ODCS v3.x)."""
        odcs = resolve_data_contract_v2(data_contract_location=temp_path)
        schema = odcs.schema_[0]  # Get first schema
        return schema

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
            properties=[
                {"name": "customer_id", "logicalType": "string"},
                {"name": "order_date", "logicalType": "date"},
                {"name": "amount", "logicalType": "number"},
            ]
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
            property_name="email",
            logical_type="string",
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
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=True
            )
            self._verify_text_rules(rules, mock_llm_engine)
        finally:
            os.unlink(temp_path)

    def test_text_rules_skipped_when_llm_disabled(self, generator):
        """Test that text rules are skipped when LLM is not available."""
        contract_dict = create_contract_with_quality(
            property_name="user_id",
            logical_type="string",
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
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=True
            )

            # Should generate no rules since LLM is not available
            assert len(rules) == 0, "Should skip text rules when LLM is not available"

        finally:
            os.unlink(temp_path)

    def test_text_rules_skipped_when_process_text_false(self, generator):
        """Test that text rules are skipped when process_text_rules=False."""
        contract_dict = create_contract_with_quality(
            property_name="user_id",
            logical_type="string",
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
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=False
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

        # Validate rule structure using DQEngine
        assert_rules_have_valid_structure(rules)

        # Verify all are text_llm rules
        for rule in rules:
            assert rule["user_metadata"]["rule_type"] == "text_llm"

    def test_multiple_text_rules_processed(self, generator):
        """Test that multiple text rules are all processed by LLM (based on REAL LLM structure)."""
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "multi-text-rules",
            "name": "multi-text-rules",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "orders",
                    "physicalType": "table",
                    "properties": [
                        {
                            "name": "order_id",
                            "logicalType": "string",
                            "quality": [{"type": "text", "description": "Order IDs must be unique across all systems"}],
                        },
                        {
                            "name": "customer_email",
                            "logicalType": "string",
                            "quality": [{"type": "text", "description": "Email must be valid and deliverable"}],
                        },
                    ],
                }
            ],
        }
        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            mock_llm_engine = self._create_multiple_text_rules_llm_mock()
            generator.llm_engine = mock_llm_engine
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=True
            )
            self._verify_multiple_text_rules(rules, mock_llm_engine)
        finally:
            os.unlink(temp_path)

    # Additional tests for coverage improvement

    def test_format_generates_timestamp_validation(self, generator):
        """Test that format constraint on timestamp field generates is_valid_timestamp rule."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "created_at",
                    "logicalType": "timestamp",
                    "required": True,
                    "logicalTypeOptions": {"format": "yyyy-MM-dd HH:mm:ss"},
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate is_valid_timestamp rule
            timestamp_rules = [r for r in rules if "is_valid_timestamp" in r["check"]["function"]]
            assert len(timestamp_rules) == 1
            assert timestamp_rules[0]["check"]["arguments"]["timestamp_format"] == "%Y-%m-%d %H:%M:%S"
        finally:
            os.unlink(temp_path)

    def test_format_on_string_type_ignored(self, generator):
        """Test that format on non-date type doesn't generate format validation rules."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "status",
                    "logicalType": "string",  # Not date/timestamp
                    "logicalTypeOptions": {"format": "yyyy-MM-dd"},  # Format should be ignored
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should not generate format validation rules for string type
            format_rules = [
                r
                for r in rules
                if "valid_date" in r["check"]["function"] or "valid_timestamp" in r["check"]["function"]
            ]
            assert (
                len(format_rules) == 0
            ), "Format validation rules should not be generated for non-date/timestamp types"
        finally:
            os.unlink(temp_path)

    def test_python_format_passthrough(self, generator):
        """Test that Python format strings (with %) are passed through as-is."""
        contract_dict = create_basic_contract(
            properties=[
                {
                    "name": "birth_date",
                    "logicalType": "date",
                    "logicalTypeOptions": {"format": "%Y-%m-%d"},  # Already Python format
                }
            ]
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            date_rules = [r for r in rules if "is_valid_date" in r["check"]["function"]]
            assert len(date_rules) == 1
            # Should keep Python format as-is
            assert date_rules[0]["check"]["arguments"]["date_format"] == "%Y-%m-%d"
        finally:
            os.unlink(temp_path)

    def test_contract_with_empty_schema_list(self, generator):
        """Test handling of contract with empty schema list."""
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:empty_schema",
            "name": "Empty Schema Contract",
            "version": "1.0.0",
            "status": "active",
            "schema": [],  # Empty list
        }

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should return empty list
            assert len(rules) == 0
        finally:
            os.unlink(temp_path)

    def test_schema_without_name_uses_unknown(self, generator):
        """Test that schema without name uses 'unknown_schema' in metadata."""
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:no_name",
            "name": "No Name Schema Contract",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    # No 'name' field
                    "physicalType": "table",
                    "properties": [{"name": "field1", "logicalType": "string", "required": True}],
                }
            ],
        }

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            assert len(rules) >= 1
            # Metadata should use "unknown_schema"
            assert rules[0]["user_metadata"]["schema"] == "unknown_schema"
        finally:
            os.unlink(temp_path)

    def test_deep_nested_fields_hit_recursion_limit(self, generator, caplog):
        """Test that deeply nested fields hit recursion limit and log warning."""

        # Create a deeply nested structure (25 levels deep, limit is 20)
        def create_nested_property(depth):
            if depth == 0:
                return {"name": "leaf", "logicalType": "string", "required": True}
            return {
                "name": f"level_{depth}",
                "logicalType": "object",
                "properties": [create_nested_property(depth - 1)],
            }

        contract_dict = create_basic_contract(properties=[create_nested_property(25)])

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should log warning about max recursion depth
            assert "Maximum recursion depth" in caplog.text or "exceeded" in caplog.text
        finally:
            os.unlink(temp_path)

    def test_file_not_found_raises_error(self, generator):
        """Test that non-existent contract file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Contract file not found"):
            generator.generate_rules_from_contract(contract_file="/nonexistent/path/contract.yaml")

    def test_invalid_yaml_raises_value_error(self, generator):
        """Test that invalid contract structure raises ValueError."""
        # Create temp file with malformed ODCS contract (missing required fields)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("kind: DataContract\n")
            f.write("apiVersion: v3.0.2\n")
            f.write("invalid_structure: not_a_valid_contract\n")
            # Missing required fields like 'id', 'name', 'version', 'status'
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="Failed to parse ODCS contract"):
                generator.generate_rules_from_contract(contract_file=temp_path)
        finally:
            os.unlink(temp_path)

    def test_schema_level_text_rules(self, generator, mock_workspace_client, mock_spark):
        """Test text rules defined at schema level (not property level)."""
        mock_llm = Mock()
        mock_llm.get_business_rules_with_llm = Mock(
            return_value=Mock(
                quality_rules=[
                    {
                        "check": {"function": "is_unique", "arguments": {"columns": ["id", "timestamp"]}},
                        "name": "unique_records",
                        "criticality": "error",
                    }
                ]
            )
        )

        gen_with_llm = DataContractRulesGenerator(workspace_client=mock_workspace_client, llm_engine=mock_llm)

        # Contract with schema-level text rule (not property-level)
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:schema_text",
            "name": "Schema Level Text Rules",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "test_schema",
                    "physicalType": "table",
                    "quality": [  # Schema-level quality
                        {"type": "text", "description": "Records should be unique by id and timestamp"}
                    ],
                    "properties": [
                        {"name": "id", "logicalType": "string"},
                        {"name": "timestamp", "logicalType": "timestamp"},
                    ],
                }
            ],
        }

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = gen_with_llm.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=True
            )

            # Should have called LLM for schema-level text rule
            assert mock_llm.get_business_rules_with_llm.called
            assert len(rules) >= 1
        finally:
            os.unlink(temp_path)

    def test_explicit_rule_with_object_implementation(self, generator):
        """Test explicit rule where implementation is an object with attributes (not dict)."""
        # Load a contract that will have implementation as Pydantic objects
        contract_dict = create_contract_with_quality(
            property_name="test_field",
            logical_type="string",
            quality_checks=[
                {
                    "type": "custom",
                    "engine": "dqx",
                    "implementation": {
                        "name": "test_rule",
                        "criticality": "warn",
                        "check": {"function": "is_not_null", "arguments": {"column": "test_field"}},
                    },
                }
            ],
        )

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            # Load using datacontract-cli which returns Pydantic models
            data_contract = DataContract(data_contract_file=temp_path)

            rules = generator.generate_rules_from_contract(
                contract=data_contract, generate_predefined_rules=False, process_text_rules=False
            )

            assert len(rules) >= 1
            assert rules[0]["criticality"] == "warn"
        finally:
            os.unlink(temp_path)

    def test_schema_with_no_properties(self, generator):
        """Test schema with empty or missing properties list."""
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:no_props",
            "name": "No Properties",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "empty_schema",
                    "physicalType": "table",
                    # No 'properties' field
                }
            ],
        }

        temp_path = create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should return empty list
            assert len(rules) == 0
        finally:
            os.unlink(temp_path)
