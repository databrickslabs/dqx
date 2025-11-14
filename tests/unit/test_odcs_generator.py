"""
Unit tests for ODCS data contract integration.
"""

import json
from unittest.mock import MagicMock, Mock

import pytest

from databricks.labs.dqx.datacontract import ODCSContract, ODCSProperty
from databricks.labs.dqx.profiler.generator import DQGenerator


class TestODCSProperty:
    """Test ODCSProperty dataclass."""

    def test_odcs_property_basic(self):
        """Test basic ODCSProperty creation."""
        prop = ODCSProperty(
            name="test_column",
            logical_type="string",
            physical_type="VARCHAR(50)",
            description="Test column",
            required=True,
        )
        assert prop.name == "test_column"
        assert prop.logical_type == "string"
        assert prop.required is True
        assert prop.unique is False

    def test_odcs_property_with_quality_constraints(self):
        """Test ODCSProperty with quality constraints."""
        prop = ODCSProperty(
            name="email",
            logical_type="string",
            required=True,
            not_null=True,
            not_empty=True,
            pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        )
        assert prop.name == "email"
        assert prop.not_null is True
        assert prop.not_empty is True
        assert prop.pattern is not None

    def test_odcs_property_with_range(self):
        """Test ODCSProperty with min/max values."""
        prop = ODCSProperty(name="age", logical_type="integer", min_value=0, max_value=120)
        assert prop.min_value == 0
        assert prop.max_value == 120

    def test_odcs_property_with_valid_values(self):
        """Test ODCSProperty with valid values enum."""
        prop = ODCSProperty(name="status", logical_type="string", valid_values=["active", "inactive", "pending"])
        assert len(prop.valid_values) == 3
        assert "active" in prop.valid_values


class TestODCSContract:
    """Test ODCSContract dataclass and parsing."""

    def test_odcs_contract_basic(self):
        """Test basic ODCSContract creation."""
        contract = ODCSContract(
            api_version="v3.0.2",
            name="test_contract",
            version="1.0.0",
            properties=[],
        )
        assert contract.name == "test_contract"
        assert contract.version == "1.0.0"
        assert len(contract.properties) == 0

    def test_parse_odcs_contract_minimal(self):
        """Test parsing minimal ODCS contract."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "name": "minimal_contract",
            "version": "1.0.0",
        }
        contract = ODCSContract.from_dict(contract_dict)
        assert contract.name == "minimal_contract"
        assert contract.version == "1.0.0"
        assert contract.api_version == "v3.0.2"

    def test_parse_odcs_contract_with_properties(self):
        """Test parsing ODCS contract with schema properties."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "name": "test_contract",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "user_id": {"logicalType": "string", "required": True, "unique": True},
                    "email": {
                        "logicalType": "string",
                        "required": True,
                        "quality": {"notNull": True, "notEmpty": True},
                    },
                }
            },
        }
        contract = ODCSContract.from_dict(contract_dict)
        assert len(contract.properties) == 2
        assert contract.properties[0].name == "user_id"
        assert contract.properties[0].required is True
        assert contract.properties[0].unique is True
        assert contract.properties[1].name == "email"
        assert contract.properties[1].not_null is True

    def test_parse_odcs_contract_with_columns(self):
        """Test parsing ODCS contract using 'columns' format."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "name": "table_contract",
            "version": "1.0.0",
            "schema": {
                "type": "table",
                "columns": {
                    "col1": {"logicalType": "integer", "required": True},
                    "col2": {"logicalType": "string", "pattern": "^[A-Z]{3}$"},
                },
            },
        }
        contract = ODCSContract.from_dict(contract_dict)
        assert len(contract.properties) == 2
        assert contract.properties[0].name == "col1"
        assert contract.properties[1].pattern == "^[A-Z]{3}$"

    def test_parse_property_with_range(self):
        """Test parsing property with min/max values."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "age": {"logicalType": "integer", "minValue": 0, "maxValue": 120},
                    "price": {
                        "logicalType": "numeric",
                        "quality": {"minValue": 0.01, "maxValue": 10000.0},
                    },
                }
            },
        }
        contract = ODCSContract.from_dict(contract_dict)
        assert contract.properties[0].min_value == 0
        assert contract.properties[0].max_value == 120
        assert contract.properties[1].min_value == 0.01

    def test_parse_property_with_valid_values(self):
        """Test parsing property with valid values."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "status": {"logicalType": "string", "validValues": ["active", "inactive"]},
                }
            },
        }
        contract = ODCSContract.from_dict(contract_dict)
        assert contract.properties[0].valid_values == ["active", "inactive"]


class TestDQGeneratorODCS:
    """Test DQGenerator ODCS contract integration."""

    @pytest.fixture
    def mock_workspace_client(self):
        """Create mock WorkspaceClient."""
        mock_ws = Mock()
        mock_ws.config._product_info = ("dqx", "0.0.0")
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
    def generator_with_llm(self, mock_workspace_client, mock_spark):
        """Create DQGenerator instance with mocked LLM."""
        gen = DQGenerator(workspace_client=mock_workspace_client, spark=mock_spark)
        gen.llm_engine = MagicMock()
        return gen

    def test_generate_rules_unsupported_format(self, generator):
        """Test error for unsupported contract format."""
        with pytest.raises(ValueError, match="Contract format 'unknown' not supported"):
            generator.generate_rules_from_contract(contract={}, format="unknown")

    def test_generate_rules_minimal_contract(self, generator):
        """Test generating rules from minimal contract."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "name": "minimal",
            "version": "1.0.0",
            "schema": {},
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert isinstance(rules, list)
        assert len(rules) == 0

    def test_generate_implicit_rule_required(self, generator):
        """Test generating implicit rule for required field."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "name": "test",
            "version": "1.0.0",
            "schema": {"properties": {"user_id": {"logicalType": "string", "required": True}}},
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict, generate_implicit_rules=True)
        assert len(rules) == 1
        assert rules[0]["check"]["function"] == "is_not_null"
        assert rules[0]["check"]["arguments"]["column"] == "user_id"
        assert rules[0]["criticality"] == "error"
        assert rules[0]["user_metadata"]["odcs_dimension"] == "completeness"
        assert rules[0]["user_metadata"]["odcs_rule_type"] == "implicit"

    def test_generate_implicit_rule_not_empty(self, generator):
        """Test generating implicit rule for not empty string."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "email": {
                        "logicalType": "string",
                        "required": True,
                        "quality": {"notNull": True, "notEmpty": True},
                    }
                }
            },
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert len(rules) == 1
        assert rules[0]["check"]["function"] == "is_not_null_and_not_empty"
        assert rules[0]["check"]["arguments"]["trim_strings"] is True

    def test_generate_implicit_rule_valid_values(self, generator):
        """Test generating implicit rule for valid values."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {"status": {"logicalType": "string", "validValues": ["active", "inactive", "pending"]}}
            },
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert len(rules) == 1
        assert rules[0]["check"]["function"] == "is_in_list"
        assert rules[0]["check"]["arguments"]["allowed"] == ["active", "inactive", "pending"]
        assert rules[0]["user_metadata"]["odcs_dimension"] == "validity"

    def test_generate_implicit_rule_pattern(self, generator):
        """Test generating implicit rule for regex pattern."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {"properties": {"code": {"logicalType": "string", "pattern": "^[A-Z]{3}-[0-9]{4}$"}}},
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert len(rules) == 1
        assert rules[0]["check"]["function"] == "regex_match"
        assert rules[0]["check"]["arguments"]["regex"] == "^[A-Z]{3}-[0-9]{4}$"

    def test_generate_implicit_rule_range_both(self, generator):
        """Test generating implicit rule for min and max values."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {"properties": {"age": {"logicalType": "integer", "minValue": 0, "maxValue": 120}}},
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert len(rules) == 1
        assert rules[0]["check"]["function"] == "is_in_range"
        assert rules[0]["check"]["arguments"]["min_limit"] == 0
        assert rules[0]["check"]["arguments"]["max_limit"] == 120

    def test_generate_implicit_rule_min_only(self, generator):
        """Test generating implicit rule for min value only."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {"properties": {"price": {"logicalType": "numeric", "minValue": 0.01}}},
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert len(rules) == 1
        assert rules[0]["check"]["function"] == "is_not_less_than"
        assert rules[0]["check"]["arguments"]["limit"] == 0.01

    def test_generate_implicit_rule_max_only(self, generator):
        """Test generating implicit rule for max value only."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {"properties": {"discount": {"logicalType": "numeric", "maxValue": 100.0}}},
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert len(rules) == 1
        assert rules[0]["check"]["function"] == "is_not_greater_than"
        assert rules[0]["check"]["arguments"]["limit"] == 100.0

    def test_generate_implicit_rule_unique(self, generator):
        """Test generating implicit rule for unique constraint."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {"properties": {"user_id": {"logicalType": "string", "unique": True}}},
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert len(rules) == 1
        assert rules[0]["check"]["function"] == "is_unique"
        assert rules[0]["user_metadata"]["odcs_dimension"] == "uniqueness"

    def test_generate_implicit_rule_date_format(self, generator):
        """Test generating implicit rule for date format."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {"properties": {"birth_date": {"logicalType": "date", "format": "yyyy-MM-dd", "required": True}}},
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert len(rules) == 2  # is_not_null + is_valid_date
        date_rule = [r for r in rules if r["check"]["function"] == "is_valid_date"][0]
        assert date_rule["check"]["arguments"]["date_format"] == "yyyy-MM-dd"

    def test_generate_implicit_rule_timestamp_format(self, generator):
        """Test generating implicit rule for timestamp format."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "created_at": {"logicalType": "timestamp", "format": "yyyy-MM-dd HH:mm:ss", "required": True}
                }
            },
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        timestamp_rule = [r for r in rules if r["check"]["function"] == "is_valid_timestamp"][0]
        assert timestamp_rule["check"]["arguments"]["timestamp_format"] == "yyyy-MM-dd HH:mm:ss"

    def test_generate_multiple_implicit_rules(self, generator):
        """Test generating multiple implicit rules for one property."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "email": {
                        "logicalType": "string",
                        "required": True,
                        "pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                        "quality": {"notNull": True, "notEmpty": True},
                    }
                }
            },
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert len(rules) == 2  # notNull+notEmpty + pattern
        functions = [r["check"]["function"] for r in rules]
        assert "is_not_null_and_not_empty" in functions
        assert "regex_match" in functions

    def test_criticality_mapping(self, generator):
        """Test custom criticality mapping for dimensions."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "user_id": {"required": True},
                    "email": {"pattern": "^.+@.+$"},
                }
            },
        }
        rules = generator.generate_rules_from_contract(
            contract=contract_dict, criticality_mapping={"completeness": "error", "validity": "warn"}
        )
        completeness_rule = [r for r in rules if r["user_metadata"]["odcs_dimension"] == "completeness"][0]
        validity_rule = [r for r in rules if r["user_metadata"]["odcs_dimension"] == "validity"][0]
        assert completeness_rule["criticality"] == "error"
        assert validity_rule["criticality"] == "warn"

    def test_skip_implicit_rules(self, generator):
        """Test skipping implicit rule generation."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {"properties": {"user_id": {"required": True}}},
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict, generate_implicit_rules=False)
        assert len(rules) == 0

    def test_process_text_rules_without_llm(self, generator):
        """Test text rules are skipped when LLM is unavailable."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "description": {
                        "logicalType": "string",
                        "quality": {"text": "Description should be professional and clear"},
                    }
                }
            },
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict, process_text_rules=True)
        # Should not fail, just skip text rules
        assert isinstance(rules, list)

    def test_process_text_rules_with_llm(self, generator_with_llm):
        """Test text rules are processed with LLM."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "description": {
                        "logicalType": "string",
                        "quality": {"text": "Description should not be empty"},
                    }
                }
            },
        }

        # Mock LLM response
        mock_prediction = Mock()
        mock_prediction.quality_rules = json.dumps(
            [
                {
                    "check": {"function": "is_not_null_and_not_empty", "arguments": {"column": "description"}},
                    "criticality": "error",
                }
            ]
        )
        generator_with_llm.llm_engine.get_business_rules_with_llm.return_value = mock_prediction

        rules = generator_with_llm.generate_rules_from_contract(
            contract=contract_dict, generate_implicit_rules=False, process_text_rules=True
        )

        assert len(rules) == 1
        assert rules[0]["user_metadata"]["odcs_rule_type"] == "text_llm"
        assert "odcs_text_expectation" in rules[0]["user_metadata"]

    def test_process_explicit_dqx_rules(self, generator):
        """Test processing explicit DQX format rules."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "test",
            "version": "1.0.0",
            "schema": {
                "properties": {
                    "delivery_date": {
                        "logicalType": "date",
                        "quality": {
                            "custom": {
                                "criticality": "warn",
                                "check": {
                                    "function": "sql_expression",
                                    "arguments": {
                                        "expression": "delivery_date >= order_date",
                                        "msg": "Delivery date must be after order date",
                                    },
                                },
                            }
                        },
                    }
                }
            },
        }
        rules = generator.generate_rules_from_contract(
            contract=contract_dict, generate_implicit_rules=False, process_text_rules=False
        )
        assert len(rules) == 1
        assert rules[0]["check"]["function"] == "sql_expression"
        assert rules[0]["criticality"] == "warn"
        assert rules[0]["user_metadata"]["odcs_rule_type"] == "explicit"

    def test_contract_metadata_in_rules(self, generator):
        """Test that contract metadata is included in generated rules."""
        contract_dict = {
            "apiVersion": "v3.0.2",
            "name": "customer_data",
            "version": "2.5.0",
            "domain": "sales",
            "dataProduct": "analytics",
            "schema": {"properties": {"customer_id": {"required": True}}},
        }
        rules = generator.generate_rules_from_contract(contract=contract_dict)
        assert len(rules) == 1
        metadata = rules[0]["user_metadata"]
        assert metadata["odcs_contract_name"] == "customer_data"
        assert metadata["odcs_contract_version"] == "2.5.0"
        assert metadata["odcs_property"] == "customer_id"

    def test_invalid_contract_raises_error(self, generator):
        """Test that invalid contract raises ValueError."""
        contract_dict = {"invalid": "contract"}
        with pytest.raises(ValueError, match="Failed to parse ODCS contract"):
            generator.generate_rules_from_contract(contract=contract_dict)
