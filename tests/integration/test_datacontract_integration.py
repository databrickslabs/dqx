"""
Integration tests for data contract (ODCS) rule generation.

Covers all code paths through DQGenerator.generate_rules_from_contract and the
underlying DataContractRulesGenerator: contract loading (file, object, YAML string),
rule generation flags (schema validation, predefined, text rules), default_criticality,
multiple schemas, recursive type validation, DECIMAL validation, and error paths
(ParameterError, NotFound, ODCSContractError, InvalidPhysicalTypeError).
"""

import logging
import os
import tempfile
from typing import Any

import pytest
import yaml
from datacontract.data_contract import DataContract
from pyspark.sql import types as spark_types

from databricks.sdk.errors import NotFound
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidPhysicalTypeError, ODCSContractError, ParameterError
from databricks.labs.dqx.profiler.generator import DQGenerator
from tests.conftest import get_schema_validation_rules


def _generate_rules_from_temp_contract(
    workspace_client: Any,
    spark: Any,
    contract: dict[str, Any],
    **kwargs: Any,
) -> list[dict]:
    """Write contract to a temp YAML file, run generator, return rules. Cleans up the file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(contract, f)
        path = f.name
    try:
        generator = DQGenerator(workspace_client=workspace_client, spark=spark)
        return generator.generate_rules_from_contract(contract_file=path, **kwargs)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def _temp_contract_path_with_content(content: str) -> str:
    """Write raw content to a temp YAML file and return its path. Caller must os.unlink(path)."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(content)
        return f.name


# Minimal valid ODCS v3.x contract (single schema, one property) for contract-from-string path
MINIMAL_ODCS_YAML = """
kind: DataContract
apiVersion: v3.0.2
id: test:minimal
name: Minimal Contract
version: 1.0.0
status: active
schema:
  - name: minimal_schema
    physicalType: table
    properties:
      - name: id
        physicalType: STRING
        logicalType: string
        required: true
"""

# Two schemas for multiple-schema path
MULTI_SCHEMA_ODCS = {
    "kind": "DataContract",
    "apiVersion": "v3.0.2",
    "id": "test:multi",
    "name": "Multi Schema Contract",
    "version": "1.0.0",
    "status": "active",
    "schema": [
        {
            "name": "schema_a",
            "physicalType": "table",
            "properties": [
                {"name": "id_a", "physicalType": "STRING", "logicalType": "string", "required": True},
            ],
        },
        {
            "name": "schema_b",
            "physicalType": "table",
            "properties": [
                {"name": "id_b", "physicalType": "STRING", "logicalType": "string", "required": True},
            ],
        },
    ],
}


class TestDataContractIntegration:
    """Integration tests for data contract processing."""

    @pytest.fixture
    def sample_contract_path(self):
        """Path to sample data contract."""
        tests_dir = os.path.dirname(os.path.dirname(__file__))
        return os.path.join(tests_dir, "resources", "sample_datacontract.yaml")

    @pytest.fixture
    def multi_schema_contract_path(self):
        """Path to a temporary contract file with two schemas."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.safe_dump(MULTI_SCHEMA_ODCS, f)
            path = f.name
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    def test_generate_rules_with_text_processing(self, ws, spark, sample_contract_path):
        """Full path: contract_file, predefined + text rules + schema validation; validates checks."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=True
        )
        assert len(rules) > 36
        text_llm_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "text_llm"]
        assert len(text_llm_rules) > 0
        predefined_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "predefined"]
        explicit_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "explicit"]
        assert len(predefined_rules) > 0
        assert len(explicit_rules) > 0
        status = DQEngine.validate_checks(rules)
        assert not status.has_errors, f"Generated rules have validation errors: {status.errors}"

    def test_generate_rules_includes_schema_validation(self, ws, spark, sample_contract_path):
        """Full path: contract_file, schema validation on; one schema_validation rule, validate_checks."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path,
            generate_predefined_rules=True,
            process_text_rules=False,
        )
        schema_validation_rules = get_schema_validation_rules(rules)
        assert len(schema_validation_rules) >= 1
        assert schema_validation_rules[0]["check"]["arguments"].get("strict") is True
        assert "expected_schema" in schema_validation_rules[0]["check"]["arguments"]
        status = DQEngine.validate_checks(rules)
        assert not status.has_errors, f"Generated rules have validation errors: {status.errors}"

    def test_generate_rules_from_contract_object_from_file_path(self, ws, spark, sample_contract_path):
        """Path: load from DataContract(data_contract_file=...); same output as contract_file."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        contract = DataContract(data_contract_file=sample_contract_path)
        rules_from_object = generator.generate_rules_from_contract(
            contract=contract, generate_predefined_rules=True, process_text_rules=False
        )
        rules_from_file = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=False
        )
        assert len(rules_from_object) == len(rules_from_file)
        status = DQEngine.validate_checks(rules_from_object)
        assert not status.has_errors, f"Generated rules have validation errors: {status.errors}"

    def test_generate_rules_from_contract_object_from_yaml_string(self, ws, spark):
        """Path: load from DataContract(data_contract_str=...); ODCS from string, validate_checks."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        contract = DataContract(data_contract_str=MINIMAL_ODCS_YAML)
        rules = generator.generate_rules_from_contract(
            contract=contract,
            generate_predefined_rules=True,
            process_text_rules=False,
        )
        schema_rules = get_schema_validation_rules(rules)
        assert len(schema_rules) == 1
        assert schema_rules[0]["user_metadata"]["schema"] == "minimal_schema"
        status = DQEngine.validate_checks(rules)
        assert not status.has_errors, f"Generated rules have validation errors: {status.errors}"

    def test_generate_rules_predefined_disabled(self, ws, spark, sample_contract_path):
        """Path: generate_predefined_rules=False; schema_validation + explicit only, validate_checks."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path,
            generate_predefined_rules=False,
            process_text_rules=False,
        )
        predefined_rules = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "predefined"]
        assert len(predefined_rules) == 0
        schema_rules = get_schema_validation_rules(rules)
        assert len(schema_rules) >= 1
        status = DQEngine.validate_checks(rules)
        assert not status.has_errors, f"Generated rules have validation errors: {status.errors}"

    def test_generate_rules_default_criticality_warn(self, ws, spark, sample_contract_path):
        """Path: default_criticality='warn'; at least one rule has criticality warn, validate_checks."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path,
            generate_predefined_rules=True,
            process_text_rules=False,
            default_criticality="warn",
        )
        warn_rules = [r for r in rules if r.get("criticality") == "warn"]
        assert len(warn_rules) >= 1
        status = DQEngine.validate_checks(rules)
        assert not status.has_errors, f"Generated rules have validation errors: {status.errors}"

    def test_generate_rules_multiple_schemas(self, ws, spark, multi_schema_contract_path):
        """Path: contract with two schemas; two schema_validation rules, one per schema, validate_checks."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract_file=multi_schema_contract_path,
            generate_predefined_rules=True,
            process_text_rules=False,
        )
        schema_validation_rules = get_schema_validation_rules(rules)
        assert len(schema_validation_rules) == 2
        names = {r["name"] for r in schema_validation_rules}
        assert "schema_a_schema_validation" in names
        assert "schema_b_schema_validation" in names
        status = DQEngine.validate_checks(rules)
        assert not status.has_errors, f"Generated rules have validation errors: {status.errors}"

    def test_schema_validation_expected_schema_parseable_by_spark(self, ws, spark, sample_contract_path):
        """Generated expected_schema DDL string is parseable by Spark StructType.fromDDL (required at runtime)."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path,
            generate_predefined_rules=False,
            process_text_rules=False,
        )
        schema_validation_rules = get_schema_validation_rules(rules)
        assert len(schema_validation_rules) >= 1
        ddl = schema_validation_rules[0]["check"]["arguments"]["expected_schema"]
        parsed = spark_types.StructType.fromDDL(ddl)
        assert isinstance(parsed, spark_types.StructType)
        assert len(parsed.fields) > 0

    def test_generate_rules_complex_nested_types_valid(self, ws, spark):
        """Contract with nested ARRAY, MAP, STRUCT generates valid rules and DDL parseable by Spark."""
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:complex",
            "name": "Complex Types",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "nested_schema",
                    "physicalType": "table",
                    "properties": [
                        {"name": "id", "physicalType": "STRING", "required": True},
                        {"name": "tags", "physicalType": "ARRAY<ARRAY<INT>>"},
                        {"name": "meta", "physicalType": "MAP<STRING, ARRAY<INT>>"},
                        {"name": "nested", "physicalType": "STRUCT<a:INT, b:ARRAY<STRING>>"},
                    ],
                },
            ],
        }
        rules = _generate_rules_from_temp_contract(
            ws,
            spark,
            contract,
            generate_predefined_rules=True,
            process_text_rules=False,
        )
        status = DQEngine.validate_checks(rules)
        assert not status.has_errors, f"Generated rules have validation errors: {status.errors}"
        schema_rules = get_schema_validation_rules(rules)
        assert len(schema_rules) == 1
        ddl = schema_rules[0]["check"]["arguments"]["expected_schema"]
        assert "tags ARRAY<ARRAY<INT>>" in ddl or "ARRAY<ARRAY<INT>>" in ddl
        assert "MAP<STRING,ARRAY<INT>>" in ddl or "MAP<STRING, ARRAY<INT>>" in ddl
        assert "STRUCT<" in ddl and "a:INT" in ddl and "b:ARRAY<STRING>" in ddl
        parsed = spark_types.StructType.fromDDL(ddl)
        assert isinstance(parsed, spark_types.StructType)

    def test_generate_rules_decimal_within_spark_limit(self, ws, spark):
        """Contract with DECIMAL(38,10) (Spark max precision) generates valid rules."""
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:decimal",
            "name": "Decimal",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "dec_schema",
                    "physicalType": "table",
                    "properties": [
                        {"name": "id", "physicalType": "STRING"},
                        {"name": "amount", "physicalType": "DECIMAL(38,10)"},
                    ],
                },
            ],
        }
        rules = _generate_rules_from_temp_contract(
            ws,
            spark,
            contract,
            generate_predefined_rules=False,
            process_text_rules=False,
        )
        schema_rules = get_schema_validation_rules(rules)
        assert len(schema_rules) == 1
        ddl = schema_rules[0]["check"]["arguments"]["expected_schema"]
        assert "DECIMAL(38,10)" in ddl
        status = DQEngine.validate_checks(rules)
        assert not status.has_errors

    def test_generate_rules_geography_interval_types_valid(self, ws, spark):
        """Contract with GEOGRAPHY or INTERVAL physicalType generates rules and DDL contains type (prefix branch)."""
        for physical_type in ("GEOGRAPHY(Point)", "INTERVAL DAY"):
            contract = {
                "kind": "DataContract",
                "apiVersion": "v3.0.2",
                "id": "test:geo",
                "name": "Geo",
                "version": "1.0.0",
                "status": "active",
                "schema": [
                    {
                        "name": "geo_schema",
                        "physicalType": "table",
                        "properties": [
                            {"name": "id", "physicalType": "STRING"},
                            {"name": "geom", "physicalType": physical_type},
                        ],
                    },
                ],
            }
            rules = _generate_rules_from_temp_contract(
                ws,
                spark,
                contract,
                generate_predefined_rules=False,
                process_text_rules=False,
            )
            schema_rules = get_schema_validation_rules(rules)
            assert len(schema_rules) == 1
            ddl = schema_rules[0]["check"]["arguments"]["expected_schema"]
            assert physical_type.upper() in ddl or physical_type in ddl
            status = DQEngine.validate_checks(rules)
            assert not status.has_errors

    def test_schema_with_empty_properties_no_crash(self, ws, spark, caplog):
        """Schema with properties: [] produces no schema_validation rule for that schema; no exception."""
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:empty_props",
            "name": "Empty Props",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "empty_schema",
                    "physicalType": "table",
                    "properties": [],
                },
            ],
        }
        with caplog.at_level(logging.WARNING):
            rules = _generate_rules_from_temp_contract(
                ws,
                spark,
                contract,
                generate_predefined_rules=False,
                process_text_rules=False,
            )
        schema_rules = get_schema_validation_rules(rules)
        assert len(schema_rules) == 0
        assert "no flat properties" in caplog.text or "skipping schema validation" in caplog.text.lower()

    def test_nameless_property_warning_in_schema_info(self, ws, spark, caplog):
        """Schema with nameless property and text rule triggers warning when building schema info for LLM."""
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:nameless",
            "name": "Nameless",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "mixed_schema",
                    "physicalType": "table",
                    "properties": [
                        {"name": "id", "physicalType": "STRING", "logicalType": "string"},
                        {"name": "", "physicalType": "STRING", "logicalType": "string"},
                        {
                            "name": "data",
                            "physicalType": "STRING",
                            "logicalType": "string",
                            "quality": [
                                {"type": "text", "description": "Data must be non-empty"},
                            ],
                        },
                    ],
                },
            ],
        }
        with caplog.at_level(logging.WARNING):
            _generate_rules_from_temp_contract(
                ws,
                spark,
                contract,
                generate_predefined_rules=False,
                process_text_rules=True,
            )
        assert "Skipping property without name in schema" in caplog.text

    def test_error_invalid_inner_physical_type_raises(self, ws, spark):
        """Contract with invalid inner type (e.g. ARRAY<NOT_A_TYPE>) raises InvalidPhysicalTypeError."""
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:invalid",
            "name": "Invalid",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "s",
                    "physicalType": "table",
                    "properties": [
                        {"name": "id", "physicalType": "STRING"},
                        {"name": "bad", "physicalType": "ARRAY<NOT_A_VALID_TYPE>"},
                    ],
                },
            ],
        }
        with pytest.raises(InvalidPhysicalTypeError) as exc_info:
            _generate_rules_from_temp_contract(
                ws,
                spark,
                contract,
                generate_predefined_rules=False,
                process_text_rules=False,
            )
        assert "NOT_A_VALID_TYPE" in str(exc_info.value) or "not a valid" in str(exc_info.value).lower()

    def test_error_decimal_precision_over_38_raises(self, ws, spark):
        """Contract with DECIMAL(100,2) raises InvalidPhysicalTypeError (Spark limit 38)."""
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:decimal_invalid",
            "name": "Invalid Decimal",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "s",
                    "physicalType": "table",
                    "properties": [
                        {"name": "id", "physicalType": "STRING"},
                        {"name": "col", "physicalType": "DECIMAL(100,2)"},
                    ],
                },
            ],
        }
        with pytest.raises(InvalidPhysicalTypeError) as exc_info:
            _generate_rules_from_temp_contract(
                ws,
                spark,
                contract,
                generate_predefined_rules=False,
                process_text_rules=False,
            )
        assert "38" in str(exc_info.value) or "precision" in str(exc_info.value).lower()

    def test_error_decimal_scale_over_precision_raises(self, ws, spark):
        """Contract with DECIMAL(10,50) (scale > precision) raises InvalidPhysicalTypeError."""
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:decimal_scale",
            "name": "Invalid Scale",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "s",
                    "physicalType": "table",
                    "properties": [
                        {"name": "id", "physicalType": "STRING"},
                        {"name": "col", "physicalType": "DECIMAL(10,50)"},
                    ],
                },
            ],
        }
        with pytest.raises(InvalidPhysicalTypeError) as exc_info:
            _generate_rules_from_temp_contract(
                ws,
                spark,
                contract,
                generate_predefined_rules=False,
                process_text_rules=False,
            )
        assert "scale" in str(exc_info.value).lower() or "precision" in str(exc_info.value).lower()


class TestDataContractIntegrationErrors:
    """Integration tests for data contract error paths and invalid inputs."""

    @pytest.fixture
    def sample_contract_path(self):
        """Path to sample data contract."""
        tests_dir = os.path.dirname(os.path.dirname(__file__))
        return os.path.join(tests_dir, "resources", "sample_datacontract.yaml")

    def test_error_neither_contract_nor_file(self, ws, spark):
        """Error path: neither contract nor contract_file; raises ParameterError."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        with pytest.raises(ParameterError, match="Either .*contract.*contract_file.*must be provided"):
            generator.generate_rules_from_contract()

    def test_error_contract_object_without_path_or_data(self, ws, spark):
        """Error path: DataContract() with no file path, data_contract, or data_contract_str; ParameterError."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        empty_contract = DataContract()
        with pytest.raises(ParameterError, match="DataContract object must have either"):
            generator.generate_rules_from_contract(contract=empty_contract)

    def test_error_both_contract_and_file(self, ws, spark, sample_contract_path):
        """Error path: both contract and contract_file; raises ParameterError."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        contract = DataContract(data_contract_file=sample_contract_path)
        with pytest.raises(ParameterError, match="Cannot provide both"):
            generator.generate_rules_from_contract(contract=contract, contract_file=sample_contract_path)

    def test_error_unsupported_contract_format(self, ws, spark, sample_contract_path):
        """Error path: contract_format != 'odcs'; raises ParameterError."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        with pytest.raises(ParameterError, match="not supported"):
            generator.generate_rules_from_contract(contract_file=sample_contract_path, contract_format="unknown")

    def test_error_contract_file_not_found(self, ws, spark):
        """Error path: contract_file path does not exist; raises NotFound."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        with pytest.raises(NotFound, match="Contract file not found"):
            generator.generate_rules_from_contract(contract_file="/nonexistent/contract.yaml")

    def test_error_invalid_contract_raises_odcs_error(self, ws, spark):
        """Error path: malformed ODCS (missing required fields); raises ODCSContractError."""
        path = _temp_contract_path_with_content(
            "kind: DataContract\napiVersion: v3.0.2\ninvalid_structure: missing_required_fields\n"
        )
        try:
            generator = DQGenerator(workspace_client=ws, spark=spark)
            with pytest.raises(ODCSContractError, match="Failed to parse ODCS contract"):
                generator.generate_rules_from_contract(contract_file=path)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    def _contract_with_physical_type(self, physical_type: str) -> dict[str, Any]:
        """Minimal ODCS contract with one schema and one property with given physicalType."""
        return {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:pt",
            "name": "PT",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "s",
                    "physicalType": "table",
                    "properties": [
                        {"name": "id", "physicalType": "STRING"},
                        {"name": "col", "physicalType": physical_type},
                    ],
                },
            ],
        }

    def test_error_unmatched_angle_brackets_raises(self, ws, spark):
        """Contract with physicalType ARRAY<INT (no closing >) raises InvalidPhysicalTypeError."""
        contract = self._contract_with_physical_type("ARRAY<INT")
        with pytest.raises(InvalidPhysicalTypeError) as exc_info:
            _generate_rules_from_temp_contract(
                ws, spark, contract, generate_predefined_rules=False, process_text_rules=False
            )
        msg = str(exc_info.value).lower()
        assert "unmatched" in msg or "bracket" in msg or "angle" in msg or "invalid" in msg

    def test_error_empty_array_raises(self, ws, spark):
        """Contract with physicalType ARRAY<> raises InvalidPhysicalTypeError."""
        contract = self._contract_with_physical_type("ARRAY<>")
        with pytest.raises(InvalidPhysicalTypeError) as exc_info:
            _generate_rules_from_temp_contract(
                ws, spark, contract, generate_predefined_rules=False, process_text_rules=False
            )
        assert "empty" in str(exc_info.value).lower() or "ARRAY" in str(exc_info.value)

    def test_error_empty_map_raises(self, ws, spark):
        """Contract with physicalType MAP<> raises InvalidPhysicalTypeError."""
        contract = self._contract_with_physical_type("MAP<>")
        with pytest.raises(InvalidPhysicalTypeError) as exc_info:
            _generate_rules_from_temp_contract(
                ws, spark, contract, generate_predefined_rules=False, process_text_rules=False
            )
        assert "empty" in str(exc_info.value).lower() or "MAP" in str(exc_info.value)

    def test_error_map_wrong_number_of_params_raises(self, ws, spark):
        """Contract with physicalType MAP<STRING> or MAP<INT,INT,INT> raises InvalidPhysicalTypeError."""
        for bad_type in ("MAP<STRING>", "MAP<INT,INT,INT>"):
            contract = self._contract_with_physical_type(bad_type)
            with pytest.raises(InvalidPhysicalTypeError) as exc_info:
                _generate_rules_from_temp_contract(
                    ws, spark, contract, generate_predefined_rules=False, process_text_rules=False
                )
            msg = str(exc_info.value).lower()
            assert "two" in msg or "exactly" in msg or "parameter" in msg

    def test_error_empty_struct_raises(self, ws, spark):
        """Contract with physicalType STRUCT<> raises InvalidPhysicalTypeError."""
        contract = self._contract_with_physical_type("STRUCT<>")
        with pytest.raises(InvalidPhysicalTypeError) as exc_info:
            _generate_rules_from_temp_contract(
                ws, spark, contract, generate_predefined_rules=False, process_text_rules=False
            )
        assert "empty" in str(exc_info.value).lower() or "STRUCT" in str(exc_info.value)

    def test_error_struct_field_without_colon_raises(self, ws, spark):
        """Contract with physicalType STRUCT<id INT> (no colon) raises InvalidPhysicalTypeError."""
        contract = self._contract_with_physical_type("STRUCT<id INT>")
        with pytest.raises(InvalidPhysicalTypeError) as exc_info:
            _generate_rules_from_temp_contract(
                ws, spark, contract, generate_predefined_rules=False, process_text_rules=False
            )
        msg = str(exc_info.value).lower()
        assert "name" in msg and ("type" in msg or ":" in msg or "colon" in msg)

    def test_error_struct_field_missing_name_or_type_raises(self, ws, spark):
        """Contract with physicalType STRUCT<:INT> or STRUCT<name:> raises InvalidPhysicalTypeError."""
        for bad_type in ("STRUCT<:INT>", "STRUCT<name:>"):
            contract = self._contract_with_physical_type(bad_type)
            with pytest.raises(InvalidPhysicalTypeError):
                _generate_rules_from_temp_contract(
                    ws, spark, contract, generate_predefined_rules=False, process_text_rules=False
                )

    def test_error_recursion_depth_exceeded_raises(self, ws, spark):
        """Contract with physicalType nested beyond max depth raises InvalidPhysicalTypeError."""
        nested = "ARRAY<" * 51 + "INT" + ">" * 51
        contract = self._contract_with_physical_type(nested)
        with pytest.raises(InvalidPhysicalTypeError) as exc_info:
            _generate_rules_from_temp_contract(
                ws, spark, contract, generate_predefined_rules=False, process_text_rules=False
            )
        msg = str(exc_info.value)
        assert "50" in msg or "depth" in msg.lower() or "maximum" in msg.lower()

    def test_error_empty_physical_type_raises(self, ws, spark):
        """Contract with physicalType '' or whitespace raises InvalidPhysicalTypeError."""
        for bad_type in ("", "   "):
            contract = self._contract_with_physical_type(bad_type)
            with pytest.raises(InvalidPhysicalTypeError) as exc_info:
                _generate_rules_from_temp_contract(
                    ws, spark, contract, generate_predefined_rules=False, process_text_rules=False
                )
            assert "physicalType" in str(exc_info.value) or "Unity" in str(exc_info.value)

    def test_error_unknown_physical_type_raises(self, ws, spark):
        """Contract with physicalType NOT_A_UC_TYPE raises InvalidPhysicalTypeError."""
        contract = self._contract_with_physical_type("NOT_A_UC_TYPE")
        with pytest.raises(InvalidPhysicalTypeError) as exc_info:
            _generate_rules_from_temp_contract(
                ws, spark, contract, generate_predefined_rules=False, process_text_rules=False
            )
        msg = str(exc_info.value).lower()
        assert "not a valid" in msg or "unity catalog" in msg or "NOT_A_UC_TYPE" in str(exc_info.value)
