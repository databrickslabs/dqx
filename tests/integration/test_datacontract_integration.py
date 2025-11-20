import os
import copy
import pytest
import pyspark.sql.functions as F
from datacontract.data_contract import DataContract

from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.check_funcs import make_condition, register_rule


class TestDataContractIntegration:
    """Integration tests for data contract processing."""

    @pytest.fixture
    def sample_contract_path(self):
        """Path to sample data contract."""
        tests_dir = os.path.dirname(os.path.dirname(__file__))
        return os.path.join(tests_dir, "resources", "sample_datacontract.yaml")

    @pytest.fixture
    def expected_contract_rules(self):
        """
        Expected DQX rules generated from sample data contract.
        Must match the rules generated from resources/sample_datacontract.yaml.
        """
        return [
            {
                'check': {'function': 'is_not_null', 'arguments': {'column': 'sensor_id'}},
                'name': 'sensor_id_is_null',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'sensor_id',
                    'dimension': 'completeness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {'function': 'is_unique', 'arguments': {'columns': ['sensor_id']}},
                'name': 'sensor_id_not_unique',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'sensor_id',
                    'dimension': 'uniqueness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'regex_match',
                    'arguments': {'column': 'sensor_id', 'regex': '^SENSOR-[A-Z]{2}-[0-9]{4}$'},
                },
                'name': 'sensor_id_invalid_pattern',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'sensor_id',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'sql_expression',
                    'arguments': {'expression': 'LENGTH(sensor_id) = 15', 'columns': ['sensor_id']},
                },
                'name': 'sensor_id_invalid_length',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'sensor_id',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {'function': 'is_not_null', 'arguments': {'column': 'machine_id'}},
                'name': 'machine_id_is_null',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'machine_id',
                    'dimension': 'completeness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'regex_match',
                    'arguments': {'column': 'machine_id', 'regex': '^MACHINE-[A-Z0-9]{6}$'},
                },
                'name': 'machine_id_invalid_pattern',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'machine_id',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {'function': 'is_not_null', 'arguments': {'column': 'reading_timestamp'}},
                'name': 'reading_timestamp_is_null',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'reading_timestamp',
                    'dimension': 'completeness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'is_valid_date',
                    'arguments': {'column': 'reading_timestamp', 'date_format': '%Y-%m-%d %H:%M:%S'},
                },
                'name': 'reading_timestamp_valid_date_format',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'reading_timestamp',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {'function': 'is_not_null', 'arguments': {'column': 'calibration_date'}},
                'name': 'calibration_date_is_null',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'calibration_date',
                    'dimension': 'completeness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'is_valid_date',
                    'arguments': {'column': 'calibration_date', 'date_format': '%Y-%m-%d'},
                },
                'name': 'calibration_date_valid_date_format',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'calibration_date',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {'function': 'is_not_null', 'arguments': {'column': 'temperature_celsius'}},
                'name': 'temperature_celsius_is_null',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'temperature_celsius',
                    'dimension': 'completeness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'sql_expression',
                    'arguments': {
                        'expression': 'temperature_celsius >= -273.15 AND temperature_celsius <= 200.0',
                        'columns': ['temperature_celsius'],
                    },
                },
                'name': 'temperature_celsius_out_of_range',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'temperature_celsius',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {'function': 'is_not_null', 'arguments': {'column': 'humidity_percentage'}},
                'name': 'humidity_percentage_is_null',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'humidity_percentage',
                    'dimension': 'completeness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'sql_expression',
                    'arguments': {
                        'expression': 'humidity_percentage >= 0.0 AND humidity_percentage <= 100.0',
                        'columns': ['humidity_percentage'],
                    },
                },
                'name': 'humidity_percentage_out_of_range',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'humidity_percentage',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'sql_expression',
                    'arguments': {
                        'expression': 'pressure_bar >= 0.1 AND pressure_bar <= 10.0',
                        'columns': ['pressure_bar'],
                    },
                },
                'name': 'pressure_bar_out_of_range',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'pressure_bar',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {'function': 'is_not_null', 'arguments': {'column': 'vibration_level'}},
                'name': 'vibration_level_is_null',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'vibration_level',
                    'dimension': 'completeness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'is_in_range',
                    'arguments': {'column': 'vibration_level', 'min_limit': 0, 'max_limit': 10},
                },
                'name': 'vibration_level_out_of_range',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'vibration_level',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {'function': 'is_not_null', 'arguments': {'column': 'sensor_status'}},
                'name': 'sensor_status_is_null',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'sensor_status',
                    'dimension': 'completeness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'regex_match',
                    'arguments': {'column': 'sensor_status', 'regex': '^(active|inactive|maintenance|faulty)$'},
                },
                'name': 'sensor_status_invalid_pattern',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'sensor_status',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'regex_match',
                    'arguments': {'column': 'alert_level', 'regex': '^(none|low|medium|high|critical)$'},
                },
                'name': 'alert_level_invalid_pattern',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'alert_level',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {'function': 'is_not_null', 'arguments': {'column': 'location'}},
                'name': 'location_is_null',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'location',
                    'dimension': 'completeness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'regex_match',
                    'arguments': {'column': 'location', 'regex': '^[A-Z]{3}-[A-Z]{2}-[0-9]{3}$'},
                },
                'name': 'location_invalid_pattern',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'location',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'sql_expression',
                    'arguments': {'expression': 'LENGTH(location) = 10', 'columns': ['location']},
                },
                'name': 'location_invalid_length',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'location',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {'function': 'is_not_null', 'arguments': {'column': 'device_model'}},
                'name': 'device_model_is_null',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'device_model',
                    'dimension': 'completeness',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'regex_match',
                    'arguments': {'column': 'device_model', 'regex': '^[A-Z]{2}[0-9]{4}-[A-Z]{1}$'},
                },
                'name': 'device_model_invalid_pattern',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'device_model',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'sql_expression',
                    'arguments': {'expression': 'LENGTH(notes) <= 500', 'columns': ['notes']},
                },
                'name': 'notes_too_long',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'notes',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'regex_match',
                    'arguments': {'column': 'technician_id', 'regex': '^TECH-[0-9]{5}$'},
                },
                'name': 'technician_id_invalid_pattern',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'technician_id',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'regex_match',
                    'arguments': {
                        'column': 'alert_email',
                        'regex': '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$',
                    },
                },
                'name': 'alert_email_invalid_pattern',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'field': 'alert_email',
                    'dimension': 'validity',
                    'rule_type': 'predefined',
                },
            },
            {
                'check': {
                    'function': 'is_not_null_and_not_empty',
                    'arguments': {'column': 'sensor_id', 'trim_strings': True},
                },
                'name': 'sensor_id_not_empty',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'rule_type': 'explicit',
                    'field': 'sensor_id',
                },
            },
            {
                'check': {
                    'function': 'is_valid_timestamp',
                    'arguments': {'column': 'reading_timestamp', 'timestamp_format': '%Y-%m-%d %H:%M:%S'},
                },
                'name': 'valid_reading_timestamp',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'rule_type': 'explicit',
                    'field': 'reading_timestamp',
                },
            },
            {
                'check': {
                    'function': 'sql_expression',
                    'arguments': {'expression': 'calibration_date <= date(reading_timestamp)'},
                },
                'name': 'calibration_date_logical',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'rule_type': 'explicit',
                    'field': 'calibration_date',
                },
            },
            {
                'check': {
                    'function': 'is_in_list',
                    'arguments': {
                        'column': 'sensor_status',
                        'allowed': ['active', 'inactive', 'maintenance', 'faulty'],
                    },
                },
                'name': 'sensor_status_valid_values',
                'criticality': 'warn',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'rule_type': 'explicit',
                    'field': 'sensor_status',
                },
            },
            {
                'check': {
                    'function': 'regex_match',
                    'arguments': {
                        'column': 'alert_email',
                        'regex': '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$',
                    },
                },
                'name': 'valid_alert_email',
                'criticality': 'warn',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'rule_type': 'explicit',
                    'field': 'alert_email',
                },
            },
            {
                'check': {
                    'function': 'is_data_fresh_per_time_window',
                    'arguments': {
                        'column': 'reading_timestamp',
                        'window_minutes': 60,
                        'min_records_per_window': 1,
                        'lookback_windows': 24,
                    },
                },
                'name': 'sensor_data_freshness',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'rule_type': 'explicit',
                },
            },
            {
                'check': {
                    'function': 'is_aggr_not_less_than',
                    'arguments': {'column': 'sensor_id', 'limit': 1, 'aggr_type': 'count'},
                },
                'name': 'dataset_not_empty',
                'criticality': 'warn',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'rule_type': 'explicit',
                },
            },
        ]

    def test_generate_rules_from_contract_file(self, ws, spark, sample_contract_path, expected_contract_rules):
        """Test generating rules from data contract file."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        actual_rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=False
        )
        assert actual_rules == expected_contract_rules

    def test_generate_rules_from_datacontract_object_and_path(
        self, ws, spark, sample_contract_path, expected_contract_rules
    ):
        """Test generating rules from DataContract object - should produce same output as from file."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        contract = DataContract(data_contract_file=sample_contract_path)
        rules_from_object = generator.generate_rules_from_contract(contract=contract, process_text_rules=False)
        assert rules_from_object == expected_contract_rules

    def test_generate_rules_with_default_criticality(self, ws, spark, sample_contract_path, expected_contract_rules):
        """Test that default_criticality is correctly applied."""
        generator = DQGenerator(workspace_client=ws, spark=spark)
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, default_criticality="warn", process_text_rules=False
        )

        expected_contract_rules_warn = copy.deepcopy(expected_contract_rules)
        for rule in expected_contract_rules_warn:
            # exclude explicit rules that have criticality set to error in the contract
            if rule["name"] not in {
                "sensor_id_not_empty",
                "valid_reading_timestamp",
                "calibration_date_logical",
                "sensor_data_freshness",
            }:
                rule["criticality"] = "warn"
        assert rules == expected_contract_rules_warn

    def test_generate_rules_and_skip_predefined_rules(self, ws, spark, sample_contract_path):
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

    def test_generate_rules_with_text_processing(self, ws, spark, sample_contract_path):
        """Test generating rules with process_text_rules=True for LLM-based rule generation."""
        generator = DQGenerator(workspace_client=ws, spark=spark)

        # Generate rules with text processing enabled
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=True
        )

        # Verify rules were generated
        assert len(rules) > 35  # More than predefined + explicit rules due to text-based rules

        # Verify that text-based rules were processed by LLM
        # The sample contract has a text expectation about duplicate sensor readings
        text_llm_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "text_llm"]
        assert len(text_llm_rules) > 0, "Expected at least one text_llm rule from text expectations"

        # Verify that we still have predefined and explicit rules too
        predefined_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "predefined"]
        explicit_rules = [r for r in rules if r["user_metadata"]["rule_type"] == "explicit"]
        assert len(predefined_rules) > 0, "Expected predefined rules"
        assert len(explicit_rules) > 0, "Expected explicit DQX rules"

    def test_generate_rules_from_datacontract_obj_with_custom_check_func(self, ws, spark, sample_contract_path):
        """Test generating rules with custom check functions."""

        @register_rule("row")
        def not_ends_with_suffix(column: str, suffix: str):
            """
            Example of custom python row-level check function.
            """
            return make_condition(
                F.col(column).endswith(suffix), f"Column {column} ends with {suffix}", f"{column}_ends_with_{suffix}"
            )

        custom_check_functions = {"not_ends_with_suffix": not_ends_with_suffix}

        generator = DQGenerator(workspace_client=ws, spark=spark, custom_check_functions=custom_check_functions)

        data_contract_str = """
        kind: DataContract
        apiVersion: v3.0.2
        id: urn:datacontract:sensors:iot_sensor_data
        name: IoT Sensor Data Quality Contract
        version: 2.1.0
        status: active
        
        schema:
          - name: sensor_readings
            properties:
              - name: sensor_id
                quality:
                  # Explicit check with error criticality
                  - type: custom
                    engine: dqx
                    description: Sensor ID must not be empty
                    implementation:
                      criticality: error
                      name: sensor_id_not_empty
                      check:
                        function: is_not_null_and_not_empty
                        arguments:
                          column: sensor_id
                          trim_strings: true
                  - type: custom
                    engine: dqx
                    description: Sensor name must not end with '_test'
                    implementation:
                      criticality: error
                      name: sensor_name_not_end_with_test
                      check:
                        function: not_ends_with_suffix
                        arguments:
                          column: sensor_name
                          suffix: _test"""

        contract = DataContract(data_contract_str=data_contract_str)

        # Generate rules with text processing enabled
        rules = generator.generate_rules_from_contract(
            contract=contract, generate_predefined_rules=False, process_text_rules=False
        )

        # Verify rules were generated
        expected_rules = [
            {
                "check": {
                    "function": "is_not_null_and_not_empty",
                    "arguments": {"column": "sensor_id", "trim_strings": True},
                },
                "name": "sensor_id_not_empty",
                "criticality": "error",
                "user_metadata": {
                    "contract_id": "urn:datacontract:sensors:iot_sensor_data",
                    "contract_version": "2.1.0",
                    "odcs_version": "v3.0.2",
                    "schema": "sensor_readings",
                    "rule_type": "explicit",
                    "field": "sensor_id",
                },
            },
            {
                "check": {
                    "function": "not_ends_with_suffix",
                    "arguments": {"column": "sensor_name", "suffix": "_test"},
                },
                "name": "sensor_name_not_end_with_test",
                "criticality": "error",
                "user_metadata": {
                    "contract_id": "urn:datacontract:sensors:iot_sensor_data",
                    "contract_version": "2.1.0",
                    "odcs_version": "v3.0.2",
                    "schema": "sensor_readings",
                    "rule_type": "explicit",
                    "field": "sensor_id",
                },
            },
        ]

        assert rules == expected_rules, f"Rules do not match expected: {rules}"
