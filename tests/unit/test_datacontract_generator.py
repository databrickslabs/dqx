import json
import logging
import os
import tempfile
from unittest.mock import Mock

import pytest
import pyspark.sql.functions as F
import yaml
from datacontract.data_contract import DataContract
from datacontract.lint.resolve import resolve_data_contract

from databricks.sdk.errors import NotFound
import databricks.labs.dqx.profiler.generator as generator_module
from databricks.labs.dqx.check_funcs import make_condition, register_rule
import databricks.labs.dqx.datacontract.contract_rules_generator as contract_rules_generator_module
from databricks.labs.dqx.datacontract.contract_rules_generator import DataContractRulesGenerator
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import (
    InvalidPhysicalTypeError,
    ODCSContractError,
    ParameterError,
    MissingParameterError,
)
from databricks.labs.dqx.profiler.generator import DQGenerator
from tests.conftest import get_schema_validation_rules


class DataContractGeneratorTestBase:
    """Base class with shared fixtures for data contract generator tests."""

    @pytest.fixture
    def sample_contract_path(self):
        """Path to sample data contract for testing."""
        tests_dir = os.path.dirname(os.path.dirname(__file__))
        return os.path.join(tests_dir, "resources", "sample_datacontract.yaml")

    def get_expected_contract_rules(self):
        """
        Expected DQX rules generated from sample data contract.
        Must match the rules generated from resources/sample_datacontract.yaml.
        """
        return [
            {
                'check': {
                    'function': 'has_valid_schema',
                    'arguments': {
                        'expected_schema': (
                            'sensor_id STRING, machine_id STRING, reading_timestamp TIMESTAMP, '
                            'calibration_date DATE, temperature_celsius DECIMAL(5,2), humidity_percentage DECIMAL(5,2), '
                            'pressure_bar DECIMAL(4,2), vibration_level INT, sensor_status STRING, alert_level STRING, '
                            'location STRING, device_model STRING, notes STRING, technician_id STRING, '
                            'is_anomaly BOOLEAN, alert_email STRING'
                        ),
                        'strict': True,
                    },
                },
                'name': 'sensor_readings_schema_validation',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'rule_type': 'schema_validation',
                },
            },
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
            {
                'check': {'function': 'is_not_null_and_not_empty', 'for_each_column': ['technician_id', 'alert_email']},
                'name': 'technician_and_email_are_mandatory_for_high_alerts',
                'criticality': 'warn',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'sensor_readings',
                    'rule_type': 'explicit',
                },
                'filter': 'alert_level in (\'high\', \'critical\')',
            },
            {
                'check': {
                    'function': 'has_valid_schema',
                    'arguments': {
                        'expected_schema': (
                            'col_string STRING, col_varchar STRING, col_char STRING, col_int INT, col_integer INT, '
                            'col_smallint SMALLINT, col_tinyint TINYINT, col_long BIGINT, col_bigint BIGINT, '
                            'col_float FLOAT, col_double DOUBLE, col_number DOUBLE, col_decimal DECIMAL(10,2), '
                            'col_decimal_precision DECIMAL(12,4), col_numeric DECIMAL(8,2), col_boolean BOOLEAN, '
                            'col_bool BOOLEAN, col_date DATE, col_timestamp TIMESTAMP, col_datetime TIMESTAMP, '
                            'col_binary BINARY, col_object BINARY, col_array ARRAY<STRING>'
                        ),
                        'strict': True,
                    },
                },
                'name': 'all_data_types_schema_validation',
                'criticality': 'error',
                'user_metadata': {
                    'contract_id': 'urn:datacontract:sensors:iot_sensor_data',
                    'contract_version': '2.1.0',
                    'odcs_version': 'v3.0.2',
                    'schema': 'all_data_types',
                    'rule_type': 'schema_validation',
                },
            },
        ]

    def create_basic_contract(
        self,
        schema_name: str = "test_table",
        properties: list[dict] | None = None,
        contract_id: str = "test",
        contract_version: str = "1.0.0",
    ) -> dict:
        """
        Create a basic ODCS v3.x contract dictionary with specified properties.

        Args:
            schema_name: Name of the schema.
            properties: List of ODCS v3.x property dictionaries. Each property should have:
                       - name: property name
                       - logicalType: data type (string, number, integer, etc.)
                       - required: boolean (optional)
                       - unique: boolean (optional)
                       - logicalTypeOptions: dict with pattern, minimum, maximum, etc. (optional)
                       - quality: list of quality checks (optional)
            contract_id: Contract ID.
            contract_version: Contract version.

        Returns:
            An ODCS v3.x contract dictionary.
        """
        if properties is None:
            properties = [{"name": "user_id", "physicalType": "STRING", "required": True}]

        return {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": contract_id,
            "name": contract_id,
            "version": contract_version,
            "status": "active",
            "schema": [{"name": schema_name, "physicalType": "table", "properties": properties}],
        }

    def create_contract_with_quality(
        self,
        property_name: str,
        logical_type: str,
        quality_checks: list[dict],
        schema_name: str = "test_table",
    ) -> dict:
        """
        Create an ODCS v3.x contract with quality checks for a single property.

        Args:
            property_name: Name of the property.
            logical_type: Logical type of the property (string, number, etc.).
            quality_checks: List of ODCS v3.x quality check dictionaries.
                           For DQX rules, use format:
                           {
                               "type": "custom",
                               "engine": "dqx",
                               "implementation": {
                                   "name": "rule_name",
                                   "criticality": "error|warn",
                                   "check": {"function": "...", "arguments": {...}}
                               }
                           }
            schema_name: Name of the schema.

        Returns:
            An ODCS v3.x contract dictionary with quality checks.
        """
        # All quality checks should already be in ODCS v3.x format. physicalType required for schema validation.
        property_def = {
            "name": property_name,
            "logicalType": logical_type,
            "physicalType": "STRING",
            "quality": quality_checks,
        }
        return self.create_basic_contract(schema_name=schema_name, properties=[property_def])

    def create_test_contract_file(
        self,
        user_id_pattern: str = "^USER-[0-9]{4}$",
        age_min: int = 0,
        age_max: int = 120,
        status_values: list[str] | None = None,
        custom_contract: dict | None = None,
    ) -> str:
        """
        Create a temporary ODCS v3.x contract file for testing.

        Args:
            user_id_pattern: Pattern for user_id property.
            age_min: Minimum age value.
            age_max: Maximum age value.
            status_values: List of valid status values.
            custom_contract: Optional custom contract dict to use instead of default.

        Returns:
            Path to the temporary contract file.
        """
        if custom_contract:
            contract_dict = custom_contract
        else:
            if status_values is None:
                status_values = ["active", "inactive"]

            # Create ODCS v3.x properties directly. physicalType required (Unity Catalog).
            properties: list[dict] = [
                {
                    "name": "user_id",
                    "physicalType": "STRING",
                    "logicalType": "string",
                    "required": True,
                    "logicalTypeOptions": {"pattern": user_id_pattern},
                },
                {
                    "name": "age",
                    "physicalType": "INT",
                    "logicalType": "integer",
                    "logicalTypeOptions": {"minimum": age_min, "maximum": age_max},
                },
                {
                    "name": "status",
                    "physicalType": "STRING",
                    "logicalType": "string",
                    "logicalTypeOptions": {"pattern": f"^({'|'.join(status_values)})$"},  # enum as pattern
                },
            ]

            contract_dict = self.create_basic_contract(schema_name="users", properties=properties)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.safe_dump(contract_dict, f)
            return f.name

    def _generate_rules_and_get_schema_ddl(
        self, generator, temp_path, generate_predefined_rules=True, process_text_rules=False
    ):
        """Generate rules from temp contract file and return (schema_rules, expected_schema_ddl)."""
        rules = generator.generate_rules_from_contract(
            contract_file=temp_path,
            generate_predefined_rules=generate_predefined_rules,
            process_text_rules=process_text_rules,
        )
        schema_rules = get_schema_validation_rules(rules)
        ddl = schema_rules[0]["check"]["arguments"]["expected_schema"] if schema_rules else None
        return schema_rules, ddl

    def _assert_unity_ddl_contains(self, ddl, expected_substrings):
        """Assert that each expected substring appears in the DDL string."""
        assert ddl is not None, "Expected non-empty DDL"
        for substring in expected_substrings:
            assert substring in ddl, f"Expected DDL to contain {substring!r}, got: {ddl!r}"

    def _assert_special_column_escapes_in_ddl(self, schema_rules, ddl):
        """Assert DDL backtick-escapes special/digit-starting column names (Databricks/ANSI)."""
        assert len(schema_rules) == 1
        assert ddl is not None
        assert "`col-name` STRING" in ddl
        assert "`col name` STRING" in ddl
        assert "`123col` STRING" in ddl, "Identifiers starting with digit must be backtick-escaped"
        assert "normal_col STRING" in ddl


class TestDataContractGeneratorBasic(DataContractGeneratorTestBase):
    """Test basic data contract generator functionality."""

    def test_import_error_without_datacontract_cli(self, generator):
        """Test that appropriate error is raised if datacontract-cli not installed."""
        # This test documents the expected behavior when neither contract nor file is provided
        # In dev env with datacontract-cli, it will raise ValueError
        with pytest.raises(ParameterError):
            generator.generate_rules_from_contract(contract_file=None, contract=None)

    def test_requires_either_contract_or_file(self, generator):
        """Test that either contract or contract_file must be provided."""
        with pytest.raises(ParameterError, match="Either 'contract' or 'contract_file' must be provided"):
            generator.generate_rules_from_contract()

    def test_requires_either_valid_contract_object(self, generator):
        """Test that either contract or contract_file must be provided."""
        with pytest.raises(
            ParameterError,
            match="DataContract object must have either a file path, data_contract dict, or data_contract_str attribute",
        ):
            generator.generate_rules_from_contract(DataContract())

    def test_cannot_provide_both_contract_and_file(self, generator, sample_contract_path):
        """Test that both contract and contract_file cannot be provided."""
        contract = DataContract(data_contract_file=sample_contract_path)
        with pytest.raises(ParameterError, match="Cannot provide both"):
            generator.generate_rules_from_contract(contract=contract, contract_file=sample_contract_path)

    def test_unsupported_contract_format(self, generator, sample_contract_path):
        """Test error for unsupported contract format."""
        with pytest.raises(ParameterError, match="Contract format 'unknown' not supported"):
            generator.generate_rules_from_contract(contract_file=sample_contract_path, contract_format="unknown")

    def test_init_raises_when_llm_engine_provided_but_llm_extras_missing(self, mock_workspace_client, monkeypatch):
        """Test that DataContractRulesGenerator raises ImportError when llm_engine given but LLM deps missing."""
        monkeypatch.setattr(contract_rules_generator_module, "missing_required_packages", lambda _: True)
        with pytest.raises(ImportError, match="pip install databricks-labs-dqx\\[llm\\]"):
            DataContractRulesGenerator(workspace_client=mock_workspace_client, llm_engine=Mock())

    def test_explicit_rule_with_criticality_warn(self, generator):
        """Test that explicit DQX rules preserve 'warn' criticality from contract."""
        contract_dict = self.create_contract_with_quality(
            property_name="email",
            logical_type="string",
            quality_checks=[
                {
                    "type": "custom",
                    "engine": "dqx",
                    "implementation": {
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

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=False,
            )

            explicit = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"]
            assert len(explicit) == 1
            assert explicit[0]["criticality"] == "warn"  # Should preserve 'warn' from contract
            assert explicit[0]["check"]["function"] == "regex_match"
        finally:
            os.unlink(temp_path)

    def test_explicit_rule_with_criticality_error(self, generator):
        """Test that explicit DQX rules preserve 'error' criticality from contract."""
        contract_dict = self.create_contract_with_quality(
            property_name="user_id",
            logical_type="string",
            quality_checks=[
                {
                    "type": "custom",
                    "engine": "dqx",
                    "implementation": {
                        "criticality": "error",  # Explicitly set to error
                        "check": {
                            "function": "is_not_null_and_not_empty",
                            "arguments": {"column": "user_id", "trim_strings": True},
                        },
                    },
                }
            ],
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=False,
            )

            explicit = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"]
            assert len(explicit) == 1
            assert explicit[0]["criticality"] == "error"  # Should preserve 'error' from contract
            assert explicit[0]["check"]["function"] == "is_not_null_and_not_empty"
        finally:
            os.unlink(temp_path)

    def test_explicit_rule_without_criticality_uses_dqx_default(self, generator):
        """Test that explicit DQX rules without criticality use DQX's default (error)."""
        contract_dict = self.create_contract_with_quality(
            property_name="user_id",
            logical_type="string",
            quality_checks=[
                {
                    "type": "custom",
                    "engine": "dqx",
                    "implementation": {
                        # No criticality specified
                        "check": {
                            "function": "is_not_null",
                            "arguments": {"column": "user_id"},
                        },
                    },
                }
            ],
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=False,
            )

            explicit = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"]
            assert len(explicit) == 1
            # When criticality is not specified in contract, it won't be in the rule
            # DQX engine will use its default 'error' when applying the rule
            assert "criticality" not in explicit[0] or explicit[0].get("criticality") == "error"
            assert explicit[0]["check"]["function"] == "is_not_null"
        finally:
            os.unlink(temp_path)

    def test_predefined_rules_use_default_criticality(self, generator):
        """Test that predefined rules use the default_criticality parameter."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "user_id", "physicalType": "STRING", "logicalType": "string", "required": True}]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            # Generate with warn as default
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=True,
                process_text_rules=False,
                default_criticality="warn",
            )

            predefined = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "predefined"]
            assert len(predefined) == 1
            assert predefined[0]["criticality"] == "warn"  # Predefined rule should use default_criticality
            assert predefined[0]["check"]["function"] == "is_not_null"
        finally:
            os.unlink(temp_path)

    def test_schema_validation_raises_when_physical_type_missing(self, generator):
        """When a property has no physicalType, InvalidPhysicalTypeError is raised."""
        # Build contract manually so "b" has no physicalType (create_basic_contract would inject it).
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:missing_pt",
            "name": "Missing PT",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "missing_pt",
                    "physicalType": "table",
                    "properties": [
                        {"name": "a", "physicalType": "STRING"},
                        {"name": "b", "logicalType": "integer"},
                    ],
                }
            ],
        }
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError) as exc_info:
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
            assert "physicalType is required" in str(exc_info.value)
            assert "missing_pt" in str(exc_info.value)
            assert "b" in str(exc_info.value)
        finally:
            os.unlink(temp_path)

    def test_schema_validation_raises_when_physical_type_invalid(self, generator):
        """When physicalType is not a valid Unity Catalog type, InvalidPhysicalTypeError is raised."""
        contract_dict = self.create_basic_contract(
            schema_name="invalid_pt",
            properties=[
                {"name": "a", "physicalType": "STRING"},
                {"name": "b", "physicalType": "varchar(100)"},  # ODCS-style, not Unity
            ],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError) as exc_info:
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
            assert "not a valid Unity Catalog" in str(exc_info.value)
            assert "invalid_pt" in str(exc_info.value)
            assert "b" in str(exc_info.value)
        finally:
            os.unlink(temp_path)

    def test_schema_validation_ddl_escapes_special_column_names(self, generator):
        """Test that column names with special chars or starting with digit are backtick-escaped (Databricks/ANSI)."""
        contract_dict = self.create_basic_contract(
            schema_name="special_cols",
            properties=[
                {"name": "col-name", "physicalType": "STRING"},
                {"name": "col name", "physicalType": "STRING"},
                {"name": "123col", "physicalType": "STRING"},
                {"name": "normal_col", "physicalType": "STRING"},
            ],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            schema_rules, ddl = self._generate_rules_and_get_schema_ddl(
                generator, temp_path, generate_predefined_rules=False
            )
            self._assert_special_column_escapes_in_ddl(schema_rules, ddl)
        finally:
            os.unlink(temp_path)

    def test_schema_with_only_nested_objects_includes_binary_column(self, generator):
        """Test that schema with only nested object properties produces one column as BINARY (Databricks byte sequence)."""
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:nested_only",
            "name": "Nested Only",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "nested_schema",
                    "physicalType": "table",
                    "properties": [
                        {
                            "name": "payload",
                            "physicalType": "BINARY",
                            "logicalType": "object",
                            "properties": [
                                {"name": "nested_field", "physicalType": "STRING", "logicalType": "string"},
                            ],
                        },
                    ],
                },
            ],
        }
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=False,
            )
            schema_rules = get_schema_validation_rules(rules)
            assert len(schema_rules) == 1, "Nested object is represented as one BINARY column"
            ddl = schema_rules[0]["check"]["arguments"]["expected_schema"]
            assert "payload BINARY" in ddl
        finally:
            os.unlink(temp_path)

    def test_property_with_no_physical_type_raises(self, generator):
        """Test that property with no physicalType raises InvalidPhysicalTypeError."""
        contract_dict = self.create_basic_contract(
            schema_name="no_type",
            properties=[
                {"name": "untyped_col"},
            ],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError) as exc_info:
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
            assert "physicalType is required" in str(exc_info.value)
            assert "untyped_col" in str(exc_info.value)
        finally:
            os.unlink(temp_path)

    def test_generate_rules_from_contract_file(self, generator, sample_contract_path):
        """Test generating rules from data contract file."""
        actual_rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=True, process_text_rules=False
        )
        assert actual_rules == self.get_expected_contract_rules()

    def test_generate_rules_from_datacontract_object_and_path(self, generator, sample_contract_path):
        """Test generating rules from DataContract object - should produce same output as from file."""
        contract = DataContract(data_contract_file=sample_contract_path)
        rules_from_object = generator.generate_rules_from_contract(contract=contract, process_text_rules=False)
        assert rules_from_object == self.get_expected_contract_rules()

    def test_generate_rules_with_default_criticality(self, generator, sample_contract_path):
        """Test that default_criticality is correctly applied."""
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, default_criticality="warn", process_text_rules=False
        )

        expected_contract_rules_warn = self.get_expected_contract_rules()
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

    def test_generate_rules_and_skip_predefined_rules(self, generator, sample_contract_path):
        """Test that generate_predefined_rules=False skips predefined rules."""
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path,
            generate_predefined_rules=False,
            process_text_rules=False,
        )

        # Should have explicit rules plus schema validation (no predefined rules)
        # Sample ODCS v3.x contract has 2 schemas (sensor_readings, all_data_types) + 8 explicit DQX rules
        schema_rules = get_schema_validation_rules(rules)
        explicit_rules = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"]
        assert len(schema_rules) == 2
        assert len(explicit_rules) == 8
        assert len(rules) == 10

    def test_generate_rules_from_datacontract_obj_with_custom_check_func(
        self, mock_workspace_client, mock_spark, sample_contract_path
    ):
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

        generator = DQGenerator(
            workspace_client=mock_workspace_client, spark=mock_spark, custom_check_functions=custom_check_functions
        )

        data_contract_str = """
        kind: DataContract
        apiVersion: v3.0.2
        id: urn:datacontract:sensors:iot_sensor_data
        name: IoT Sensor Data Quality Contract
        version: 2.1.0
        status: active

        schema:
          - name: sensor_readings
            physicalType: table
            properties:
              - name: sensor_id
                physicalType: STRING
                logicalType: string
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
                          suffix: _test
              - name: sensor_name
                physicalType: STRING
                logicalType: string"""

        contract = DataContract(data_contract_str=data_contract_str)

        # Generate rules with text processing enabled
        rules = generator.generate_rules_from_contract(
            contract=contract,
            generate_predefined_rules=False,
            process_text_rules=False,
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

        explicit_rules = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"]
        assert len(explicit_rules) == len(expected_rules), f"Explicit rules count mismatch: {explicit_rules}"
        assert len(get_schema_validation_rules(rules)) == 1, "Expected one schema_validation rule"
        for exp in expected_rules:
            match = next(
                (r for r in explicit_rules if r.get("name") == exp.get("name") and r.get("check") == exp.get("check")),
                None,
            )
            assert match is not None, f"No matching rule for expected {exp} in {explicit_rules}"


class TestDataContractGeneratorSchemaValidation(DataContractGeneratorTestBase):
    """Tests for schema validation rule generation (has_valid_schema, physicalType)."""

    def test_schema_validation_rule_generated_by_default(self, generator, sample_contract_path):
        """Test that rules include has_valid_schema per schema when the contract defines a schema."""
        rules = generator.generate_rules_from_contract(
            contract_file=sample_contract_path, generate_predefined_rules=False, process_text_rules=False
        )
        schema_rules = get_schema_validation_rules(rules)
        assert len(schema_rules) >= 1, "Expected at least one schema validation rule"
        assert schema_rules[0]["check"]["arguments"]["strict"] is True
        assert schema_rules[0]["user_metadata"]["schema"] == "sensor_readings"
        assert "expected_schema" in schema_rules[0]["check"]["arguments"]

    def test_schema_validation_expected_schema_matches_contract(self, generator):
        """Test that derived expected_schema DDL matches contract properties (physicalType = Unity types)."""
        contract_dict = self.create_basic_contract(
            properties=[
                {"name": "id", "physicalType": "STRING", "required": True},
                {"name": "amount", "physicalType": "DOUBLE"},
                {"name": "created_at", "physicalType": "TIMESTAMP"},
            ]
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=False,
            )
            schema_rules = get_schema_validation_rules(rules)
            assert len(schema_rules) == 1
            ddl = schema_rules[0]["check"]["arguments"]["expected_schema"]
            assert "id STRING" in ddl
            assert "amount DOUBLE" in ddl
            assert "created_at TIMESTAMP" in ddl  # physicalType used as-is (Unity Catalog)
        finally:
            os.unlink(temp_path)

    def test_schema_validation_ddl_uses_unity_physical_type(self, generator):
        """Test that expected_schema uses physicalType (Unity Catalog) as-is when valid."""
        contract_dict = self.create_basic_contract(
            schema_name="unity_types",
            properties=[
                {"name": "id", "physicalType": "STRING"},
                {"name": "amount", "physicalType": "DOUBLE"},
                {"name": "qty", "physicalType": "INT"},
                {"name": "big_id", "physicalType": "BIGINT"},
                {"name": "flag", "physicalType": "BOOLEAN"},
                {"name": "dt", "physicalType": "DATE"},
                {"name": "ts", "physicalType": "TIMESTAMP"},
                {"name": "raw", "physicalType": "BINARY"},
                {"name": "dec_precision", "physicalType": "DECIMAL(12,4)"},
                {"name": "arr_col", "physicalType": "ARRAY<STRING>"},
            ],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            schema_rules, ddl = self._generate_rules_and_get_schema_ddl(
                generator, temp_path, generate_predefined_rules=False
            )
            assert len(schema_rules) == 1
            self._assert_unity_ddl_contains(
                ddl,
                [
                    "id STRING",
                    "amount DOUBLE",
                    "qty INT",
                    "big_id BIGINT",
                    "flag BOOLEAN",
                    "dt DATE",
                    "ts TIMESTAMP",
                    "raw BINARY",
                    "dec_precision DECIMAL(12,4)",
                    "arr_col ARRAY<STRING>",
                ],
            )
        finally:
            os.unlink(temp_path)

    def test_schema_validation_decimal_physical_type_normalized_no_spaces(self, generator):
        """DECIMAL with spaces in physicalType is normalized to DECIMAL(p,s) for consistent DDL."""
        contract_dict = self.create_basic_contract(
            properties=[
                {"name": "id", "physicalType": "STRING"},
                {"name": "amount", "physicalType": "DECIMAL ( 10 , 2 )"},
            ],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            schema_rules, ddl = self._generate_rules_and_get_schema_ddl(
                generator, temp_path, generate_predefined_rules=False
            )
            assert len(schema_rules) == 1
            assert "amount DECIMAL(10,2)" in ddl, "DECIMAL must be normalized without spaces for strict schema match"
            assert "DECIMAL ( 10 , 2 )" not in ddl, "DDL must not retain spaces from physicalType input"
        finally:
            os.unlink(temp_path)

    def test_schema_validation_complex_physical_type_normalized_uppercase(self, generator):
        """Complex types (ARRAY, MAP, STRUCT) are normalized to uppercase for consistent DDL."""
        contract_dict = self.create_basic_contract(
            properties=[
                {"name": "tags", "physicalType": "array<string>"},
                {"name": "meta", "physicalType": "map<string, int>"},
            ],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            schema_rules, ddl = self._generate_rules_and_get_schema_ddl(
                generator, temp_path, generate_predefined_rules=False
            )
            assert len(schema_rules) == 1
            assert "tags ARRAY<STRING>" in ddl, "Complex types must be normalized to uppercase"
            assert "meta MAP<STRING,INT>" in ddl, "MAP type must be normalized to uppercase"
            assert "array<string>" not in ddl and "map<string" not in ddl, "DDL must not retain lowercase type"
        finally:
            os.unlink(temp_path)


class TestDataContractGeneratorSchemaValidationRecursiveAndDecimal(DataContractGeneratorTestBase):
    """Tests for recursive type validation, DECIMAL limits, recursion depth, and nameless-property warnings."""

    def test_schema_validation_recursive_invalid_inner_type_array(self, generator):
        """ARRAY<NOT_A_TYPE> raises InvalidPhysicalTypeError."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "ARRAY<NOT_A_TYPE>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError) as exc_info:
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
            assert "NOT_A_TYPE" in str(exc_info.value) or "not a valid" in str(exc_info.value).lower()
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursive_invalid_inner_type_map_value(self, generator):
        """MAP<STRING, NOT_A_TYPE> raises InvalidPhysicalTypeError."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "MAP<STRING, NOT_A_TYPE>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError):
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursive_invalid_inner_type_map_key(self, generator):
        """MAP<NOT_A_TYPE, INT> raises InvalidPhysicalTypeError."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "MAP<NOT_A_TYPE, INT>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError):
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursive_invalid_inner_type_struct(self, generator):
        """STRUCT<a:INT, b:NOT_A_TYPE> raises InvalidPhysicalTypeError."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "STRUCT<a:INT, b:NOT_A_TYPE>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError):
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursive_invalid_nested_array_map(self, generator):
        """ARRAY<MAP<STRING, BAD>> raises InvalidPhysicalTypeError."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "ARRAY<MAP<STRING, BAD>>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError):
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursive_invalid_struct_array_inner(self, generator):
        """STRUCT<x:ARRAY<INVALID>> raises InvalidPhysicalTypeError."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "STRUCT<x:ARRAY<INVALID>>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError):
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursive_valid_array_array_int(self, generator):
        """ARRAY<ARRAY<INT>> is valid and appears in DDL."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "ARRAY<ARRAY<INT>>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            schema_rules, ddl = self._generate_rules_and_get_schema_ddl(
                generator, temp_path, generate_predefined_rules=False
            )
            assert len(schema_rules) == 1
            self._assert_unity_ddl_contains(ddl, ["col ARRAY<ARRAY<INT>>"])
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursive_valid_map_string_array_int(self, generator):
        """MAP<STRING, ARRAY<INT>> is valid and appears in DDL."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "MAP<STRING, ARRAY<INT>>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            schema_rules, ddl = self._generate_rules_and_get_schema_ddl(
                generator, temp_path, generate_predefined_rules=False
            )
            assert len(schema_rules) == 1
            self._assert_unity_ddl_contains(ddl, ["col MAP<STRING,ARRAY<INT>>"])
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursive_valid_struct_with_array(self, generator):
        """STRUCT<a:INT, b:ARRAY<STRING>> is valid and appears in DDL."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "STRUCT<a:INT, b:ARRAY<STRING>>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            schema_rules, ddl = self._generate_rules_and_get_schema_ddl(
                generator, temp_path, generate_predefined_rules=False
            )
            assert len(schema_rules) == 1
            assert "col STRUCT<" in ddl
            assert "a:INT" in ddl
            assert "b:ARRAY<STRING>" in ddl
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursive_valid_struct_nested_struct(self, generator):
        """STRUCT<a:STRUCT<x:INT, y:STRING>> is valid and appears in DDL."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "STRUCT<a:STRUCT<x:INT, y:STRING>>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            schema_rules, ddl = self._generate_rules_and_get_schema_ddl(
                generator, temp_path, generate_predefined_rules=False
            )
            assert len(schema_rules) == 1
            assert "col STRUCT<" in ddl
            assert "a:STRUCT<" in ddl
            assert "x:INT" in ddl
            assert "y:STRING" in ddl
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursive_valid_deep_array(self, generator):
        """ARRAY<ARRAY<ARRAY<STRING>>> is valid."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "ARRAY<ARRAY<ARRAY<STRING>>>"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            schema_rules, ddl = self._generate_rules_and_get_schema_ddl(
                generator, temp_path, generate_predefined_rules=False
            )
            assert len(schema_rules) == 1
            assert "ARRAY<ARRAY<ARRAY<STRING>>>" in ddl
        finally:
            os.unlink(temp_path)

    def test_schema_validation_recursion_limit_exceeded(self, generator):
        """Type nested beyond max depth raises InvalidPhysicalTypeError."""
        depth = 52  # One more than _MAX_TYPE_RECURSION_DEPTH (50)
        nested = "STRING"
        for _ in range(depth):
            nested = f"ARRAY<{nested}>"
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": nested}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError) as exc_info:
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
            assert "depth" in str(exc_info.value).lower() or "50" in str(exc_info.value)
        finally:
            os.unlink(temp_path)

    def test_schema_validation_decimal_precision_over_38_raises(self, generator):
        """DECIMAL(100,2) raises InvalidPhysicalTypeError."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "DECIMAL(100,2)"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError) as exc_info:
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
            assert "38" in str(exc_info.value) or "precision" in str(exc_info.value).lower()
        finally:
            os.unlink(temp_path)

    def test_schema_validation_decimal_scale_over_precision_raises(self, generator):
        """DECIMAL(10,50) raises InvalidPhysicalTypeError."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "DECIMAL(10,50)"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with pytest.raises(InvalidPhysicalTypeError) as exc_info:
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
            assert "scale" in str(exc_info.value).lower() or "precision" in str(exc_info.value).lower()
        finally:
            os.unlink(temp_path)

    def test_schema_validation_decimal_38_10_accepted(self, generator):
        """DECIMAL(38,10) is accepted (Spark max precision)."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "col", "physicalType": "DECIMAL(38,10)"}],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            schema_rules, ddl = self._generate_rules_and_get_schema_ddl(
                generator, temp_path, generate_predefined_rules=False
            )
            assert len(schema_rules) == 1
            assert "col DECIMAL(38,10)" in ddl
        finally:
            os.unlink(temp_path)

    def test_schema_validation_nameless_property_warns_in_ddl_and_schema_info(self, generator, caplog):
        """Skipping property without name logs warning in DDL and schema_info paths."""
        contract_dict = self.create_basic_contract(
            properties=[
                {"name": "valid_col", "physicalType": "STRING"},
                {"name": "", "physicalType": "INT"},  # nameless
            ],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            with caplog.at_level(logging.WARNING):
                generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=True,
                    process_text_rules=False,
                )
            # Both _schema_object_to_ddl and _build_schema_info_from_model (via text/predefined path) may see props
            # Predefined rules iterate properties and skip nameless with warning; DDL path also skips with warning
            assert any("Skipping property without name" in rec.message for rec in caplog.records)
        finally:
            os.unlink(temp_path)


class TestDataContractGeneratorPredefinedRules(DataContractGeneratorTestBase):
    """Test predefined rule generation from field constraints."""

    def test_required_field_generates_is_not_null(self, generator):
        """Test that required fields generate is_not_null rules."""
        # Create a simple contract with a required property
        contract_dict = self.create_basic_contract(
            properties=[{"name": "user_id", "physicalType": "STRING", "logicalType": "string", "required": True}]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=True,
                process_text_rules=False,
            )

            predefined = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "predefined"]
            assert len(predefined) == 1
            assert predefined[0]["check"]["function"] == "is_not_null"
            assert predefined[0]["check"]["arguments"]["column"] == "user_id"
            assert predefined[0]["user_metadata"]["dimension"] == "completeness"
        finally:
            os.unlink(temp_path)

    def test_unique_field_generates_is_unique(self, generator):
        """Test that unique fields generate is_unique rules."""
        contract_dict = self.create_basic_contract(
            properties=[{"name": "user_id", "physicalType": "STRING", "logicalType": "string", "unique": True}]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(contract_file=temp_path)

            unique_rules = [r for r in rules if r["check"]["function"] == "is_unique"]
            assert len(unique_rules) == 1
            assert unique_rules[0]["check"]["arguments"]["columns"] == ["user_id"]
        finally:
            os.unlink(temp_path)

    def test_enum_generates_is_in_list(self, generator):
        """Test that enum fields generate regex_match rules (ODCS v3.x uses pattern for enums)."""
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "status",
                    "physicalType": "STRING",
                    "logicalType": "string",
                    "logicalTypeOptions": {"pattern": "^(active|inactive|pending)$"},
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "code",
                    "physicalType": "STRING",
                    "logicalType": "string",
                    "logicalTypeOptions": {"pattern": "^[A-Z]{3}-[0-9]{4}$"},
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(contract_file=temp_path)

            pattern_rules = [r for r in rules if r["check"]["function"] == "regex_match"]
            assert len(pattern_rules) == 1
            assert pattern_rules[0]["check"]["arguments"]["regex"] == "^[A-Z]{3}-[0-9]{4}$"
        finally:
            os.unlink(temp_path)

    def test_range_generates_is_in_range(self, generator):
        """Test that minimum/maximum generate is_in_range rules."""
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "age",
                    "physicalType": "INT",
                    "logicalType": "integer",
                    "logicalTypeOptions": {"minimum": 0, "maximum": 120},
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "event_date",
                    "physicalType": "DATE",
                    "logicalType": "date",
                    "logicalTypeOptions": {"format": "%Y-%m-%d"},
                },
                {
                    "name": "event_time",
                    "physicalType": "TIMESTAMP",
                    "logicalType": "timestamp",
                    "logicalTypeOptions": {"format": "%Y-%m-%d %H:%M:%S"},
                },
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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

        for rule in rules:
            metadata = rule["user_metadata"]
            assert "contract_id" in metadata
            assert "contract_version" in metadata
            assert "odcs_version" in metadata
            assert "schema" in metadata
            # field is optional (not present for schema-level rules)
            assert "rule_type" in metadata
            assert metadata["rule_type"] in {"predefined", "explicit", "text_llm", "schema_validation"}

    def test_missing_datacontract_cli_dependency(self, generator, monkeypatch):
        """Test error when datacontract-cli is not installed."""
        monkeypatch.setattr(generator_module, "DATACONTRACT_ENABLED", False)

        # Attempt to generate rules should raise ImportError
        with pytest.raises(MissingParameterError, match="Data contract support requires datacontract-cli"):
            generator.generate_rules_from_contract(contract_file="dummy.yaml")

    def test_nested_fields_generate_rules(self, generator):
        """Test that nested field structures generate rules with proper column paths."""
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "customer",
                    "physicalType": "BINARY",
                    "logicalType": "object",
                    "required": True,
                    "properties": [
                        {"name": "name", "physicalType": "STRING", "logicalType": "string", "required": True},
                        {
                            "name": "email",
                            "physicalType": "STRING",
                            "logicalType": "string",
                            "required": True,
                            "logicalTypeOptions": {"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"},
                        },
                        {
                            "name": "address",
                            "physicalType": "BINARY",
                            "logicalType": "object",
                            "properties": [
                                {"name": "street", "physicalType": "STRING", "logicalType": "string", "required": True},
                                {"name": "city", "physicalType": "STRING", "logicalType": "string", "required": True},
                                {
                                    "name": "zipcode",
                                    "physicalType": "STRING",
                                    "logicalType": "string",
                                    "logicalTypeOptions": {"pattern": "^[0-9]{5}$"},
                                },
                            ],
                        },
                    ],
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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

        # Check that metadata also contains the nested paths (skip schema_validation rules; they have no 'field')
        for rule in rules:
            if "field" not in rule.get("user_metadata", {}):
                continue
            field = rule["user_metadata"]["field"]
            assert "." in field or field == "customer", f"Nested fields should have dot notation: {field}"

        return rules

    def test_fields_without_quality_checks(self, generator):
        """Test that fields without quality checks are handled gracefully."""
        contract_dict = self.create_basic_contract(
            properties=[
                {"name": "id", "physicalType": "STRING", "logicalType": "string", "required": True},
                {"name": "optional_field", "physicalType": "STRING", "logicalType": "string", "required": False},
                {"name": "another_field", "physicalType": "STRING", "logicalType": "string"},
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=True,
                process_text_rules=False,
            )

            # Should only generate predefined rule for required field (plus schema_validation)
            predefined = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "predefined"]
            assert len(predefined) == 1, "Should only generate one predefined rule for field with constraints"
            assert predefined[0]["check"]["arguments"]["column"] == "id"

        finally:
            os.unlink(temp_path)

    def test_empty_text_description_skipped(self, generator):
        """Test that text quality checks with empty/whitespace descriptions are skipped."""
        contract_dict = self.create_contract_with_quality(
            property_name="user_id",
            logical_type="string",
            quality_checks=[
                {"type": "text", "description": ""},  # Empty description
                {"type": "text", "description": "   "},  # Whitespace only
            ],
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=True,
            )

            # Should generate no text_llm rules; only schema_validation when schema is present
            text_rules = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "text_llm"]
            assert len(text_rules) == 0, "Should skip text rules with empty or whitespace-only descriptions"

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
                        {"name": "user_id", "physicalType": "STRING", "logicalType": "string", "required": True},
                    ],
                }
            ],
        }

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        """Test that contracts with multiple schemas generate rules for all schemas (predefined only)."""
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
                        {"name": "user_id", "physicalType": "STRING", "logicalType": "string", "required": True},
                        {"name": "email", "physicalType": "STRING", "logicalType": "string", "required": True},
                    ],
                },
                {
                    "name": "orders",
                    "physicalType": "table",
                    "properties": [
                        {"name": "order_id", "physicalType": "STRING", "logicalType": "string", "required": True},
                        {"name": "user_id", "physicalType": "STRING", "logicalType": "string", "required": True},
                    ],
                },
            ],
        }

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=True,
                process_text_rules=False,
            )

            # Should generate rules for both schemas (2 schema_validation + 4 predefined is_not_null)
            schemas_in_rules = {rule["user_metadata"]["schema"] for rule in rules}
            assert "users" in schemas_in_rules, "Should have rules for users schema"
            assert "orders" in schemas_in_rules, "Should have rules for orders schema"

            # 2 schema_validation + 4 predefined (2 required per schema)
            assert len(rules) == 6, f"Should have 6 rules (2 schema + 4 predefined), got {len(rules)}"

        finally:
            os.unlink(temp_path)

    def test_multiple_schemas_with_schema_validation(self, generator):
        """Test that contracts with multiple schemas get one schema_validation rule per schema."""
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "multi-schema-sv",
            "name": "multi-schema-sv",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "users",
                    "physicalType": "table",
                    "properties": [
                        {"name": "user_id", "physicalType": "STRING", "logicalType": "string", "required": True},
                        {"name": "email", "physicalType": "STRING", "logicalType": "string", "required": True},
                    ],
                },
                {
                    "name": "orders",
                    "physicalType": "table",
                    "properties": [
                        {"name": "order_id", "physicalType": "STRING", "logicalType": "string", "required": True},
                        {"name": "user_id", "physicalType": "STRING", "logicalType": "string", "required": True},
                    ],
                },
            ],
        }
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)
        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=True,
                process_text_rules=False,
            )
            schema_validation_rules = get_schema_validation_rules(rules)
            assert len(schema_validation_rules) == 2, "Expected one schema_validation rule per schema"
            names = {r["name"] for r in schema_validation_rules}
            assert "users_schema_validation" in names
            assert "orders_schema_validation" in names
        finally:
            os.unlink(temp_path)

    def test_field_with_multiple_constraints(self, generator):
        """Test that fields with multiple constraints generate multiple rules."""
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "product_code",
                    "physicalType": "STRING",
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

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_contract_with_quality(
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

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=False,
            )

            # Should have 2 explicit rules with different criticalities plus schema validation
            explicit = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"]
            assert len(explicit) == 2, f"Should have 2 explicit rules, got {len(explicit)}"

            criticalities = {rule["criticality"] for rule in explicit}
            assert "error" in criticalities, "Should have error criticality"
            assert "warn" in criticalities, "Should have warn criticality"

        finally:
            os.unlink(temp_path)


class TestDataContractGeneratorConstraints(DataContractGeneratorTestBase):
    """Test constraint-specific predefined rule generation."""

    def test_field_with_only_minimum_constraint(self, generator):
        """Test that field with only minimum (no maximum) generates sql_expression rule for float."""
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "temperature",
                    "physicalType": "DOUBLE",
                    "logicalType": "number",
                    "logicalTypeOptions": {
                        "minimum": -273.15,  # Float minimum, should use sql_expression
                    },
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "humidity",
                    "physicalType": "DOUBLE",
                    "logicalType": "number",
                    "logicalTypeOptions": {
                        "maximum": 100.0,  # Float maximum, should use sql_expression
                    },
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "age",
                    "physicalType": "INT",
                    "logicalType": "integer",
                    "logicalTypeOptions": {
                        "maximum": 150,  # Integer maximum
                    },
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate is_aggr_not_greater_than rule for integer maximum
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "is_aggr_not_greater_than" in rule_functions

            # Verify the rule details
            max_rule = next(r for r in rules if r["check"]["function"] == "is_aggr_not_greater_than")
            assert max_rule["check"]["arguments"]["column"] == "age"
            assert max_rule["check"]["arguments"]["limit"] == 150
            assert max_rule["check"]["arguments"]["aggr_type"] == "max"

        finally:
            os.unlink(temp_path)

    def test_field_with_only_integer_minimum_constraint(self, generator):
        """Test that field with only integer minimum generates is_aggr_not_less_than rule."""
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "quantity",
                    "physicalType": "INT",
                    "logicalType": "integer",
                    "logicalTypeOptions": {
                        "minimum": 0,  # Integer minimum
                    },
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should generate is_aggr_not_less_than rule for integer minimum
            rule_functions = [rule["check"]["function"] for rule in rules]
            assert "is_aggr_not_less_than" in rule_functions

            # Verify the rule details
            min_rule = next(r for r in rules if r["check"]["function"] == "is_aggr_not_less_than")
            assert min_rule["check"]["arguments"]["column"] == "quantity"
            assert min_rule["check"]["arguments"]["limit"] == 0
            assert min_rule["check"]["arguments"]["aggr_type"] == "min"

        finally:
            os.unlink(temp_path)

    def test_explicit_dqx_quality_check_detection(self, generator):
        """Test that explicit DQX quality checks are correctly identified."""
        contract_dict = self.create_contract_with_quality(
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

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=False,
            )

            # Should have 1 explicit rule (the DQX one) plus schema validation
            explicit = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"]
            assert len(explicit) == 1, f"Should only extract one DQX explicit rule, got {len(explicit)}"
            assert explicit[0]["check"]["function"] == "is_not_null"

        finally:
            os.unlink(temp_path)

    def test_non_dqx_custom_quality_checks_ignored(self, generator):
        """Test that custom quality checks with non-dqx engines are ignored."""
        contract_dict = self.create_contract_with_quality(
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

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=False,
            )

            # Should generate no explicit rules; only schema_validation is present
            explicit = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"]
            assert len(explicit) == 0, "Should ignore non-DQX custom quality checks"
            assert len(get_schema_validation_rules(rules)) == 1

        finally:
            os.unlink(temp_path)

    def test_minimum_and_maximum_together(self, generator):
        """Test that field with both minimum and maximum generates is_in_range rule."""
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "age",
                    "physicalType": "INT",
                    "logicalType": "integer",
                    "logicalTypeOptions": {
                        "minimum": 0,
                        "maximum": 120,
                    },
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "code",
                    "physicalType": "STRING",
                    "logicalType": "string",
                    "logicalTypeOptions": {
                        "minLength": 5,
                    },
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "description",
                    "physicalType": "STRING",
                    "logicalType": "string",
                    "logicalTypeOptions": {
                        "maxLength": 200,
                    },
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "postal_code",
                    "physicalType": "STRING",
                    "logicalType": "string",
                    "logicalTypeOptions": {
                        "minLength": 5,
                        "maxLength": 10,
                    },
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "status",
                    "physicalType": "STRING",
                    "logicalType": "string",
                    "logicalTypeOptions": {
                        "description": "User status field",
                        # No minimum, maximum, minLength, maxLength, etc.
                    },
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        odcs = resolve_data_contract(data_contract_location=temp_path)
        schema = odcs.schema_[0]  # Get first schema
        return schema

    def _create_test_rules_generator(self, mock_workspace_client):
        """Helper to create a test rules generator with mocked workspace client."""
        return DataContractRulesGenerator(workspace_client=mock_workspace_client)

    def _verify_schema_info(self, schema_info, expected_columns):
        """Helper to verify schema info structure."""
        schema_dict = json.loads(schema_info)
        assert "columns" in schema_dict
        assert len(schema_dict["columns"]) == len(expected_columns)
        column_names = [col["name"] for col in schema_dict["columns"]]
        for col_name in expected_columns:
            assert col_name in column_names

    def test_build_schema_info_from_model(self, generator, mock_workspace_client):
        """Test that schema info is correctly built from a model."""
        contract_dict = self.create_basic_contract(
            properties=[
                {"name": "customer_id", "physicalType": "STRING", "logicalType": "string"},
                {"name": "order_date", "physicalType": "DATE", "logicalType": "date"},
                {"name": "amount", "physicalType": "DOUBLE", "logicalType": "number"},
            ]
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            model = self._load_contract_and_get_model(temp_path)
            gen = self._create_test_rules_generator(mock_workspace_client)
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
        mock_llm_engine.detect_business_rules_with_llm.return_value = mock_prediction
        return mock_llm_engine

    def _verify_text_rules(self, rules, mock_llm_engine):
        """Helper to verify text rule generation."""
        assert mock_llm_engine.detect_business_rules_with_llm.called
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
        contract_dict = self.create_contract_with_quality(
            property_name="email",
            logical_type="string",
            quality_checks=[
                {
                    "type": "text",
                    "description": "Email addresses must be valid and from approved domains (company.com or partner.com)",
                }
            ],
        )
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_contract_with_quality(
            property_name="user_id",
            logical_type="string",
            quality_checks=[
                {
                    "type": "text",
                    "description": "User IDs must follow the corporate standard format",
                }
            ],
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            # Generator without LLM engine (llm_engine=None by default in fixture)
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=True,
            )

            # Should generate no text_llm rules (only schema_validation when schema is present)
            text_rules = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "text_llm"]
            assert len(text_rules) == 0, "Should skip text rules when LLM is not available"

        finally:
            os.unlink(temp_path)

    def test_text_rules_skipped_when_process_text_false(self, generator):
        """Test that text rules are skipped when process_text_rules=False."""
        contract_dict = self.create_contract_with_quality(
            property_name="user_id",
            logical_type="string",
            quality_checks=[
                {
                    "type": "text",
                    "description": "User IDs must follow the corporate standard format",
                }
            ],
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            # Mock LLM engine
            mock_llm_engine = Mock()
            generator.llm_engine = mock_llm_engine

            # Generate with process_text_rules=False
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=False,
            )

            # Should not call LLM
            assert not mock_llm_engine.detect_business_rules_with_llm.called

            # Should generate no text_llm rules (only schema_validation)
            text_rules = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "text_llm"]
            assert len(text_rules) == 0, "Should skip text rules when process_text_rules=False"

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
        mock_llm_engine.detect_business_rules_with_llm.side_effect = [mock_prediction1, mock_prediction2]
        return mock_llm_engine

    def _verify_multiple_text_rules(self, rules, mock_llm_engine):
        """Helper to verify multiple text rules were generated."""
        assert mock_llm_engine.detect_business_rules_with_llm.call_count == 2
        text_rules = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "text_llm"]
        assert len(text_rules) == 2
        fields_with_rules = {r["user_metadata"]["field"] for r in text_rules}
        assert "order_id" in fields_with_rules
        assert "customer_email" in fields_with_rules

        assert len(text_rules) > 0

        for rule in text_rules:
            # Use DQEngine's validate_checks method
            validation_status = DQEngine.validate_checks([rule], validate_custom_check_functions=False)
            assert not validation_status.has_errors, f"Rule validation failed: {validation_status.errors}"

            # Additionally check user_metadata if present (this is contract-specific, not in DQEngine validation)
            if "user_metadata" in rule:
                assert isinstance(rule["user_metadata"], dict), "user_metadata must be a dictionary"

        # Verify all are text_llm rules
        for rule in text_rules:
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
                            "physicalType": "STRING",
                            "logicalType": "string",
                            "quality": [{"type": "text", "description": "Order IDs must be unique across all systems"}],
                        },
                        {
                            "name": "customer_email",
                            "physicalType": "STRING",
                            "logicalType": "string",
                            "quality": [{"type": "text", "description": "Email must be valid and deliverable"}],
                        },
                    ],
                }
            ],
        }
        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            mock_llm_engine = self._create_multiple_text_rules_llm_mock()
            generator.llm_engine = mock_llm_engine
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path,
                generate_predefined_rules=False,
                process_text_rules=True,
            )
            self._verify_multiple_text_rules(rules, mock_llm_engine)
        finally:
            os.unlink(temp_path)

    # Additional tests for coverage improvement

    def test_format_generates_timestamp_validation(self, generator):
        """Test that format constraint on timestamp field generates is_valid_timestamp rule."""
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "created_at",
                    "physicalType": "TIMESTAMP",
                    "logicalType": "timestamp",
                    "required": True,
                    "logicalTypeOptions": {"format": "yyyy-MM-dd HH:mm:ss"},
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "status",
                    "physicalType": "STRING",
                    "logicalType": "string",  # Not date/timestamp
                    "logicalTypeOptions": {"format": "yyyy-MM-dd"},  # Format should be ignored
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        contract_dict = self.create_basic_contract(
            properties=[
                {
                    "name": "birth_date",
                    "physicalType": "DATE",
                    "logicalType": "date",
                    "logicalTypeOptions": {"format": "%Y-%m-%d"},  # Already Python format
                }
            ]
        )

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
                    "properties": [
                        {"name": "field1", "physicalType": "STRING", "logicalType": "string", "required": True}
                    ],
                }
            ],
        }

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            assert len(rules) >= 1
            # Metadata should use "unknown_schema"
            assert rules[0]["user_metadata"]["schema"] == "unknown_schema"
        finally:
            os.unlink(temp_path)

    def test_property_without_name_is_skipped(self, generator, caplog):
        """Test that property without name is skipped and warning is logged."""
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:no_prop_name",
            "name": "Property Without Name Contract",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "test_schema",
                    "properties": [
                        {"name": "valid_field", "physicalType": "STRING", "logicalType": "string", "required": True},
                        # Property without name - should be skipped
                        {"logicalType": "integer", "required": False},
                    ],
                }
            ],
        }

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            with caplog.at_level(logging.WARNING):
                rules = generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=True,
                    process_text_rules=False,
                )

            # Should have rules for valid_field only (schema_validation has no 'field')
            field_names = {rule["user_metadata"]["field"] for rule in rules if "field" in rule["user_metadata"]}
            assert "valid_field" in field_names
            # Should NOT have rules for the unnamed property
            assert all("None" not in name for name in field_names)

            # Should log warning about skipping property without name
            assert "Skipping property without name" in caplog.text
        finally:
            os.unlink(temp_path)

    @staticmethod
    def _assert_invalid_explicit_rules_filtered(rules, caplog):
        """Assert that invalid explicit rules were filtered and expected warnings were logged."""
        explicit = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"]
        assert len(explicit) == 0, "Invalid explicit rules should be filtered out"
        assert len(get_schema_validation_rules(rules)) == 1
        assert "Excluding invalid rule" in caplog.text
        assert "invalid_rule" in caplog.text
        assert "excluded 2 invalid rule(s)" in caplog.text

    def test_invalid_explicit_rule_is_filtered_out(self, generator, caplog):
        """Test that contracts with invalid explicit rules filter them out with warnings."""
        contract_dict = {
            "kind": "DataContract",
            "apiVersion": "v3.0.2",
            "id": "test:invalid_rule",
            "name": "Invalid Rule Contract",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "test_schema",
                    "properties": [
                        {
                            "name": "amount",
                            "physicalType": "DOUBLE",
                            "logicalType": "number",
                            "quality": [
                                {
                                    "type": "custom",
                                    "engine": "dqx",
                                    "implementation": {
                                        "check": {
                                            "function": "nonexistent_function_that_does_not_exist",
                                            "arguments": {"column": "amount", "limit": 1000},
                                        },
                                        "name": "invalid_rule",
                                        "criticality": "error",
                                    },
                                },
                                {
                                    "type": "custom",
                                    "engine": "dqx",
                                    "implementation": {
                                        "check": "invalid_format",
                                        "name": "invalid_rule",
                                        "criticality": "error",
                                    },
                                },
                            ],
                        }
                    ],
                }
            ],
        }

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            with caplog.at_level(logging.WARNING):
                rules = generator.generate_rules_from_contract(
                    contract_file=temp_path,
                    generate_predefined_rules=False,
                    process_text_rules=False,
                )
            self._assert_invalid_explicit_rules_filtered(rules, caplog)
        finally:
            os.unlink(temp_path)

    def test_deep_nested_fields_hit_recursion_limit(self, generator, caplog):
        """Test that deeply nested fields hit recursion limit and log warning."""

        # Create a deeply nested structure (25 levels deep, limit is 20)
        def create_nested_property(depth):
            if depth == 0:
                return {"name": "leaf", "physicalType": "STRING", "logicalType": "string", "required": True}
            return {
                "name": f"level_{depth}",
                "physicalType": "BINARY",
                "logicalType": "object",
                "properties": [create_nested_property(depth - 1)],
            }

        contract_dict = self.create_basic_contract(properties=[create_nested_property(25)])

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

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
        with pytest.raises(NotFound, match="Contract file not found"):
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
            with pytest.raises(ODCSContractError, match="Failed to parse ODCS contract"):
                generator.generate_rules_from_contract(contract_file=temp_path)
        finally:
            os.unlink(temp_path)

    def test_schema_level_text_rules(self, generator, mock_workspace_client, mock_spark):
        """Test text rules defined at schema level (not property level)."""
        mock_llm = Mock()
        mock_llm.detect_business_rules_with_llm = Mock(
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
                        {"name": "id", "physicalType": "STRING", "logicalType": "string"},
                        {"name": "timestamp", "physicalType": "TIMESTAMP", "logicalType": "timestamp"},
                    ],
                }
            ],
        }

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = gen_with_llm.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=False, process_text_rules=True
            )

            # Should have called LLM for schema-level text rule
            assert mock_llm.detect_business_rules_with_llm.called
            assert len(rules) >= 1
        finally:
            os.unlink(temp_path)

    def test_explicit_rule_with_object_implementation(self, generator):
        """Test explicit rule where implementation is an object with attributes (not dict)."""
        # Load a contract that will have implementation as Pydantic objects
        contract_dict = self.create_contract_with_quality(
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

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            # Load using datacontract-cli which returns Pydantic models
            data_contract = DataContract(data_contract_file=temp_path)

            rules = generator.generate_rules_from_contract(
                contract=data_contract,
                generate_predefined_rules=False,
                process_text_rules=False,
            )

            explicit = [r for r in rules if r.get("user_metadata", {}).get("rule_type") == "explicit"]
            assert len(explicit) >= 1
            assert explicit[0]["criticality"] == "warn"
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

        temp_path = self.create_test_contract_file(custom_contract=contract_dict)

        try:
            rules = generator.generate_rules_from_contract(
                contract_file=temp_path, generate_predefined_rules=True, process_text_rules=False
            )

            # Should return empty list
            assert len(rules) == 0
        finally:
            os.unlink(temp_path)
