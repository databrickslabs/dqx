import os
import json
import yaml

import pytest
from unittest.mock import create_autospec
from sqlalchemy import create_engine, select

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.engine import DQEngineCore
from databricks.labs.dqx.checks_storage import LakebaseChecksStorageHandler
from databricks.labs.dqx.config import LakebaseChecksStorageConfig
from databricks.labs.dqx.config import LakebaseConnectionConfig

import testing.postgresql


TEST_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    },
    {
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "for_each_column": ["col1", "col2"],
            "arguments": {},
            "user_metadata": {"rule_type": "completeness"},
        },
    },
]


def test_save_checks_to_local_file_as_yaml(make_local_check_file_as_yaml):
    file = make_local_check_file_as_yaml
    DQEngineCore.save_checks_in_local_file(TEST_CHECKS, file)
    _validate_file(file, "yaml")

    checks = DQEngineCore.load_checks_from_local_file(file)
    assert checks == TEST_CHECKS, "The loaded checks do not match the expected checks."


def test_save_checks_to_local_file_as_json(make_local_check_file_as_json):
    file = make_local_check_file_as_json
    DQEngineCore.save_checks_in_local_file(TEST_CHECKS, file)
    _validate_file(file, "json")

    checks = DQEngineCore.load_checks_from_local_file(file)
    assert checks == TEST_CHECKS, "The loaded checks do not match the expected checks."


@pytest.mark.parametrize(
    "filename, expected_exception, expected_message",
    [
        ("", ValueError, "The file path \\('location' field\\) must not be empty or None"),
        (None, ValueError, "The file path \\('location' field\\) must not be empty or None"),
    ],
)
def test_load_checks_from_local_file_exceptions(filename, expected_exception, expected_message):
    with pytest.raises(expected_exception, match=expected_message):
        DQEngineCore.save_checks_in_local_file(TEST_CHECKS, filename)


def _validate_file(file_path: str, file_format: str = "yaml") -> None:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path, "r", encoding="utf-8") as file:
        if file_format == "json":
            json.load(file)
        yaml.safe_load(file)


def test_lakebase_checks_storage_handler_save():
    ws = create_autospec(WorkspaceClient)
    spark = create_autospec("pyspark.sql.SparkSession")
    location = "test.public.checks"

    expected_checks = [
        {
            'name': 'id_is_null',
            'criticality': 'error',
            'check': {'function': 'is_not_null', 'arguments': {'column': 'id'}},
            'filter': None,
            'run_config_name': 'default',
            'user_metadata': None,
        },
        {
            'name': 'name_is_null',
            'criticality': 'warning',
            'check': {'function': 'is_not_null', 'arguments': {'column': 'name'}},
            'filter': "col1 < 3",
            'run_config_name': 'extended',
            'user_metadata': {'team': 'data-engineers'},
        },
    ]

    with testing.postgresql.Postgresql() as postgresql:
        connection_string = postgresql.url()

        engine = create_engine(connection_string)
        handler = LakebaseChecksStorageHandler(ws, spark, engine)
        config = LakebaseChecksStorageConfig(location, connection_string)
        schema_name, table_name = handler._get_schema_and_table_name(config)
        table = handler._get_table_definition(schema_name, table_name)

        handler.save(expected_checks, config)

        with engine.connect() as conn:
            result = conn.execute(select(table)).mappings().all()
            result = [dict(check) for check in result]

        assert len(result) == 2, f"Expected 2 checks, got {len(result)}"

        for check in result:
            assert 'name' in check, "Missing 'name' field"
            assert 'criticality' in check, "Missing 'criticality' field"
            assert 'check' in check, "Missing 'check' field"
            assert 'run_config_name' in check, "Missing 'run_config_name' field"
            assert check['criticality'] in ['error', 'warning', 'info'], f"Invalid criticality: {check['criticality']}"

        id_check = next((c for c in result if c['name'] == 'id_is_null'), None)
        name_check = next((c for c in result if c['name'] == 'name_is_null'), None)

        assert id_check is not None, "Missing 'id_is_null' check"
        assert name_check is not None, "Missing 'name_is_null' check"

        assert id_check['criticality'] == 'error'
        assert id_check['check'] == {'function': 'is_not_null', 'arguments': {'column': 'id'}}
        assert id_check['filter'] is None
        assert id_check['run_config_name'] == 'default'
        assert id_check['user_metadata'] is None

        assert name_check['criticality'] == 'warning'
        assert name_check['check'] == {'function': 'is_not_null', 'arguments': {'column': 'name'}}
        assert name_check['filter'] == "col1 < 3"
        assert name_check['run_config_name'] == 'extended'
        assert name_check['user_metadata'] == {'team': 'data-engineers'}


def test_installation_checks_storage_handler_postgresql_parsing():
    connection_string = (
        "postgresql://user@databricks.com:password@instance-test.database.azuredatabricks.net:5432/dqx?sslmode=require"
    )
    connection_config = LakebaseConnectionConfig._parse_connection_string(connection_string)

    assert connection_config.user == "user@databricks.com"
    assert connection_config.instance_name == "instance-test.database.azuredatabricks.net"
    assert connection_config.port == "5432"
    assert connection_config.database == "dqx"
