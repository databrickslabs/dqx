import os
import json

import yaml
import pytest
import testing.postgresql
from sqlalchemy import create_engine, select


from databricks.labs.dqx.engine import DQEngineCore
from databricks.labs.dqx.checks_storage import LakebaseChecksStorageHandler
from databricks.labs.dqx.config import LakebaseChecksStorageConfig
from databricks.labs.dqx.config import LakebaseConnectionConfig
from databricks.labs.dqx.errors import InvalidConfigError

from tests.conftest import compare_checks


TEST_CHECKS = [
    {
        "name": "id_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    },
    {
        "name": "name_is_null",
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
        ("", InvalidConfigError, "The file path \\('location' field\\) must not be empty or None"),
        (None, InvalidConfigError, "The file path \\('location' field\\) must not be empty or None"),
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


def test_lakebase_checks_storage_handler_save(ws, spark):
    location = "test.public.checks"

    with testing.postgresql.Postgresql() as postgresql:
        connection_string = postgresql.url()

        engine = create_engine(connection_string)
        handler = LakebaseChecksStorageHandler(ws, spark, engine)
        config = LakebaseChecksStorageConfig(location, connection_string)
        schema_name, table_name = handler.get_schema_and_table_name(config)
        table = handler.get_table_definition(schema_name, table_name)

        handler.save(TEST_CHECKS, config)

        with engine.connect() as conn:
            result = conn.execute(select(table)).mappings().all()
            result = [dict(check) for check in result]

        compare_checks(result, TEST_CHECKS)


def test_installation_checks_storage_handler_postgresql_parsing():
    connection_string = (
        "postgresql://user@domain.com:password@instance-test.database.azuredatabricks.net:5432/dqx?sslmode=require"
    )
    connection_config = LakebaseConnectionConfig.parse_connection_string(connection_string)

    assert connection_config.user == "user@domain.com"
    assert connection_config.instance_name == "instance-test.database.azuredatabricks.net"
    assert connection_config.port == "5432"
    assert connection_config.database == "dqx"
