import pytest

from databricks.sdk.errors import NotFound

from databricks.labs.dqx.config import InstallationChecksStorageConfig, LakebaseChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine

from tests.conftest import compare_checks, connection_string


TEST_CHECKS = [
    {
        "name": "col1_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col1"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
    },
    {
        "name": "col2_is_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "col2"}},
        "user_metadata": {"check_type": "completeness", "check_owner": "someone@email.com"},
    },
    {
        "name": "column_not_less_than",
        "criticality": "warn",
        "check": {
            "function": "is_not_less_than",
            "arguments": {"column": "col_2", "limit": 1},
        },
        "user_metadata": {"check_type": "standardization", "check_owner": "someone_else@email.com"},
    },
    {
        "name": "column_in_list",
        "criticality": "warn",
        "check": {"function": "is_in_list", "arguments": {"column": "col_2", "allowed": [1, 2]}},
    },
]

location = "dqx.config.checks"


def test_load_checks_when_checks_lakebase_table_does_not_exist(ws, spark, connection_string):
    with pytest.raises(NotFound, match=f"Table '{location}' does not exist in the Lakebase instance"):
        dq_engine = DQEngine(ws)
        config = LakebaseChecksStorageConfig(location=location, connection_string=connection_string)
        dq_engine.load_checks(config=config)


def test_save_and_load_checks_from_lakebase_table(ws, make_schema, make_random, spark, connection_string):
    dq_engine = DQEngine(ws)
    config = LakebaseChecksStorageConfig(location=location, connection_string=connection_string)
    dq_engine.save_checks(checks=TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)
    compare_checks(checks, TEST_CHECKS)


def test_save_and_load_checks_from_lakebase_table_with_run_config(ws, spark, connection_string):
    dq_engine = DQEngine(ws, spark)
    run_config_name = "workflow_001"
    config_save = LakebaseChecksStorageConfig(
        location=location, connection_string=connection_string, run_config_name=run_config_name
    )
    dq_engine.save_checks(TEST_CHECKS[:1], config=config_save)
    config_load = LakebaseChecksStorageConfig(
        location=location, connection_string=connection_string, run_config_name=run_config_name
    )
    checks = dq_engine.load_checks(config=config_load)
    compare_checks(checks, TEST_CHECKS[:1])

    run_config_name2 = "workflow_002"
    config_save2 = LakebaseChecksStorageConfig(
        location=location,
        connection_string=connection_string,
        run_config_name=run_config_name2,
        mode="overwrite",
    )
    dq_engine.save_checks(TEST_CHECKS[1:], config=config_save2)
    config_load2 = LakebaseChecksStorageConfig(
        location=location, connection_string=connection_string, run_config_name=run_config_name
    )
    checks = dq_engine.load_checks(config=config_load2)
    compare_checks(checks, TEST_CHECKS[:1])

    dq_engine.save_checks(
        TEST_CHECKS[1:], config=LakebaseChecksStorageConfig(location=location, connection_string=connection_string)
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(location=location, connection_string=connection_string)
    )
    compare_checks(checks, TEST_CHECKS[1:])


def test_save_and_load_checks_to_lakebase_table_output_modes(ws, make_schema, make_random, spark, connection_string):
    dq_engine = DQEngine(ws, spark)
    run_config_name = "workflow_003"
    dq_engine.save_checks(
        TEST_CHECKS[:1],
        config=LakebaseChecksStorageConfig(
            location=location, connection_string=connection_string, run_config_name=run_config_name, mode="append"
        ),
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=location, connection_string=connection_string, run_config_name=run_config_name
        )
    )
    compare_checks(checks, TEST_CHECKS[:1])

    run_config_name = "workflow_004"
    dq_engine.save_checks(
        TEST_CHECKS[1:],
        config=LakebaseChecksStorageConfig(
            location=location, connection_string=connection_string, run_config_name=run_config_name, mode="overwrite"
        ),
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=location, connection_string=connection_string, run_config_name=run_config_name
        )
    )
    compare_checks(checks, TEST_CHECKS[1:])


def test_save_load_checks_from_lakebase_table_in_user_installation(ws, spark, installation_ctx, connection_string):
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = location
    run_config.connection_string = connection_string
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    dq_engine = DQEngine(ws, spark)
    config = InstallationChecksStorageConfig(
        location=location,
        connection_string=connection_string,
        run_config_name=run_config.name,
        assume_user=True,
        product_name=product_name,
    )

    dq_engine.save_checks(TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)
    compare_checks(checks, TEST_CHECKS)
