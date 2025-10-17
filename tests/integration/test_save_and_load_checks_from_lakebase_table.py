import re
from unittest import skip

import pytest

from databricks.labs.dqx.config import InstallationChecksStorageConfig, LakebaseChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound

from tests.conftest import compare_checks
from tests.integration.test_save_and_load_checks_from_table import EXPECTED_CHECKS as TEST_CHECKS


def test_load_checks_when_lakebase_table_does_not_exist(
    ws, spark, make_shared_lakebase_instance, lakebase_user, make_random
):
    instance = make_shared_lakebase_instance(ws, make_random)
    dq_engine = DQEngine(ws, spark)

    lakebase_location = _create_lakebase_location(instance.database_name, make_random)
    config = LakebaseChecksStorageConfig(location=lakebase_location, user=lakebase_user, instance_name=instance.name)

    with pytest.raises(NotFound, match=f"Table '{config.location}' does not exist in the Lakebase instance"):
        dq_engine.load_checks(config=config)


def test_save_and_load_checks_from_lakebase_table(ws, spark, make_shared_lakebase_instance, lakebase_user, make_random):
    instance = make_shared_lakebase_instance(ws, make_random)
    dq_engine = DQEngine(ws, spark)

    lakebase_location = _create_lakebase_location(instance.database_name, make_random)
    config = LakebaseChecksStorageConfig(location=lakebase_location, user=lakebase_user, instance_name=instance.name)

    dq_engine.save_checks(checks=TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)

    compare_checks(checks, TEST_CHECKS)


def test_save_and_load_checks_from_lakebase_table_with_run_config(
    ws, spark, make_shared_lakebase_instance, lakebase_user, make_random
):
    instance = make_shared_lakebase_instance(ws, make_random)
    dq_engine = DQEngine(ws, spark)

    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    # test first run config
    config = LakebaseChecksStorageConfig(
        location=lakebase_location, user=lakebase_user, instance_name=instance.name, run_config_name="workflow_001"
    )
    dq_engine.save_checks(TEST_CHECKS[:1], config=config)
    checks = dq_engine.load_checks(config=config)

    compare_checks(checks, TEST_CHECKS[:1])

    # test second run config
    second_config = LakebaseChecksStorageConfig(
        location=lakebase_location, user=lakebase_user, instance_name=instance.name, run_config_name="workflow_002"
    )
    dq_engine.save_checks(TEST_CHECKS[1:], config=second_config)
    checks = dq_engine.load_checks(config=second_config)

    compare_checks(checks, TEST_CHECKS[1:])


def test_save_and_load_checks_from_lakebase_table_with_output_modes(
    ws, spark, make_shared_lakebase_instance, lakebase_user, make_random
):
    instance = make_shared_lakebase_instance(ws, make_random)
    dq_engine = DQEngine(ws, spark)

    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    run_config_name = "workflow_003"
    dq_engine.save_checks(
        TEST_CHECKS[:1],
        config=LakebaseChecksStorageConfig(
            location=lakebase_location,
            user=lakebase_user,
            instance_name=instance.name,
            run_config_name=run_config_name,
            mode="append",
        ),
    )

    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=lakebase_location,
            user=lakebase_user,
            instance_name=instance.name,
            run_config_name=run_config_name,
        )
    )
    compare_checks(checks, TEST_CHECKS[:1])

    run_config_name = "workflow_004"
    dq_engine.save_checks(
        TEST_CHECKS[1:],
        config=LakebaseChecksStorageConfig(
            location=lakebase_location,
            user=lakebase_user,
            instance_name=instance.name,
            run_config_name=run_config_name,
            mode="overwrite",
        ),
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=lakebase_location,
            user=lakebase_user,
            instance_name=instance.name,
            run_config_name=run_config_name,
        )
    )
    compare_checks(checks, TEST_CHECKS[1:])


def test_save_and_load_checks_from_lakebase_table_with_user_installation(
    ws, spark, installation_ctx, make_shared_lakebase_instance, lakebase_user, make_random
):
    instance = make_shared_lakebase_instance(ws, make_random)

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = _create_lakebase_location(instance.database_name, make_random)
    installation_ctx.installation.save(config)
    product_name = installation_ctx.product_info.product_name()

    config = InstallationChecksStorageConfig(
        instance_name=instance.name,
        user=lakebase_user,
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    dq_engine = DQEngine(ws, spark)

    dq_engine.save_checks(TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)

    compare_checks(checks, TEST_CHECKS)


def test_profiler_workflow_save_to_lakebase(
    ws, spark, setup_workflows, make_shared_lakebase_instance, lakebase_user, make_random
):
    installation_ctx, run_config = setup_workflows()

    instance = make_shared_lakebase_instance(ws, make_random)

    lakebase_location = _create_lakebase_location(instance.database_name, make_random)

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = lakebase_location
    run_config.lakebase_user = lakebase_user
    run_config.lakebase_instance_name = instance.name
    run_config.lakebase_port = "5432"

    installation_ctx.installation.save(config)

    installation_ctx.deployed_workflows.run_workflow("profiler", run_config.name)

    dq_engine = DQEngine(ws, spark)
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=lakebase_location, instance_name=instance.name, user=lakebase_user, run_config_name=run_config.name
        )
    )
    assert checks, "Checks are missing"


@skip("ad-hoc only")
def test_delete_all_leftover_lakebase_instances(ws):
    pattern = re.compile(r"^dqx-test-[A-Za-z0-9]{10}$")

    instances = []

    for instance in ws.database.list_database_instances():
        if pattern.match(instance.name):
            instances.append(instance.name)

    for instance in instances:
        ws.database.delete_database_instance(name=instance)


def _create_lakebase_location(database_name, make_random):
    table_name = f"checks-{make_random(10).lower()}"
    location = f"{database_name}.config.{table_name}"
    return location
