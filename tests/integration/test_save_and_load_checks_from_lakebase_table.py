import yaml

import pytest

from databricks.labs.dqx.config import InstallationChecksStorageConfig, LakebaseChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk.errors import NotFound

from tests.conftest import compare_checks
from tests.integration.test_save_and_load_checks_from_table import EXPECTED_CHECKS as TEST_CHECKS


def test_load_checks_when_lakebase_table_does_not_exist(ws, spark, make_lakebase_instance, user, location):
    instance_name = make_lakebase_instance()
    dq_engine = DQEngine(ws, spark)
    config = LakebaseChecksStorageConfig(location=location, user=user, instance_name=instance_name)

    with pytest.raises(NotFound, match=f"Table '{config.location}' does not exist in the Lakebase instance"):
        dq_engine.load_checks(config=config)


def test_save_and_load_checks_from_lakebase_table(ws, spark, make_lakebase_instance, user, location):
    instance_name = make_lakebase_instance()
    dq_engine = DQEngine(ws, spark)
    config = LakebaseChecksStorageConfig(location=location, user=user, instance_name=instance_name)
    dq_engine.save_checks(checks=TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)
    compare_checks(checks, TEST_CHECKS)


def test_save_and_load_checks_from_lakebase_table_with_run_config(ws, spark, make_lakebase_instance, user, location):
    instance_name = make_lakebase_instance()
    dq_engine = DQEngine(ws, spark)

    # test first run config
    run_config_name = "workflow_001"
    config_save = LakebaseChecksStorageConfig(
        location=location, user=user, instance_name=instance_name, run_config_name=run_config_name
    )
    dq_engine.save_checks(TEST_CHECKS[:1], config=config_save)
    config_load = LakebaseChecksStorageConfig(
        location=location, user=user, instance_name=instance_name, run_config_name=run_config_name
    )
    checks = dq_engine.load_checks(config=config_load)
    compare_checks(checks, TEST_CHECKS[:1])

    # test second run config
    run_config_name = "workflow_002"
    config_save = LakebaseChecksStorageConfig(
        location=location, user=user, instance_name=instance_name, run_config_name=run_config_name
    )
    dq_engine.save_checks(TEST_CHECKS[1:], config=config_save)
    config_load = LakebaseChecksStorageConfig(
        location=location, user=user, instance_name=instance_name, run_config_name=run_config_name
    )
    checks = dq_engine.load_checks(config=config_load)
    compare_checks(checks, TEST_CHECKS[1:])

    # test default config
    dq_engine.save_checks(
        TEST_CHECKS[1:],
        config=LakebaseChecksStorageConfig(location=location, user=user, instance_name=instance_name),
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(location=location, user=user, instance_name=instance_name)
    )
    compare_checks(checks, TEST_CHECKS[1:])


def test_save_and_load_checks_from_lakebase_table_with_output_modes(
    ws,
    spark,
    make_lakebase_instance,
    user,
    location,
):
    instance_name = make_lakebase_instance()
    dq_engine = DQEngine(ws, spark)

    run_config_name = "workflow_003"
    dq_engine.save_checks(
        TEST_CHECKS[:1],
        config=LakebaseChecksStorageConfig(
            location=location,
            instance_name=instance_name,
            user=user,
            run_config_name=run_config_name,
            mode="append",
        ),
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=location, instance_name=instance_name, user=user, run_config_name=run_config_name
        )
    )
    compare_checks(checks, TEST_CHECKS[:1])

    run_config_name = "workflow_004"
    dq_engine.save_checks(
        TEST_CHECKS[1:],
        config=LakebaseChecksStorageConfig(
            location=location,
            instance_name=instance_name,
            user=user,
            run_config_name=run_config_name,
            mode="overwrite",
        ),
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=location, instance_name=instance_name, user=user, run_config_name=run_config_name
        )
    )
    compare_checks(checks, TEST_CHECKS[1:])


def test_save_and_load_checks_from_lakebase_table_with_user_installation(
    ws, spark, installation_ctx, make_lakebase_instance, user, location
):
    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = location
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    instance_name = make_lakebase_instance()

    config = InstallationChecksStorageConfig(
        instance_name=instance_name,
        user=user,
        run_config_name=run_config.name,
        product_name=product_name,
        assume_user=True,
    )

    dq_engine = DQEngine(ws, spark)
    dq_engine.save_checks(TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)
    compare_checks(checks, TEST_CHECKS)


def test_save_and_load_checks_from_lakebase_table_with_profiler(ws, spark, make_lakebase_instance, df, user, location):
    profiler = DQProfiler(ws)
    _, profiles = profiler.profile(df)
    generator = DQGenerator(ws)
    checks = generator.generate_dq_rules(profiles)

    instance_name = make_lakebase_instance()
    dq_engine = DQEngine(ws, spark)

    dq_engine.save_checks(
        checks, config=LakebaseChecksStorageConfig(location=location, instance_name=instance_name, user=user)
    )
    loaded_checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(location=location, instance_name=instance_name, user=user)
    )
    compare_checks(loaded_checks, checks)


def test_save_and_load_checks_from_lakebase_table_with_check_validaton(
    ws, spark, make_lakebase_instance, user, location
):
    dq_engine = DQEngine(ws, spark)

    checks = yaml.safe_load(
        """ 
    - criticality: error
    check:
        function: is_not_null_and_not_empty
        arguments:
        column: name

    - criticality: error
    check:
        function: is_not_null
        for_each_column:
        - region
        - state
    """
    )

    instance_name = make_lakebase_instance()

    dq_engine.save_checks(
        checks, config=LakebaseChecksStorageConfig(location=location, instance_name=instance_name, user=user)
    )
    loaded_checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(location=location, instance_name=instance_name, user=user)
    )
    assert not dq_engine.validate_checks(loaded_checks).has_errors
