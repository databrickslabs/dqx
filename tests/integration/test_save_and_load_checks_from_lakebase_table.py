import pytest

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from databricks.labs.dqx.config import InstallationChecksStorageConfig, LakebaseChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk.errors import NotFound

from tests.conftest import compare_checks
from tests.integration.test_save_and_load_checks_from_table import EXPECTED_CHECKS as TEST_CHECKS

LOCATION = "dqx.config.checks"


def test_load_checks_when_lakebase_table_does_not_exist(ws, spark, make_lakebase_instance_and_catalog, lakebase_user):
    instance_name = make_lakebase_instance_and_catalog()
    config = LakebaseChecksStorageConfig(location=LOCATION, user=lakebase_user, instance_name=instance_name)

    with pytest.raises(NotFound, match=f"Table '{config.location}' does not exist in the Lakebase instance"):
        dq_engine = DQEngine(ws, spark)
        config = LakebaseChecksStorageConfig(location=LOCATION, user=lakebase_user, instance_name=instance_name)
        dq_engine.load_checks(config=config)


def test_save_and_load_checks_from_lakebase_table(ws, make_lakebase_instance_and_catalog, spark, lakebase_user):
    dq_engine = DQEngine(ws, spark)
    instance_name = make_lakebase_instance_and_catalog()
    config = LakebaseChecksStorageConfig(location=LOCATION, user=lakebase_user, instance_name=instance_name)
    dq_engine.save_checks(checks=TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)
    compare_checks(checks, TEST_CHECKS)


def test_save_and_load_checks_from_lakebase_table_with_run_config(
    ws, spark, make_lakebase_instance_and_catalog, lakebase_user
):
    instance_name = make_lakebase_instance_and_catalog()
    dq_engine = DQEngine(ws, spark)

    # test first run config
    run_config_name = "workflow_001"
    config_save = LakebaseChecksStorageConfig(
        location=LOCATION, user=lakebase_user, instance_name=instance_name, run_config_name=run_config_name
    )
    dq_engine.save_checks(TEST_CHECKS[:1], config=config_save)
    config_load = LakebaseChecksStorageConfig(
        location=LOCATION, user=lakebase_user, instance_name=instance_name, run_config_name=run_config_name
    )
    checks = dq_engine.load_checks(config=config_load)
    compare_checks(checks, TEST_CHECKS[:1])

    # test second run config
    run_config_name2 = "workflow_002"
    config_save2 = LakebaseChecksStorageConfig(
        location=LOCATION, user=lakebase_user, instance_name=instance_name, run_config_name=run_config_name2
    )
    dq_engine.save_checks(TEST_CHECKS[1:], config=config_save2)
    config_load2 = LakebaseChecksStorageConfig(
        location=LOCATION, user=lakebase_user, instance_name=instance_name, run_config_name=run_config_name
    )
    checks = dq_engine.load_checks(config=config_load2)
    compare_checks(checks, TEST_CHECKS[:1])

    # test default config
    dq_engine.save_checks(
        TEST_CHECKS[1:],
        config=LakebaseChecksStorageConfig(location=LOCATION, user=lakebase_user, instance_name=instance_name),
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(location=LOCATION, user=lakebase_user, instance_name=instance_name)
    )
    compare_checks(checks, TEST_CHECKS[1:])


def test_save_and_load_checks_to_lakebase_table_output_modes(
    ws,
    spark,
    make_lakebase_instance_and_catalog,
    lakebase_user,
):
    instance_name = make_lakebase_instance_and_catalog()
    dq_engine = DQEngine(ws, spark)

    run_config_name = "workflow_003"
    dq_engine.save_checks(
        TEST_CHECKS[:1],
        config=LakebaseChecksStorageConfig(
            location=LOCATION,
            instance_name=instance_name,
            user=lakebase_user,
            run_config_name=run_config_name,
            mode="append",
        ),
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=LOCATION, instance_name=instance_name, user=lakebase_user, run_config_name=run_config_name
        )
    )
    compare_checks(checks, TEST_CHECKS[:1])

    run_config_name = "workflow_004"
    dq_engine.save_checks(
        TEST_CHECKS[1:],
        config=LakebaseChecksStorageConfig(
            location=LOCATION,
            instance_name=instance_name,
            user=lakebase_user,
            run_config_name=run_config_name,
            mode="overwrite",
        ),
    )
    checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(
            location=LOCATION, instance_name=instance_name, user=lakebase_user, run_config_name=run_config_name
        )
    )
    compare_checks(checks, TEST_CHECKS[1:])


def test_save_load_checks_from_lakebase_table_in_user_installation(
    ws, spark, installation_ctx, make_lakebase_instance_and_catalog, lakebase_user
):
    instance_name = make_lakebase_instance_and_catalog()

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = LOCATION

    installation_ctx.installation.save(installation_ctx.config)

    config = InstallationChecksStorageConfig(
        instance_name=instance_name,
        user=lakebase_user,
        run_config_name=run_config.name,
        product_name="dqx",
        assume_user=True,
    )

    dq_engine = DQEngine(ws, spark)
    dq_engine.save_checks(TEST_CHECKS, config=config)
    checks = dq_engine.load_checks(config=config)
    compare_checks(checks, TEST_CHECKS)


def test_save_and_load_checks_from_profiler(ws, spark, make_lakebase_instance_and_catalog, lakebase_user):
    df = spark.createDataFrame(
        data=[
            (1, None, "2006-04-09", "ymason@example.net", "APAC", "France", "High"),
            (2, "Mark Brooks", "1992-07-27", "johnthomas@example.net", "LATAM", "Trinidad and Tobago", None),
            (3, "Lori Gardner", "2001-01-22", "heather68@example.com", None, None, None),
            (4, None, "1968-10-24", "hollandfrank@example.com", None, "Palau", "Low"),
            (5, "Laura Mitchell DDS", "1968-01-08", "paynebrett@example.org", "NA", "Qatar", None),
            (6, None, "1951-09-11", "williamstracy@example.org", "EU", "Benin", "High"),
            (7, "Benjamin Parrish", "1971-08-17", "hmcpherson@example.net", "EU", "Kazakhstan", "Medium"),
            (8, "April Hamilton", "1989-09-04", "adamrichards@example.net", "EU", "Saint Lucia", None),
            (9, "Stephanie Price", "1975-03-01", "ktrujillo@example.com", "NA", "Togo", "High"),
            (10, "Jonathan Sherman", "1976-04-13", "charles93@example.org", "NA", "Japan", "Low"),
        ],
        schema=StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("birthdate", StringType(), True),
                StructField("email", StringType(), True),
                StructField("region", StringType(), True),
                StructField("state", StringType(), True),
                StructField("tier", StringType(), True),
            ]
        ),
    )
    profiler = DQProfiler(ws)
    generator = DQGenerator(ws)
    dq_engine = DQEngine(ws, spark)
    _, profiles = profiler.profile(df)
    checks = generator.generate_dq_rules(profiles)
    instance_name = make_lakebase_instance_and_catalog()
    dq_engine.save_checks(
        checks, config=LakebaseChecksStorageConfig(location=LOCATION, instance_name=instance_name, user=lakebase_user)
    )
    loaded_checks = dq_engine.load_checks(
        config=LakebaseChecksStorageConfig(location=LOCATION, instance_name=instance_name, user=lakebase_user)
    )
    compare_checks(loaded_checks, checks)


def delete_all_leftover_instances(ws):
    import re

    pattern = re.compile(r"^dqxtest-[A-Za-z0-9]{10}$")

    instances = []

    for instance in ws.database.list_database_instances():
        if pattern.match(instance.name):
            instances.append(instance.name)

    for instance in instances:
        ws.database.delete_database_instance(name=instance)
