import pytest


from databricks.sdk.errors import NotFound

from databricks.labs.dqx.config import VolumeFileChecksStorageConfig, InstallationChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.errors import InvalidCheckError, InvalidParameterError

from tests.conftest import TEST_CATALOG


TEST_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    }
]

EXPECTED_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    }
]


def test_load_checks_from_volume_file_missing(ws, make_schema, make_volume, spark):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    with pytest.raises(NotFound, match=f"Checks file {volume} missing"):
        config = VolumeFileChecksStorageConfig(location=volume)
        DQEngine(ws, spark).load_checks(config=config)


def test_load_checks_from_volumes_volume_missing_in_path(ws, make_random, spark):
    volume = f"/{make_random(10).lower()}/"

    with pytest.raises(InvalidParameterError, match="The volume path must start with '/Volumes/'"):
        VolumeFileChecksStorageConfig(location=volume)


def test_load_checks_from_volume_catalog_missing_in_path(ws, make_random, spark):
    volume = "/Volumes/"

    with pytest.raises(InvalidParameterError, match="Invalid path: Path is missing a catalog name"):
        VolumeFileChecksStorageConfig(location=volume)


def test_load_checks_from_volume_schema_missing_in_path(ws, make_random, spark):
    volume = f"/Volumes/{make_random(10).lower()}/"

    with pytest.raises(InvalidParameterError, match="Invalid path: Path is missing a schema name"):
        VolumeFileChecksStorageConfig(location=volume)


def test_load_checks_from_volume_file_missing_in_path(ws, make_random, spark):
    catalog_name = TEST_CATALOG
    volume = f"/Volumes/{catalog_name}/{make_random(10).lower()}"

    with pytest.raises(InvalidParameterError, match="Invalid path: Path is missing a volume name"):
        VolumeFileChecksStorageConfig(location=volume)


def test_load_checks_from_volume_file_missing_in_dir_path(ws, make_schema, make_volume, spark):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/dir"

    with pytest.raises(InvalidParameterError, match="Invalid path: Path must include a file name after the volume"):
        VolumeFileChecksStorageConfig(location=volume)


def test_load_checks_from_missing_volume(ws, make_schema, make_volume, spark):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    with pytest.raises(NotFound, match=f"Checks file {volume} missing"):
        config = VolumeFileChecksStorageConfig(location=volume)
        DQEngine(ws, spark).load_checks(config=config)


def test_save_and_load_checks_from_volume(ws, spark, make_schema, make_volume):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    engine = DQEngine(ws, spark)
    config = VolumeFileChecksStorageConfig(location=volume)
    engine.save_checks(checks=TEST_CHECKS, config=config)
    checks = engine.load_checks(config=config)
    assert checks == EXPECTED_CHECKS, "Checks were not loaded correctly."


def test_load_checks_from_volume_in_installation_when_checks_file_does_not_exist(
    ws, spark, installation_ctx, make_schema, make_volume
):
    installation_ctx.installation.save(installation_ctx.config)
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = volume
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    with pytest.raises(NotFound, match=f"Checks file {volume} missing"):
        config = InstallationChecksStorageConfig(
            run_config_name=run_config.name, assume_user=True, product_name=product_name
        )
        DQEngine(ws, spark).load_checks(config=config)


def test_save_load_checks_from_volume_in_user_installation(ws, spark, installation_ctx, make_schema, make_volume):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = volume
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    dq_engine = DQEngine(ws, spark)
    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name, assume_user=True, product_name=product_name
    )
    dq_engine.save_checks(TEST_CHECKS, config=config)

    checks = dq_engine.load_checks(config=config)
    assert checks == EXPECTED_CHECKS, "Checks were not saved correctly"


def test_load_checks_from_volume_user_installation_missing(ws, spark, make_random):
    with pytest.raises(NotFound):
        config = InstallationChecksStorageConfig(
            run_config_name="default", assume_user=True, product_name=make_random(10)
        )
        DQEngine(ws, spark).load_checks(config=config)


def test_load_checks_from_volume_as_yaml_file(
    ws, spark, make_schema, make_volume, make_volume_check_file_as_yaml, expected_checks
):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    make_volume_check_file_as_yaml(install_dir=install_dir)

    checks = DQEngine(ws, spark).load_checks(
        config=VolumeFileChecksStorageConfig(location=f"{install_dir}/checks.yaml"),
    )

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_checks_from_volume_as_json_file(
    ws, spark, make_schema, make_volume, make_volume_check_file_as_json, expected_checks
):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    make_volume_check_file_as_json(install_dir=install_dir)

    checks = DQEngine(ws, spark).load_checks(
        config=VolumeFileChecksStorageConfig(location=f"{install_dir}/checks.json"),
    )

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_invalid_checks_from_volume_as_yaml_file(
    ws, spark, make_schema, make_volume, make_volume_invalid_check_file_as_yaml
):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    volume_file_path = make_volume_invalid_check_file_as_yaml(install_dir=install_dir)

    with pytest.raises(InvalidCheckError, match=f"Invalid checks in file: {volume_file_path}"):
        DQEngine(ws, spark).load_checks(
            config=VolumeFileChecksStorageConfig(location=f"{install_dir}/checks.yaml"),
        )


def test_load_invalid_checks_from_volume_as_json_file(
    ws, spark, make_schema, make_volume, make_volume_invalid_check_file_as_json
):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    volume_file_path = make_volume_invalid_check_file_as_json(install_dir=install_dir)

    with pytest.raises(InvalidCheckError, match=f"Invalid checks in file: {volume_file_path}"):
        DQEngine(ws, spark).load_checks(
            config=VolumeFileChecksStorageConfig(location=f"{install_dir}/checks.json"),
        )


def test_save_checks_in_volume_file_as_yml(ws, spark, make_schema, make_volume, installation_ctx):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

    dq_engine = DQEngine(ws, spark)
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
    dq_engine.save_checks(TEST_CHECKS, config=VolumeFileChecksStorageConfig(location=checks_path))

    checks = dq_engine.load_checks(config=VolumeFileChecksStorageConfig(location=checks_path))
    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_in_volume_file_as_json(ws, spark, make_schema, make_volume, installation_ctx):
    installation_ctx.config.run_configs[0].checks_location = "checks.json"
    installation_ctx.installation.save(installation_ctx.config)
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

    dq_engine = DQEngine(ws, spark)
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
    dq_engine.save_checks(TEST_CHECKS, config=VolumeFileChecksStorageConfig(location=checks_path))

    checks = dq_engine.load_checks(config=VolumeFileChecksStorageConfig(location=checks_path))
    assert TEST_CHECKS == checks, "Checks were not saved correctly"


TEST_CHECKS_FILTER = [
    {
        "criticality": "error",
        "filter": None,
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    },
    {
        "criticality": "error",
        "filter": "machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'",
        "check": {"function": "is_not_null", "arguments": {"column": "next_scheduled_date"}},
        "name": "next_scheduled_date_is_null",
    },
]

EXPECTED_CHECKS_FILTER = [
    {
        "criticality": "error",
        "filter": None,
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    },
    {
        "criticality": "error",
        "filter": "machine_id IN ('MCH-002', 'MCH-003') AND maintenance_type = 'preventive'",
        "check": {
            "function": "is_not_null",
            "arguments": {"column": "next_scheduled_date"},
        },
        "name": "next_scheduled_date_is_null",
    },
]


def test_save_and_load_checks_from_volume_with_filters(ws, spark, make_schema, make_volume):

    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    engine = DQEngine(ws, spark)
    config = VolumeFileChecksStorageConfig(location=volume)
    engine.save_checks(checks=TEST_CHECKS_FILTER, config=config)
    checks = engine.load_checks(config=config)
    assert checks == EXPECTED_CHECKS_FILTER, "Checks were not loaded correctly."
