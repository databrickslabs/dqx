import pytest


from databricks.sdk.errors import NotFound, BadRequest
from databricks.labs.dqx.config import VolumeFileChecksStorageConfig, InstallationChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine


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


def test_load_checks_when_volume_checks_file_missing(ws, make_schema, make_volume, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volume/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    with pytest.raises(BadRequest, match="Invalid path: unsupported first path component: Volume"):
        config = VolumeFileChecksStorageConfig(location=volume)
        DQEngine(ws, spark).load_checks(config=config)


def test_load_checks_when_checks_volume_path_missing_schema(ws, make_random, spark):
    volume = f"/Volumes/{make_random(6).lower()}"

    with pytest.raises(BadRequest, match="Invalid path: Path is missing a schema name"):
        config = VolumeFileChecksStorageConfig(location=volume)
        DQEngine(ws, spark).load_checks(config=config)


def test_load_checks_when_checks_volume_path_missing_volume_file(ws, make_random, spark):
    catalog_name = "main"
    volume = f"/Volumes/{catalog_name}/{make_random(6).lower()}"

    with pytest.raises(BadRequest, match="Invalid path: Path is missing a volume name"):
        config = VolumeFileChecksStorageConfig(location=volume)
        DQEngine(ws, spark).load_checks(config=config)


def test_load_checks_when_checks_volume_does_not_exist_with_no_file(ws, make_schema, make_volume, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

    with pytest.raises(NotFound, match=f"Checks file {volume} missing: The file being accessed is not found."):
        config = VolumeFileChecksStorageConfig(location=volume)
        DQEngine(ws, spark).load_checks(config=config)


def test_load_checks_when_checks_volume_does_not_exist(ws, make_schema, make_volume, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    with pytest.raises(NotFound, match=f"Checks file {volume} missing: The file being accessed is not found."):
        config = VolumeFileChecksStorageConfig(location=volume)
        DQEngine(ws, spark).load_checks(config=config)


def test_save_and_load_checks_from_volume(ws, make_schema, make_volume):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    engine = DQEngine(ws)
    config = VolumeFileChecksStorageConfig(location=volume)
    engine.save_checks(checks=TEST_CHECKS, config=config)
    checks = engine.load_checks(config=config)
    assert checks == EXPECTED_CHECKS, "Checks were not loaded correctly."


def test_load_checks_from_installation_when_checks_file_does_not_exist_in_volume(
    ws, installation_ctx, make_schema, make_volume
):
    installation_ctx.installation.save(installation_ctx.config)
    catalog_name = "main"
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
        DQEngine(ws).load_checks(config=config)


def test_save_load_checks_from_volume_in_user_installation(ws, installation_ctx, make_schema, make_volume):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    config = installation_ctx.config
    run_config = config.get_run_config()
    run_config.checks_location = volume
    installation_ctx.installation.save(installation_ctx.config)
    product_name = installation_ctx.product_info.product_name()

    dq_engine = DQEngine(ws)
    config = InstallationChecksStorageConfig(
        run_config_name=run_config.name, assume_user=True, product_name=product_name
    )
    dq_engine.save_checks(TEST_CHECKS, config=config)

    checks = dq_engine.load_checks(config=config)
    assert checks == EXPECTED_CHECKS, "Checks were not saved correctly"


def test_load_checks_when_user_installation_missing(ws):
    with pytest.raises(NotFound):
        config = InstallationChecksStorageConfig(run_config_name="default", assume_user=True)
        DQEngine(ws).load_checks(config=config)


def test_load_checks_from_yaml_file(ws, make_schema, make_volume, make_volume_check_file_as_yaml, expected_checks):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    make_volume_check_file_as_yaml(install_dir=install_dir)

    checks = DQEngine(ws).load_checks(
        config=VolumeFileChecksStorageConfig(location=f"{install_dir}/checks.yaml"),
    )

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_checks_from_json_file(ws, make_schema, make_volume, make_volume_check_file_as_json, expected_checks):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    make_volume_check_file_as_json(install_dir=install_dir)

    checks = DQEngine(ws).load_checks(
        config=VolumeFileChecksStorageConfig(location=f"{install_dir}/checks.json"),
    )

    assert checks == expected_checks, "Checks were not loaded correctly"


def test_load_invalid_checks_from_yaml_file(ws, make_schema, make_volume, make_volume_invalid_check_file_as_yaml):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    volume_file_path = make_volume_invalid_check_file_as_yaml(install_dir=install_dir)

    with pytest.raises(ValueError, match=f"Invalid checks in file: {volume_file_path}"):
        DQEngine(ws).load_checks(
            config=VolumeFileChecksStorageConfig(location=f"{install_dir}/checks.yaml"),
        )


def test_load_invalid_checks_from_json_file(ws, make_schema, make_volume, make_volume_invalid_check_file_as_json):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    volume_file_path = make_volume_invalid_check_file_as_json(install_dir=install_dir)

    with pytest.raises(ValueError, match=f"Invalid checks in file: {volume_file_path}"):
        DQEngine(ws).load_checks(
            config=VolumeFileChecksStorageConfig(location=f"{install_dir}/checks.json"),
        )


def test_save_checks_in_volume_file_as_yml(ws, make_schema, make_volume, installation_ctx):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

    dq_engine = DQEngine(ws)
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
    dq_engine.save_checks(TEST_CHECKS, config=VolumeFileChecksStorageConfig(location=checks_path))

    checks = dq_engine.load_checks(config=VolumeFileChecksStorageConfig(location=checks_path))
    assert TEST_CHECKS == checks, "Checks were not saved correctly"


def test_save_checks_in_volume_file_as_json(ws, make_schema, make_volume, installation_ctx):
    installation_ctx.config.run_configs[0].checks_location = "checks.json"
    installation_ctx.installation.save(installation_ctx.config)
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    install_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

    dq_engine = DQEngine(ws)
    checks_path = f"{install_dir}/{installation_ctx.config.get_run_config().checks_location}"
    dq_engine.save_checks(TEST_CHECKS, config=VolumeFileChecksStorageConfig(location=checks_path))

    checks = dq_engine.load_checks(config=VolumeFileChecksStorageConfig(location=checks_path))
    assert TEST_CHECKS == checks, "Checks were not saved correctly"
