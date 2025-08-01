import pytest
from databricks.labs.dqx.config import VolumeFileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound, BadRequest

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


def test_load_checks_when_checks_volume_not_volume(ws, make_schema, make_volume, spark):
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


def test_load_checks_when_checks_volume_path_missing_volume(ws, make_random, spark):
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


def test_save_and_load_checks_from_volume(ws, make_schema, make_volume, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume_name = make_volume(catalog_name=catalog_name, schema_name=schema_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checks.yml"

    engine = DQEngine(ws, spark)
    config = VolumeFileChecksStorageConfig(location=volume)
    engine.save_checks(checks=TEST_CHECKS, config=config)
    checks = engine.load_checks(config=config)
    assert checks == EXPECTED_CHECKS, "Checks were not loaded correctly."
