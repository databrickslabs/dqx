import pytest
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk.errors import NotFound

TEST_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_null", "for_each_column": ["col1", "col2"], "arguments": {}},
    }
]

EXPECTED_CHECKS = [
    {
        "criticality": "error",
        "check": {"function": "is_not_empty", "for_each_column": ["col1", "col2"], "arguments": {}},
    }
]

def test_load_checks_when_checks_volume_not_volume(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume = f"/Volume/{catalog_name}/{schema_name}/{make_random(6).lower()}"

    with pytest.raises(ValueError, match=f"Path must start with '/Volumes/': {volume}"):
        engine = DQEngine(ws, spark)
        engine.load_checks_from_uc_volume(volume)

def test_load_checks_when_checks_volume_path_missing_catalog_schema(ws, make_schema, make_random, spark):
    volume = f"/Volumes/{make_random(6).lower()}"

    with pytest.raises(ValueError, match="Path must be at least '/Volumes/<catalog>/<schema>/<volume>/..."):
        engine = DQEngine(ws, spark)
        engine.load_checks_from_uc_volume(volume)

def test_load_checks_when_checks_volume_path_missing_schema(ws, make_schema, make_random, spark):
    catalog_name = "main"
    volume = f"/Volumes/{catalog_name}/{make_random(6).lower()}"

    with pytest.raises(ValueError, match="Path must be at least '/Volumes/<catalog>/<schema>/<volume>/..."):
        engine = DQEngine(ws, spark)
        engine.load_checks_from_uc_volume(volume)

def test_load_checks_when_checks_volume_path_missing_volume(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}"

    with pytest.raises(ValueError, match="Path must be at least '/Volumes/<catalog>/<schema>/<volume>/..."):
        engine = DQEngine(ws, spark)
        engine.load_checks_from_uc_volume(volume)

def test_load_checks_when_checks_volume_does_not_exist(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{make_random(6).lower()}"

    with pytest.raises(NotFound, match=f"Provided Volume path does not exists: {volume}"):
        engine = DQEngine(ws, spark)
        engine.load_checks_from_uc_volume(volume)

def test_save_and_load_checks_from_uc_volume(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{make_random(6).lower()}/checks.yml"

    engine = DQEngine(ws, spark)
    engine.save_checks_in_uc_volume(TEST_CHECKS, volume)
    checks = engine.load_checks_from_uc_volume(volume)
    assert checks == EXPECTED_CHECKS, "Checks were not loaded correctly."