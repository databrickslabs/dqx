import pytest
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.checks_handlers import ChecksHandler
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
        engine.load_checks(volume, source_type="uc_volume")


def test_load_checks_when_checks_volume_path_missing_catalog_schema(ws, make_schema, make_random, spark):
    volume = f"/Volumes/{make_random(6).lower()}"

    with pytest.raises(ValueError, match="Path must be at least '/Volumes/<catalog>/<schema>/<volume>/..."):
        engine = DQEngine(ws, spark)
        engine.load_checks(volume, source_type="uc_volume")


def test_load_checks_when_checks_volume_path_missing_schema(ws, make_schema, make_random, spark):
    catalog_name = "main"
    volume = f"/Volumes/{catalog_name}/{make_random(6).lower()}"

    with pytest.raises(ValueError, match="Path must be at least '/Volumes/<catalog>/<schema>/<volume>/..."):
        engine = DQEngine(ws, spark)
        engine.load_checks(volume, source_type="uc_volume")


def test_load_checks_when_checks_volume_path_missing_volume(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}"

    with pytest.raises(ValueError, match="Path must be at least '/Volumes/<catalog>/<schema>/<volume>/..."):
        engine = DQEngine(ws, spark)
        engine.load_checks(volume, source_type="uc_volume")


def test_load_checks_when_checks_volume_does_not_exist(ws, make_schema, make_random, spark):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{make_random(6).lower()}"

    with pytest.raises(NotFound, match=f"Provided Volume path does not exists: {volume}"):
        engine = DQEngine(ws, spark)
        engine.load_checks(volume, source_type="uc_volume")


def test_load_checks_from_uc_volume_parsed_checks_error(ws, make_schema, make_random, spark, mocker):
    # Arrange
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{make_random(6).lower()}/checks.yml"

    engine = DQEngine(ws, spark)

    # Patch path validation: do nothing when called
    mocker.patch.object(engine._checks_handlers["uc_volume"], "_is_volume_path", return_value=True)
    # Patch os.makedirs
    mocker.patch("os.makedirs")
    # Patch file open to simulate empty file (yaml.safe_load returns None)
    mock_file = mocker.mock_open(read_data="")
    mocker.patch("builtins.open", mock_file)

    # Act & Assert
    with pytest.raises(ValueError, match=f"Invalid or no checks in UC volume file: {volume}"):
        engine.load_checks(volume, source_type="uc_volume")


def test_save_and_load_checks_from_uc_volume(ws, make_schema, make_random, spark, mocker):
    # Arrange: Prepare volume path and test data
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    volume = f"/Volumes/{catalog_name}/{schema_name}/{make_random(6).lower()}/checks.yml"

    #TEST_CHECKS = [{"name": "checkA", "check": "colA IS NOT NULL"}]
    #EXPECTED_CHECKS = [{"name": "checkA", "check": "colA IS NOT NULL"}]

    engine = DQEngine(ws, spark)

    # Patch path validation in the UC volume handler used by the engine
    mocker.patch.object(engine._checks_handlers["uc_volume"], "_is_volume_path", return_value=True)

    # Patch os.makedirs so no directories are created
    mocker.patch("os.makedirs")

    # Patch open to intercept file read/write operations
    mock_file = mocker.mock_open()
    mocker.patch("builtins.open", mock_file)

    # Patch yaml.safe_dump and yaml.safe_load
    mock_yaml_dump = mocker.patch("yaml.safe_dump")
    mocker.patch("yaml.safe_load", return_value=EXPECTED_CHECKS)

    # Act: Save checks to file and then load them back
    engine.save_checks(TEST_CHECKS, volume, target_type="uc_volume")
    checks = engine.load_checks(volume, source_type="uc_volume")

    # Assert: File was opened for writing with UTF-8 encoding
    mock_file.assert_any_call(volume, "w", encoding="utf-8")

    # Assert: yaml.safe_dump was called with the correct data
    mock_yaml_dump.assert_called_once_with(TEST_CHECKS, mocker.ANY, allow_unicode=True)

    # Assert: The loaded checks match EXPECTED_CHECKS
    assert checks == EXPECTED_CHECKS, "Checks were not loaded correctly."