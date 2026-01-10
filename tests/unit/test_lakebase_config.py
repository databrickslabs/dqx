"""Unit tests for LakebaseConnectionConfig and LakebaseChecksStorageConfig."""

import pytest
from databricks.labs.dqx.config import LakebaseChecksStorageConfig
from databricks.labs.dqx.errors import InvalidParameterError, InvalidConfigError


def test_create_valid_lakebase_check_storage_config():
    """Test creating LakebaseConnectionConfig with valid parameters."""
    config = LakebaseChecksStorageConfig(
        instance_name="testhost",
        location="testdb.testschema.testtable",
        client_id="00000000-0000-0000-0000-000000000000",
        port="5432",
    )

    assert config.instance_name == "testhost"
    assert config.database_name == "testdb"
    assert config.client_id == "00000000-0000-0000-0000-000000000000"
    assert config.port == "5432"


def test_create_lakebase_check_storage_config_default_port():
    """Test creating LakebaseConnectionConfig with default port."""
    config = LakebaseChecksStorageConfig(
        instance_name="testhost",
        location="testdb.testschema.testtable",
        client_id="00000000-0000-0000-0000-000000000000",
    )

    assert config.instance_name == "testhost"
    assert config.database_name == "testdb"
    assert config.client_id == "00000000-0000-0000-0000-000000000000"
    assert config.port == "5432"  # Default port


@pytest.mark.parametrize(
    "instance_name,location,client_id,port,expected_error",
    [
        # Empty/None instance_name
        ("", "testdb.testschema.testtable", "client_id", "5432", "Instance name must not be empty or None."),
        (None, "testdb.testschema.testtable", "client_id", "5432", "Instance name must not be empty or None."),
        # Empty/None database
        ("instance", "", "client_id", "5432", "Location must not be empty or None."),
        ("instance", None, "client_id", "5432", "Location must not be empty or None."),
    ],
)
def test_create_invalid_lakebase_check_storage_config(instance_name, location, client_id, port, expected_error):
    """Test LakebaseChecksStorageConfig validation with invalid parameters."""
    with pytest.raises(InvalidParameterError, match=expected_error):
        LakebaseChecksStorageConfig(instance_name=instance_name, location=location, client_id=client_id, port=port)


def test_valid_lakebase_checks_storage_config():
    """Test creating LakebaseChecksStorageConfig with valid parameters."""
    config = LakebaseChecksStorageConfig(
        instance_name="testinstance",
        location="testdb.testschema.testtable",
        client_id="00000000-0000-0000-0000-000000000000",
        port="5432",
        run_config_name="test_run",
        mode="append",
    )

    assert config.instance_name == "testinstance"
    assert config.location == "testdb.testschema.testtable"
    assert config.client_id == "00000000-0000-0000-0000-000000000000"
    assert config.port == "5432"
    assert config.run_config_name == "test_run"
    assert config.mode == "append"
    assert config.database_name == "testdb"
    assert config.schema_name == "testschema"
    assert config.table_name == "testtable"


def test_valid_lakebase_checks_storage_config_defaults():
    """Test creating LakebaseChecksStorageConfig with default values."""
    config = LakebaseChecksStorageConfig(
        instance_name="testinstance",
        location="testdb.testschema.testtable",
        client_id="00000000-0000-0000-0000-000000000000",
    )

    assert config.run_config_name == "default"
    assert config.mode == "overwrite"
    assert config.port == "5432"  # Default port


@pytest.mark.parametrize(
    "location,expected_error",
    [
        # Empty/None location
        ("", "Location must not be empty or None."),
        (None, "Location must not be empty or None."),
    ],
)
def test_invalid_location_empty(location, expected_error):
    """Test LakebaseChecksStorageConfig with empty/None location."""
    with pytest.raises(InvalidParameterError, match=expected_error):
        LakebaseChecksStorageConfig(
            instance_name="testinstance", location=location, client_id="00000000-0000-0000-0000-000000000000"
        )


@pytest.mark.parametrize(
    "location,expected_error",
    [
        # Invalid location format (not 3 parts)
        ("db", "Invalid Lakebase table name 'db'. Must be in the format 'database.schema.table'."),
        ("db.schema", "Invalid Lakebase table name 'db.schema'. Must be in the format 'database.schema.table'."),
        (
            "db.schema.table.extra",
            "Invalid Lakebase table name 'db.schema.table.extra'. Must be in the format 'database.schema.table'.",
        ),
    ],
)
def test_invalid_location_format(location, expected_error):
    """Test LakebaseChecksStorageConfig with invalid location format."""
    with pytest.raises(InvalidConfigError, match=expected_error):
        LakebaseChecksStorageConfig(
            instance_name="testinstance", location=location, client_id="00000000-0000-0000-0000-000000000000"
        )


@pytest.mark.parametrize(
    "instance_name,expected_error",
    [
        ("", "Instance name must not be empty or None."),
        (None, "Instance name must not be empty or None."),
    ],
)
def test_invalid_instance_name(instance_name, expected_error):
    """Test LakebaseChecksStorageConfig with invalid instance name."""
    with pytest.raises(InvalidParameterError, match=expected_error):
        LakebaseChecksStorageConfig(
            instance_name=instance_name,
            location="testdb.testschema.testtable",
            client_id="00000000-0000-0000-0000-000000000000",
        )


@pytest.mark.parametrize(
    "mode,expected_error",
    [
        # Invalid modes
        ("invalid", "Invalid mode 'invalid'. Must be 'append' or 'overwrite'."),
        ("INSERT", "Invalid mode 'INSERT'. Must be 'append' or 'overwrite'."),
        ("update", "Invalid mode 'update'. Must be 'append' or 'overwrite'."),
        ("delete", "Invalid mode 'delete'. Must be 'append' or 'overwrite'."),
        ("", "Invalid mode ''. Must be 'append' or 'overwrite'."),
    ],
)
def test_invalid_mode(mode, expected_error):
    """Test LakebaseChecksStorageConfig with invalid mode."""
    with pytest.raises(InvalidConfigError, match=expected_error):
        LakebaseChecksStorageConfig(
            instance_name="testinstance",
            location="testdb.testschema.testtable",
            client_id="00000000-0000-0000-0000-000000000000",
            mode=mode,
        )


def test_valid_modes():
    """Test LakebaseChecksStorageConfig with valid modes."""
    # Test 'append' mode
    config_append = LakebaseChecksStorageConfig(
        instance_name="testinstance",
        location="testdb.testschema.testtable",
        client_id="00000000-0000-0000-0000-000000000000",
        mode="append",
    )
    assert config_append.mode == "append"

    # Test 'overwrite' mode
    config_overwrite = LakebaseChecksStorageConfig(
        instance_name="testinstance",
        location="testdb.testschema.testtable",
        client_id="00000000-0000-0000-0000-000000000000",
        mode="overwrite",
    )
    assert config_overwrite.mode == "overwrite"


def test_location_properties():
    """Test database_name, schema_name, and table_name properties."""
    config = LakebaseChecksStorageConfig(
        instance_name="testinstance",
        location="testdb.testschema.testtable",
        client_id="00000000-0000-0000-0000-000000000000",
    )

    assert config.database_name == "testdb"
    assert config.schema_name == "testschema"
    assert config.table_name == "testtable"


def test_complex_location_with_special_characters():
    """Test location parsing with special characters in names."""
    config = LakebaseChecksStorageConfig(
        instance_name="testinstance",
        location="test_db.test_schema.test_table",
        client_id="00000000-0000-0000-0000-000000000000",
    )

    assert config.database_name == "test_db"
    assert config.schema_name == "test_schema"
    assert config.table_name == "test_table"


def test_eager_validation_on_init():
    """Test that validation happens eagerly during __post_init__."""
    # This should fail immediately during construction, not later
    with pytest.raises(InvalidParameterError, match="Instance name must not be empty or None"):
        LakebaseChecksStorageConfig(
            instance_name="", location="testdb.testschema.testtable", client_id="00000000-0000-0000-0000-000000000000"
        )


def test_multiple_validation_errors_precedence():
    """Test that the first validation error is raised when multiple issues exist."""
    # Empty location should be caught first, before instance name validation
    with pytest.raises(InvalidParameterError, match="Location must not be empty or None"):
        LakebaseChecksStorageConfig(instance_name="", location="", client_id="00000000-0000-0000-0000-000000000000")
