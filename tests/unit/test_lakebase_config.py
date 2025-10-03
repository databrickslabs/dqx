"""Unit tests for LakebaseConnectionConfig and LakebaseChecksStorageConfig."""

import pytest
from databricks.labs.dqx.config import LakebaseConnectionConfig, LakebaseChecksStorageConfig
from databricks.labs.dqx.errors import InvalidParameterError, InvalidConfigError


def test_create_valid_connection_string():
    """Test creating LakebaseConnectionConfig with valid connection string."""
    connection_string = "postgresql://testuser:password@testhost:5432/testdb"
    config = LakebaseConnectionConfig.create(connection_string)

    assert config.instance_name == "testhost"
    assert config.database == "testdb"
    assert config.user == "testuser"
    assert config.port == "5432"


def test_create_valid_connection_string_default_port():
    """Test creating LakebaseConnectionConfig with valid connection string without port."""
    connection_string = "postgresql://testuser:password@testhost/testdb"
    config = LakebaseConnectionConfig.create(connection_string)

    assert config.instance_name == "testhost"
    assert config.database == "testdb"
    assert config.user == "testuser"
    assert config.port == "5432"  # Default port


def test_create_valid_connection_string_with_encoded_username():
    """Test creating LakebaseConnectionConfig with URL-encoded username."""
    connection_string = "postgresql://test%40user:password@testhost:5432/testdb"
    config = LakebaseConnectionConfig.create(connection_string)

    assert config.instance_name == "testhost"
    assert config.database == "testdb"
    assert config.user == "test@user"  # URL-decoded
    assert config.port == "5432"


@pytest.mark.parametrize(
    "connection_string,expected_error",
    [
        # Empty/None connection string
        ("", "Connection string cannot be empty or None."),
        (None, "Connection string cannot be empty or None."),
        # Invalid URL scheme
        ("mysql://user:password@host:5432/db", "Invalid URL scheme 'mysql'. Expected 'postgresql'."),
        ("http://user:password@host:5432/db", "Invalid URL scheme 'http'. Expected 'postgresql'."),
        ("ftp://user:password@host:5432/db", "Invalid URL scheme 'ftp'. Expected 'postgresql'."),
        # Missing username
        (
            "postgresql://:password@host:5432/db",
            "Missing username in connection string: postgresql://:password@host:5432/db",
        ),
        ("postgresql://host:5432/db", "Missing username in connection string: postgresql://host:5432/db"),
        # Missing hostname
        (
            "postgresql://user:password@:5432/db",
            "Missing hostname in connection string: postgresql://user:password@:5432/db",
        ),
        ("postgresql://user:password@/db", "Missing hostname in connection string: postgresql://user:password@/db"),
        # Missing database name
        (
            "postgresql://user:password@host:5432/",
            "Missing database name in connection string: postgresql://user:password@host:5432/",
        ),
        (
            "postgresql://user:password@host:5432",
            "Missing database name in connection string: postgresql://user:password@host:5432",
        ),
    ],
)
def test_create_invalid_connection_strings(connection_string, expected_error):
    """Test LakebaseConnectionConfig.create() with invalid connection strings."""
    with pytest.raises(InvalidParameterError, match=expected_error):
        LakebaseConnectionConfig.create(connection_string)


def test_create_malformed_url():
    """Test creating LakebaseConnectionConfig with malformed URL."""
    with pytest.raises(InvalidParameterError, match="Invalid URL scheme"):
        LakebaseConnectionConfig.create("not-a-valid-url")


@pytest.fixture
def valid_connection_string():
    """Valid connection string for testing."""
    return "postgresql://testuser:password@testhost:5432/testdb"


def test_valid_config(valid_connection_string):
    """Test creating LakebaseChecksStorageConfig with valid parameters."""
    config = LakebaseChecksStorageConfig(
        location="testdb.testschema.testtable",
        connection_string=valid_connection_string,
        run_config_name="test_run",
        mode="append",
    )

    assert config.location == "testdb.testschema.testtable"
    assert config.connection_string == valid_connection_string
    assert config.run_config_name == "test_run"
    assert config.mode == "append"
    assert config.database_name == "testdb"
    assert config.schema_name == "testschema"
    assert config.table_name == "testtable"


def test_valid_config_defaults(valid_connection_string):
    """Test creating LakebaseChecksStorageConfig with default values."""
    config = LakebaseChecksStorageConfig(
        location="testdb.testschema.testtable", connection_string=valid_connection_string
    )

    assert config.run_config_name == "default"
    assert config.mode == "overwrite"


@pytest.mark.parametrize(
    "location,expected_error",
    [
        # Empty/None location
        ("", "The 'location' field must not be empty or None."),
        (None, "The 'location' field must not be empty or None."),
    ],
)
def test_invalid_location_empty(location, expected_error, valid_connection_string):
    """Test LakebaseChecksStorageConfig with empty/None location."""
    with pytest.raises(InvalidParameterError, match=expected_error):
        LakebaseChecksStorageConfig(location=location, connection_string=valid_connection_string)


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
        ("", "The 'location' field must not be empty or None."),
    ],
)
def test_invalid_location_format(location, expected_error, valid_connection_string):
    """Test LakebaseChecksStorageConfig with invalid location format."""
    if location == "":
        # Empty location raises InvalidParameterError
        with pytest.raises(InvalidParameterError, match=expected_error):
            LakebaseChecksStorageConfig(location=location, connection_string=valid_connection_string)
    else:
        # Invalid format raises InvalidConfigError
        with pytest.raises(InvalidConfigError, match=expected_error):
            LakebaseChecksStorageConfig(location=location, connection_string=valid_connection_string)


@pytest.mark.parametrize(
    "connection_string,expected_error_pattern",
    [
        # Invalid connection strings (delegated to LakebaseConnectionConfig.create)
        ("", "Failed to parse connection string '': Connection string cannot be empty or None."),
        (
            "mysql://user:password@host:5432/db",
            "Failed to parse connection string 'mysql://user:password@host:5432/db': Invalid URL scheme 'mysql'. Expected 'postgresql'.",
        ),
        (
            "postgresql://host:5432/db",
            "Failed to parse connection string 'postgresql://host:5432/db': Missing username in connection string",
        ),
        (
            "postgresql://user:password@:5432/db",
            "Failed to parse connection string 'postgresql://user:password@:5432/db': Missing hostname in connection string",
        ),
        (
            "postgresql://user:password@host:5432/",
            "Failed to parse connection string 'postgresql://user:password@host:5432/': Missing database name in connection string",
        ),
    ],
)
def test_invalid_connection_string(connection_string, expected_error_pattern):
    """Test LakebaseChecksStorageConfig with invalid connection strings."""
    with pytest.raises(InvalidParameterError, match=expected_error_pattern):
        LakebaseChecksStorageConfig(location="testdb.testschema.testtable", connection_string=connection_string)


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
def test_invalid_mode(mode, expected_error, valid_connection_string):
    """Test LakebaseChecksStorageConfig with invalid mode."""
    with pytest.raises(InvalidConfigError, match=expected_error):
        LakebaseChecksStorageConfig(
            location="testdb.testschema.testtable", connection_string=valid_connection_string, mode=mode
        )


def test_valid_modes(valid_connection_string):
    """Test LakebaseChecksStorageConfig with valid modes."""
    # Test 'append' mode
    config_append = LakebaseChecksStorageConfig(
        location="testdb.testschema.testtable", connection_string=valid_connection_string, mode="append"
    )
    assert config_append.mode == "append"

    # Test 'overwrite' mode
    config_overwrite = LakebaseChecksStorageConfig(
        location="testdb.testschema.testtable", connection_string=valid_connection_string, mode="overwrite"
    )
    assert config_overwrite.mode == "overwrite"


def test_connection_config_property(valid_connection_string):
    """Test that connection_config property returns correct LakebaseConnectionConfig."""
    config = LakebaseChecksStorageConfig(
        location="testdb.testschema.testtable", connection_string=valid_connection_string
    )

    conn_config = config.connection_config
    assert isinstance(conn_config, LakebaseConnectionConfig)
    assert conn_config.instance_name == "testhost"
    assert conn_config.database == "testdb"
    assert conn_config.user == "testuser"
    assert conn_config.port == "5432"


def test_location_properties(valid_connection_string):
    """Test database_name, schema_name, and table_name properties."""
    config = LakebaseChecksStorageConfig(location="mydb.myschema.mytable", connection_string=valid_connection_string)

    assert config.database_name == "mydb"
    assert config.schema_name == "myschema"
    assert config.table_name == "mytable"


def test_complex_location_with_special_characters(valid_connection_string):
    """Test location parsing with special characters in names."""
    config = LakebaseChecksStorageConfig(location="my_db.my_schema.my_table", connection_string=valid_connection_string)

    assert config.database_name == "my_db"
    assert config.schema_name == "my_schema"
    assert config.table_name == "my_table"


def test_eager_validation_on_init():
    """Test that validation happens eagerly during __post_init__."""
    # This should fail immediately during construction, not later
    with pytest.raises(InvalidParameterError, match="Failed to parse connection string"):
        LakebaseChecksStorageConfig(
            location="testdb.testschema.testtable", connection_string="invalid-connection-string"
        )


def test_multiple_validation_errors_precedence():
    """Test that the first validation error is raised when multiple issues exist."""
    # Empty location should be caught first, before connection string validation
    with pytest.raises(InvalidParameterError, match="The 'location' field must not be empty or None"):
        LakebaseChecksStorageConfig(location="", connection_string="invalid-connection-string")
