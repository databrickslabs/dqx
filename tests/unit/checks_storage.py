import pytest
from databricks.labs.dqx.checks_storage import is_table_location, get_default_checks_location


@pytest.mark.parametrize(
    "location, expected",
    [
        ("catalog.schema.table", True),  # Valid table location
        ("catalog/schema/table.yml", False),  # File path with known extension
        ("catalog.schema.table.json", False),  # File path with known extension
        ("", False),  # Empty string
        ("invalid_location", False),  # Malformed input
    ],
)
def test_is_table_location(location: str, expected: bool):
    assert is_table_location(location) == expected


@pytest.mark.parametrize(
    "table, install_folder, expected",
    [
        ("catalog.schema.table", "/path/to/install", "catalog.schema.table"),
        ("schema.table", "/install/folder", "schema.table"),
        ("table", "/install", "/install/checks/"),
        ("path/file.yml", "/install", "/install/checks/"),
        ("/path/file.yml", "/install", "/install/checks/"),
        ("", "/install", "/install/checks/"),  # Edge case: empty table name
    ],
)
def test_get_default_checks_location(table: str, install_folder: str, expected: str):
    assert get_default_checks_location(install_folder, table) == expected
