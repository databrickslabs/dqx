from unittest.mock import MagicMock, create_autospec

import pytest
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.checks_storage import TableChecksStorageHandler, is_table_location
from databricks.labs.dqx.config import TableChecksStorageConfig
from databricks.labs.dqx.errors import UnsafeSqlQueryError

_SIMPLE_CHECK = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
_VERSIONING_COLUMNS = ("created_at", "rule_fingerprint", "rule_set_fingerprint")


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
    "run_config_name",
    [
        "default'; DROP TABLE x; --",  # SQL injection via quote
        "a b",  # space
        "name'value",  # single quote
        "a;DELETE FROM t",  # semicolon
        "x\nDROP",  # newline
    ],
)
def test_save_to_table_rejects_unsafe_run_config_name(run_config_name: str):
    """save() with mode='overwrite' raises UnsafeSqlQueryError for run_config_name values outside the allowlist."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    ws.tables.get.side_effect = NotFound("table not found")  # _table_exists → False

    handler = TableChecksStorageHandler(ws, spark)
    config = TableChecksStorageConfig(
        location="catalog.schema.table", run_config_name=run_config_name, mode="overwrite"
    )

    with pytest.raises(UnsafeSqlQueryError, match="run_config_name must not contain unsafe SQL"):
        handler.save(_SIMPLE_CHECK, config)


@pytest.mark.parametrize(
    "run_config_name",
    [
        "default",
        "my-job",
        "table.name",
        "run_v2",
        "A1.B2-C3_D4",
    ],
)
def test_save_to_table_accepts_valid_run_config_name(run_config_name: str):
    """save() with mode='overwrite' does not raise for run_config_name values matching the allowlist."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    ws.tables.get.side_effect = NotFound("table not found")

    handler = TableChecksStorageHandler(ws, spark)
    config = TableChecksStorageConfig(
        location="catalog.schema.table", run_config_name=run_config_name, mode="overwrite"
    )

    # Should not raise — only asserting no UnsafeSqlQueryError
    try:
        handler.save(_SIMPLE_CHECK, config)
    except UnsafeSqlQueryError:
        pytest.fail(f"UnsafeSqlQueryError raised unexpectedly for run_config_name='{run_config_name}'")


def test_save_to_table_skips_run_config_name_check_in_append_mode():
    """save() with mode='append' does not validate run_config_name (replaceWhere is not used)."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    ws.tables.get.side_effect = NotFound("table not found")

    handler = TableChecksStorageHandler(ws, spark)
    config = TableChecksStorageConfig(
        location="catalog.schema.table",
        run_config_name="default'; DROP TABLE x; --",  # would fail in overwrite mode
        mode="append",
    )

    try:
        handler.save(_SIMPLE_CHECK, config)
    except UnsafeSqlQueryError:
        pytest.fail("UnsafeSqlQueryError should not be raised in append mode")


class _MockField:
    """Minimal stand-in for a Spark StructField exposing only .name."""

    def __init__(self, name: str):
        self.name = name


def _make_handler_with_existing_table(
    schema_fields: list,
) -> tuple[TableChecksStorageHandler, TableChecksStorageConfig, MagicMock]:
    """Return a handler/config pair where the table already exists.

    *schema_fields* is set on the mock DataFrame returned by spark.read.table so that
    _ensure_versioning_columns sees the desired existing columns.  The idempotency guard
    is configured to report no matching fingerprint so save() always proceeds past it.
    """
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    spark.read.table.return_value.schema.fields = schema_fields
    # No matching fingerprint in the table → idempotency guard does not short-circuit
    spark.read.table.return_value.filter.return_value.limit.return_value.isEmpty.return_value = True
    handler = TableChecksStorageHandler(ws, spark)
    config = TableChecksStorageConfig(location="catalog.schema.table", run_config_name="default")
    return handler, config, spark


def test_ensure_versioning_columns_quotes_location():
    """ALTER TABLE statements use backtick-quoted identifiers to prevent SQL injection via location."""
    handler, config, spark = _make_handler_with_existing_table(schema_fields=[])

    handler.save(_SIMPLE_CHECK, config)

    expected_quoted = "`catalog`.`schema`.`table`"
    for actual_call in spark.sql.call_args_list:
        sql_str = actual_call.args[0]
        assert expected_quoted in sql_str, f"Expected quoted identifier in SQL: {sql_str!r}"


def test_ensure_versioning_columns_strips_existing_backticks_before_quoting():
    """`location` values that already contain backticks are normalised before quoting."""
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    spark.read.table.return_value.schema.fields = []
    spark.read.table.return_value.filter.return_value.limit.return_value.isEmpty.return_value = True
    handler = TableChecksStorageHandler(ws, spark)
    config = TableChecksStorageConfig(location="`catalog`.`schema`.`table`", run_config_name="default")

    handler.save(_SIMPLE_CHECK, config)

    expected_quoted = "`catalog`.`schema`.`table`"
    for actual_call in spark.sql.call_args_list:
        assert expected_quoted in actual_call.args[0]


def test_ensure_versioning_columns_skips_ddl_when_all_columns_exist():
    """No ALTER TABLE is issued when the table already has all versioning columns."""
    handler, config, spark = _make_handler_with_existing_table(
        schema_fields=[_MockField(col) for col in _VERSIONING_COLUMNS]
    )

    handler.save(_SIMPLE_CHECK, config)

    spark.sql.assert_not_called()


def test_ensure_versioning_columns_adds_only_missing_columns():
    """Only the missing versioning columns are added via ALTER TABLE."""
    # Table already has rule_fingerprint and rule_set_fingerprint but not created_at
    handler, config, spark = _make_handler_with_existing_table(
        schema_fields=[_MockField("rule_fingerprint"), _MockField("rule_set_fingerprint")]
    )

    handler.save(_SIMPLE_CHECK, config)

    assert spark.sql.call_count == 1
    sql_str = spark.sql.call_args.args[0]
    assert "created_at" in sql_str
    assert "rule_fingerprint" not in sql_str
