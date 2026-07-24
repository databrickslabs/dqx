from unittest.mock import MagicMock, create_autospec

import pytest
from pyspark.sql import Row, SparkSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.checks_storage import (
    DataFrameConverter,
    LakebaseChecksStorageHandler,
    TableChecksStorageHandler,
    is_table_location,
)
from databricks.labs.dqx.config import LakebaseChecksStorageConfig, TableChecksStorageConfig
from databricks.labs.dqx.errors import UnsafeSqlQueryError

_SIMPLE_CHECK = [{"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "id"}}}]
_VERSIONING_COLUMNS = ("created_at", "rule_fingerprint", "rule_set_fingerprint")


@pytest.mark.parametrize(
    "location, expected",
    [
        ("catalog.schema.table", True),  # Valid table location
        ("schema.table", True),  # Valid 2-level table location
        ("catalog.`my-schema`.table", True),  # Backticked schema with special chars
        ("catalog.`my-schema`.`my-table`", True),  # Backticked schema and table
        ("`my-schema`.table", True),  # Backticked schema in 2-level location
        ("catalog.schema.`my-table`", True),  # Backticked table with special chars
        ("`my-catalog`.schema.table", True),  # Backticked catalog with special chars
        ("`bad`catalog.schema.table", False),  # Malformed backticks (not spanning the segment)
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
    """No ALTER TABLE is issued when the table already has all versioning and additional columns."""
    handler, config, spark = _make_handler_with_existing_table(
        schema_fields=[_MockField(col) for col in (*_VERSIONING_COLUMNS, "message_expr")]
    )

    handler.save(_SIMPLE_CHECK, config)

    spark.sql.assert_not_called()


def test_ensure_versioning_columns_adds_only_missing_columns():
    """Only the missing versioning and additional columns are added via ALTER TABLE."""
    # Table already has rule_fingerprint and rule_set_fingerprint but not created_at or message_expr
    handler, config, spark = _make_handler_with_existing_table(
        schema_fields=[_MockField("rule_fingerprint"), _MockField("rule_set_fingerprint")]
    )

    handler.save(_SIMPLE_CHECK, config)

    assert spark.sql.call_count == 2
    altered_columns = " ".join(call.args[0] for call in spark.sql.call_args_list)
    assert "created_at" in altered_columns
    assert "message_expr" in altered_columns
    assert "rule_fingerprint " not in altered_columns


def test_save_in_append_mode_with_none_config_fingerprint_writes_to_new_table():
    """config.rule_set_fingerprint=None (the default) does not suppress save() when the table is new.

    save() always computes rule_set_fingerprint from the checks content.  config.rule_set_fingerprint
    is a load-time filter, not a save-time override — setting it to None must never be interpreted
    as "no version known, skip the write".
    """
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    ws.tables.get.side_effect = NotFound("table not found")  # _table_exists → False

    handler = TableChecksStorageHandler(ws, spark)
    config = TableChecksStorageConfig(
        location="catalog.schema.table",
        run_config_name="default",
        mode="append",
        rule_set_fingerprint=None,  # explicit default — must not suppress the write
    )

    handler.save(_SIMPLE_CHECK, config)

    # saveAsTable must be called: the None config fingerprint is irrelevant to save()
    spark.createDataFrame.return_value.write.saveAsTable.assert_called_once()


def test_save_in_append_mode_skips_write_when_computed_fingerprint_already_exists():
    """save() is idempotent: a second call with identical checks is skipped.

    Idempotency is keyed on the fingerprint *computed from the checks content*, not on
    config.rule_set_fingerprint.  When the table already contains a matching fingerprint,
    saveAsTable must NOT be called again regardless of the config fingerprint value.
    """
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    # _table_exists → True (ws.tables.get does not raise)
    spark.read.table.return_value.schema.fields = [_MockField(c) for c in _VERSIONING_COLUMNS]
    # isEmpty() returns False → fingerprint already in table → idempotency guard fires
    spark.read.table.return_value.filter.return_value.isEmpty.return_value = False

    handler = TableChecksStorageHandler(ws, spark)
    config = TableChecksStorageConfig(
        location="catalog.schema.table",
        run_config_name="default",
        mode="append",
        rule_set_fingerprint=None,
    )

    handler.save(_SIMPLE_CHECK, config)

    spark.createDataFrame.return_value.write.saveAsTable.assert_not_called()


def test_save_in_append_mode_proceeds_when_computed_fingerprint_differs_from_existing():
    """save() appends a new version when the computed fingerprint is not yet in the table.

    Demonstrates that config.rule_set_fingerprint=None means "no filter at load time", not
    "skip the write".  A new fingerprint (different checks content) always triggers a write.
    """
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    # _table_exists → True
    spark.read.table.return_value.schema.fields = [_MockField(c) for c in _VERSIONING_COLUMNS]
    # isEmpty() returns True → no matching fingerprint in table → write proceeds
    spark.read.table.return_value.filter.return_value.isEmpty.return_value = True

    handler = TableChecksStorageHandler(ws, spark)
    config = TableChecksStorageConfig(
        location="catalog.schema.table",
        run_config_name="default",
        mode="append",
        rule_set_fingerprint=None,
    )

    handler.save(_SIMPLE_CHECK, config)

    spark.createDataFrame.return_value.write.saveAsTable.assert_called_once()


def test_save_skips_write_when_only_user_metadata_differs():
    """save() is idempotent when only user_metadata changes.

    user_metadata is intentionally excluded from the rule fingerprint so that
    metadata-only updates (e.g. changing an owner tag) do not produce a new version.
    When the table already contains the computed fingerprint, saveAsTable must NOT
    be called regardless of user_metadata differences.
    """
    spark = create_autospec(SparkSession)
    ws = create_autospec(WorkspaceClient)
    spark.read.table.return_value.schema.fields = [_MockField(c) for c in _VERSIONING_COLUMNS]
    # isEmpty() returns False → computed fingerprint already exists in the table
    spark.read.table.return_value.filter.return_value.isEmpty.return_value = False

    handler = TableChecksStorageHandler(ws, spark)
    config = TableChecksStorageConfig(location="catalog.schema.table", run_config_name="default", mode="append")

    # Same logical check as _SIMPLE_CHECK but with user_metadata added
    check_with_metadata = [
        {
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "user_metadata": {"owner": "alice", "team": "data-eng"},
        }
    ]
    handler.save(check_with_metadata, config)

    spark.createDataFrame.return_value.write.saveAsTable.assert_not_called()


def _lakebase_config() -> LakebaseChecksStorageConfig:
    """Build a valid LakebaseChecksStorageConfig without touching any database."""
    return LakebaseChecksStorageConfig(
        location="db.schema.table",
        instance_name="my-instance",
        run_config_name="default",
    )


def _normalize_lakebase_checks(checks: list[dict], config: LakebaseChecksStorageConfig) -> list[dict]:
    """Invoke the pure Lakebase normalization step under test.

    Calls the real normalization logic (types survive, message_expr copied) without a live
    Lakebase connection.
    """
    return LakebaseChecksStorageHandler._normalize_checks(checks, config)


def test_lakebase_table_definition_includes_nullable_message_expr_column():
    """get_table_definition exposes a nullable text message_expr column."""
    table = LakebaseChecksStorageHandler.get_table_definition("schema", "table")

    message_expr_col = table.columns["message_expr"]
    assert message_expr_col.nullable
    # Text renders as a variable-length string type in the Postgres dialect.
    assert "TEXT" in message_expr_col.type.__class__.__name__.upper() or "STRING" in str(message_expr_col.type).upper()


def test_lakebase_normalize_checks_emits_message_expr_when_present():
    """_normalize_checks copies a top-level message_expr onto the normalized row."""
    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "message_expr": "'custom message'",
        }
    ]

    normalized = _normalize_lakebase_checks(checks, _lakebase_config())

    assert normalized[0]["message_expr"] == "'custom message'"


def test_lakebase_normalize_checks_message_expr_defaults_to_none():
    """_normalize_checks sets message_expr to None when the check omits it."""
    normalized = _normalize_lakebase_checks(_SIMPLE_CHECK, _lakebase_config())

    assert normalized[0]["message_expr"] is None


def test_lakebase_normalize_checks_preserves_non_string_user_metadata():
    """Lakebase stores user_metadata as JSONB, so non-string values pass through unchanged (no json.dumps)."""
    checks = [
        {
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": "id"}},
            "user_metadata": {"confidence": 0.95, "enabled": True, "owner": "a@b.com"},
        }
    ]

    normalized = _normalize_lakebase_checks(checks, _lakebase_config())

    assert normalized[0]["user_metadata"] == {"confidence": 0.95, "enabled": True, "owner": "a@b.com"}


def test_lakebase_table_definition_omits_message_expr_when_disabled():
    """get_table_definition(include_message_expr=False) drops the message_expr column (legacy tables)."""
    table = LakebaseChecksStorageHandler.get_table_definition("schema", "table", include_message_expr=False)

    assert "message_expr" not in table.columns
    # Default keeps it, mirroring the versioning column behavior.
    default_table = LakebaseChecksStorageHandler.get_table_definition("schema", "table")
    assert "message_expr" in default_table.columns


def test_row_to_check_dict_omits_message_expr_for_legacy_row():
    """A row without a message_expr field (legacy table) yields a dict without the key."""
    row = Row(
        name="id_not_null",
        criticality="error",
        check=Row(function="is_not_null", for_each_column=[], arguments={}),
        filter=None,
        user_metadata=None,
    )

    check_dict = DataFrameConverter._row_to_check_dict(row)

    assert "message_expr" not in check_dict


def test_row_to_check_dict_reads_message_expr_when_present():
    """A row carrying message_expr surfaces it on the reconstructed check dict."""
    row = Row(
        name="id_not_null",
        criticality="error",
        check=Row(function="is_not_null", for_each_column=[], arguments={}),
        filter=None,
        user_metadata=None,
        message_expr="'m'",
    )

    check_dict = DataFrameConverter._row_to_check_dict(row)

    assert check_dict["message_expr"] == "'m'"


def test_row_to_check_dict_handles_null_user_metadata_value():
    """A legacy row whose user_metadata map holds a SQL NULL value decodes to None, not a crash."""
    row = Row(
        name="id_not_null",
        criticality="error",
        check=Row(function="is_not_null", for_each_column=[], arguments={"column": '"id"'}),
        filter=None,
        user_metadata={"missing": None, "owner": '"a@b.com"'},
    )

    check_dict = DataFrameConverter._row_to_check_dict(row)

    assert check_dict["user_metadata"] == {"missing": None, "owner": "a@b.com"}
