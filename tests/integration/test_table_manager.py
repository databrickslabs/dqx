import os
import datetime as dt
import pytest

from databricks.labs.dqx.table_manager import SparkTableDataProvider, TableManager

TEST_CATALOG = "dqx"

if not os.getenv("DATABRICKS_HOST"):
    pytest.skip(
        "DATABRICKS_HOST not set; skipping integration tests requiring Databricks workspace.", allow_module_level=True
    )


def _create_simple_table(spark, make_schema, make_random) -> tuple[str, str]:
    schema = make_schema(catalog_name=TEST_CATALOG)
    table_name = f"{TEST_CATALOG}.{schema.name}.table_manager_{make_random(8).lower()}"
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, name string")
    df.write.format("delta").saveAsTable(table_name)
    return schema.name, table_name


def _create_typed_table(spark, make_schema, make_random) -> str:
    schema = make_schema(catalog_name=TEST_CATALOG)
    table_name = f"{TEST_CATALOG}.{schema.name}.table_manager_types_{make_random(8).lower()}"
    df = spark.createDataFrame(
        [(1, "a", dt.date(2024, 1, 1), dt.datetime(2024, 1, 1, 0, 0, 0))],
        "id int, name string, event_date date, event_ts timestamp",
    )
    df.write.format("delta").saveAsTable(table_name)
    return table_name


def test_table_manager_definition_and_columns(spark, make_schema, make_random):
    _, table_name = _create_simple_table(spark, make_schema, make_random)

    manager = TableManager(spark=spark)
    definition = manager.get_table_definition(table_name)

    assert "id int" in definition
    assert "name string" in definition
    assert "Existing Primary Key" not in definition

    columns = manager.get_table_column_names(table_name)
    assert columns == ["id", "name"]


def test_table_manager_metadata_and_run_sql(spark, make_schema, make_random):
    _, table_name = _create_simple_table(spark, make_schema, make_random)

    manager = TableManager(spark=spark)
    metadata = manager.get_table_metadata_info(table_name)
    assert isinstance(metadata, str)
    assert metadata.startswith("Metadata information:") or metadata == "Limited metadata available"

    result_df = manager.run_sql(f"SELECT count(*) AS cnt FROM {table_name}")
    row = result_df.first()
    assert row is not None
    assert row["cnt"] == 2


def test_table_manager_metadata_includes_type_distribution(spark, make_schema, make_random):
    table_name = _create_typed_table(spark, make_schema, make_random)

    manager = TableManager(spark=spark)
    metadata = manager.get_table_metadata_info(table_name)

    assert "Column type distribution:" in metadata
    assert "Numeric columns (1)" in metadata
    assert "String columns (1)" in metadata
    assert "Date columns (1)" in metadata
    assert "Timestamp columns (1)" in metadata


def test_spark_table_data_provider_foreign_keys_empty(spark, make_schema, make_random):
    _, table_name = _create_simple_table(spark, make_schema, make_random)

    provider = SparkTableDataProvider(spark)
    foreign_keys = provider.get_table_foreign_keys(table_name)
    assert not foreign_keys


def test_spark_table_data_provider_foreign_keys_parsing(spark, make_schema, make_random):
    _, table_name = _create_simple_table(spark, make_schema, make_random)

    spark.sql(
        f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
          'dqx.foreign_key_valid'='FOREIGN KEY (id) REFERENCES ref_table(ref_id)',
          'dqx.foreign_key_no_cols'='FOREIGN KEY (id) REFERENCES ref_table',
          'dqx.foreign_key_invalid'='FOREIGN KEY id REFERENCES ref_table',
          'dqx.foreign_key_missing_ref'='FOREIGN KEY (id)'
        )
        """
    )

    provider = SparkTableDataProvider(spark)
    foreign_keys = provider.get_table_foreign_keys(table_name)

    assert "foreign_key_valid" in foreign_keys
    assert foreign_keys["foreign_key_valid"]["columns"] == ["ID"]
    assert foreign_keys["foreign_key_valid"]["referenced_table"] == "REF_TABLE"
    assert foreign_keys["foreign_key_valid"]["referenced_columns"] == ["REF_ID"]

    assert "foreign_key_no_cols" in foreign_keys
    assert foreign_keys["foreign_key_no_cols"]["referenced_columns"] == []

    assert "foreign_key_invalid" not in foreign_keys
    assert "foreign_key_missing_ref" not in foreign_keys
