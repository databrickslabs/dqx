from unittest import skip
import pytest
from databricks.sdk.errors import NotFound
from databricks.labs.dqx.utils import list_tables, get_table_primary_keys

from tests.constants import TEST_CATALOG


def test_list_tables(spark, ws, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    input_schema = "col1: int"
    input_df = spark.createDataFrame([[1]], input_schema)
    input_df.write.format("delta").saveAsTable(table1_name)
    input_df.write.format("delta").saveAsTable(table2_name)

    tables = list_tables(ws, patterns=[table1_name, table2_name])
    assert set(tables) == {table1_name, table2_name}

    tables = list_tables(ws, patterns=[table1_name])
    assert set(tables) == {table1_name}

    tables = list_tables(ws, patterns=[f"{catalog_name}.{schema_name}.*"])
    assert set(tables) == {table1_name, table2_name}

    tables = list_tables(ws, patterns=[f"{catalog_name}.{schema_name}.*"])
    assert set(tables) == {table1_name, table2_name}


@skip("Ad-hoc test only: Running multiple tests in parallel can cause a failure")
def test_list_tables_extended(spark, ws, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    input_schema = "col1: int"
    input_df = spark.createDataFrame([[1]], input_schema)
    input_df.write.format("delta").saveAsTable(table_name)

    tables = list_tables(ws, patterns=["*"])
    assert len(tables) > 0

    tables = list_tables(ws, patterns=None)
    assert len(tables) > 0

    tables = list_tables(ws, patterns=[table_name], exclude_matched=True)
    assert not {table_name} & set(tables)


def test_list_tables_with_exclude_patterns(spark, ws, make_schema, make_random):
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    output_table_name = f"{table_name}_dq_output"
    quarantine_table_name = f"{table_name}_dq_quarantine"

    input_schema = "col1: int"
    input_df = spark.createDataFrame([[1]], input_schema)
    input_df.write.format("delta").saveAsTable(table_name)
    input_df.write.format("delta").saveAsTable(output_table_name)
    input_df.write.format("delta").saveAsTable(quarantine_table_name)

    tables = list_tables(ws, patterns=[f"{catalog_name}.{schema_name}.*"])
    assert set(tables) == {table_name, output_table_name, quarantine_table_name}

    tables = list_tables(
        ws,
        patterns=[f"{catalog_name}.{schema_name}.*"],
        exclude_patterns=[f"{catalog_name}.{schema_name}.*_dq_output", f"{catalog_name}.{schema_name}.*_dq_quarantine"],
    )
    assert set(tables) == {table_name}

    tables = list_tables(
        ws,
        patterns=[f"{catalog_name}.{schema_name}.*"],
        exclude_patterns=["*_dq_output", "*_dq_quarantine"],
    )
    assert set(tables) == {table_name}

    tables = list_tables(
        ws,
        patterns=[f"{catalog_name}.{schema_name}.*"],
        exclude_patterns=[output_table_name, quarantine_table_name],
    )
    assert set(tables) == {table_name}

    tables = list_tables(
        ws,
        patterns=[f"{catalog_name}.{schema_name}.*"],
        exclude_patterns=[f"{catalog_name}.{schema_name}.*_dq_output", f"{catalog_name}.{schema_name}.*_dq_quarantine"],
    )
    assert set(tables) == {table_name}

    with pytest.raises(NotFound, match="No tables found matching include or exclude criteria"):
        list_tables(
            ws,
            patterns=[f"{catalog_name}.{schema_name}.*"],
            exclude_patterns=[f"{catalog_name}.{schema_name}.*"],
        )


def test_list_tables_no_matching(spark, ws, make_random):
    with pytest.raises(NotFound, match="No tables found matching include or exclude criteria"):
        list_tables(ws, patterns=[f"non_existent_catalog_{make_random(10).lower()}.*"])


def test_get_table_primary_keys_no_constraint(spark, make_schema, make_random):
    """Test get_table_primary_keys on a table without primary key constraint."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    # Create table without primary key
    spark.sql(
        f"""
        CREATE TABLE {table_name} (
            id BIGINT,
            name STRING,
            value DOUBLE
        ) USING DELTA
    """
    )

    # Should return empty set
    pk_cols = get_table_primary_keys(table_name, spark)
    assert pk_cols == set()


def test_get_table_primary_keys_single_column(spark, make_schema, make_random):
    """Test get_table_primary_keys on a table with single column primary key."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    # Create table with primary key
    spark.sql(
        f"""
        CREATE TABLE {table_name} (
            id BIGINT NOT NULL,
            name STRING,
            value DOUBLE
        ) USING DELTA
    """
    )

    # Add primary key via table properties
    # The function looks for any property key containing 'primary'
    spark.sql(
        f"""
        ALTER TABLE {table_name} 
        SET TBLPROPERTIES ('custom.primary_key' = 'id')
    """
    )

    # Should return the primary key column
    pk_cols = get_table_primary_keys(table_name, spark)
    assert pk_cols == {"id"}


def test_get_table_primary_keys_composite(spark, make_schema, make_random):
    """Test get_table_primary_keys on a table with composite primary key."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    table_name = f"{catalog_name}.{schema_name}.{make_random(10).lower()}"

    # Create table
    spark.sql(
        f"""
        CREATE TABLE {table_name} (
            region STRING NOT NULL,
            product_id BIGINT NOT NULL,
            sales DOUBLE
        ) USING DELTA
    """
    )

    # Add composite primary key via table properties
    # The function looks for any property key containing 'primary'
    spark.sql(
        f"""
        ALTER TABLE {table_name} 
        SET TBLPROPERTIES ('custom.primary_key' = 'region, product_id')
    """
    )

    # Should return both columns
    pk_cols = get_table_primary_keys(table_name, spark)
    assert pk_cols == {"region", "product_id"}


def test_get_table_primary_keys_nonexistent_table(spark, make_schema, make_random):
    """Test get_table_primary_keys gracefully handles non-existent table."""
    catalog_name = TEST_CATALOG
    schema_name = make_schema(catalog_name=catalog_name).name
    fake_table_name = f"{catalog_name}.{schema_name}.nonexistent_{make_random(10).lower()}"

    # Should return empty set without raising exception
    pk_cols = get_table_primary_keys(fake_table_name, spark)
    assert pk_cols == set()


# Note: get_table_foreign_keys() tests are NOT included here because it's a simple
# wrapper that delegates to SparkTableDataProvider.get_table_foreign_keys(),
# which is already thoroughly tested in test_table_manager.py.
# The wrapper adds only a try/except for error handling (returns empty dict on error).
