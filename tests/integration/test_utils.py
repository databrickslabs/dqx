import pytest
from databricks.sdk.errors import NotFound

from databricks.labs.dqx.utils import list_tables


def test_list_tables(spark, ws, make_schema, make_random):
    catalog_name = "main"
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

    tables = list_tables(ws, patterns=[f"{catalog_name}.*"])
    assert table1_name in tables
    assert table2_name in tables

    tables = list_tables(ws, patterns=["*"])
    assert table1_name in tables
    assert table2_name in tables

    tables = list_tables(ws, patterns=None)
    assert len(tables) > 0

    tables = list_tables(ws, patterns=[table1_name, table2_name], exclude_matched=True)
    assert not {table1_name, table2_name} & set(tables)


def test_list_tables_with_exclude_patterns(spark, ws, make_schema, make_random):
    catalog_name = "main"
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
