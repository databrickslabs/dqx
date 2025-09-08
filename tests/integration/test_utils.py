import pytest
from databricks.labs.dqx.utils import list_tables


def test_list_tables(spark, ws, make_schema, make_random):
    catalog_name = "main"
    schema_name = make_schema(catalog_name=catalog_name).name
    table1_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"
    table2_name = f"{catalog_name}.{schema_name}.{make_random(6).lower()}"

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

    tables = list_tables(ws, patterns=None)
    assert len(tables) > 0

    tables = list_tables(ws, patterns=[table1_name, table2_name], exclude_matched=True)
    assert not {table1_name, table2_name} & set(tables)


def test_list_tables_no_matching(spark, ws, make_random):
    with pytest.raises(ValueError, match="No tables found matching include or exclude criteria"):
        list_tables(ws, patterns=[f"non_existent_catalog_{make_random(6).lower()}.*"])
