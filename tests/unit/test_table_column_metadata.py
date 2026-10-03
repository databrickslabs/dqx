import json
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import ColumnInfo, TableInfo
from pyspark.sql import SparkSession

from databricks.labs.dqx.config import (
    UC_TABLE_PATTERN,
    UnityCatalogMetadataConfig,
)
from databricks.labs.dqx.utils import get_table_column_metadata


def test_get_table_column_metadata_returns_name_and_type(mock_workspace_client):
    mock_workspace_client.tables.get.return_value = TableInfo(
        columns=[
            ColumnInfo(name="user_id", type_text="string"),
            ColumnInfo(name="age", type_text="int"),
            ColumnInfo(name="balance", type_text="decimal(10,2)"),
        ]
    )

    result = get_table_column_metadata(mock_workspace_client, "main.default.users")

    assert json.loads(result) == {
        "columns": [
            {"name": "user_id", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "balance", "type": "decimal(10,2)"},
        ]
    }
    mock_workspace_client.tables.get.assert_called_once_with("main.default.users")


@pytest.mark.parametrize("columns", [None, []])
def test_get_table_column_metadata_handles_table_without_columns(mock_workspace_client, columns):
    mock_workspace_client.tables.get.return_value = TableInfo(columns=columns)

    result = get_table_column_metadata(mock_workspace_client, "main.default.empty")

    assert json.loads(result) == {"columns": []}


def test_get_table_column_metadata_normalizes_type_case(mock_workspace_client):
    """Unity Catalog returns DDL type text, which must match Spark's lowercase simpleString form."""
    mock_workspace_client.tables.get.return_value = TableInfo(
        columns=[
            ColumnInfo(name="tags", type_text="ARRAY<INT>"),
            ColumnInfo(name="address", type_text="STRUCT<city: STRING>"),
            ColumnInfo(name="amount", type_text="DECIMAL(10,2)"),
        ]
    )

    result = get_table_column_metadata(mock_workspace_client, "main.default.users")

    assert json.loads(result) == {
        "columns": [
            {"name": "tags", "type": "array<int>"},
            {"name": "address", "type": "struct<city: string>"},
            {"name": "amount", "type": "decimal(10,2)"},
        ]
    }


def test_get_table_column_metadata_tolerates_missing_field_values(mock_workspace_client):
    """Unity Catalog may omit a name or type; those become empty strings rather than nulls."""
    mock_workspace_client.tables.get.return_value = TableInfo(columns=[ColumnInfo(name="user_id")])

    result = get_table_column_metadata(mock_workspace_client, "main.default.users")

    assert json.loads(result) == {"columns": [{"name": "user_id", "type": ""}]}


@pytest.mark.parametrize(
    "location",
    [
        "main.default.users",
        "catalog1.schema1.customers",
        "cat_1.sch_2.tbl_3",
        "`my-catalog`.schema.table",  # backtick-quoted names are valid UC tables
        "main.`my-schema`.tbl",
        "`my-catalog`.`my-schema`.`my-table`",
    ],
)
def test_uc_table_pattern_matches_three_level_names(location):
    assert UC_TABLE_PATTERN.match(location)


@pytest.mark.parametrize(
    "location",
    [
        "default.users",  # two-level, resolved against the session catalog
        "users",  # bare name or temporary view
        "temp_from_dataframe_1_abcdef",
        "/Volumes/main/default/data",
        "s3://bucket/path",
        "main.default.users.extra",
    ],
)
def test_uc_table_pattern_rejects_non_uc_locations(location):
    assert not UC_TABLE_PATTERN.match(location)


def test_get_table_column_metadata_baseline_unchanged_when_config_none(mock_workspace_client):
    """Byte-identical back-compat: a None config emits exactly the pre-enrichment payload."""
    mock_workspace_client.tables.get.return_value = TableInfo(
        columns=[ColumnInfo(name="id", type_text="string")],
    )
    result = get_table_column_metadata(mock_workspace_client, "main.default.t")
    assert result == json.dumps({"columns": [{"name": "id", "type": "string"}]})


def test_get_table_column_metadata_enriches_with_config(mock_workspace_client):
    """Passing a config switches to the enriched renderer and attaches table/column comments."""
    mock_workspace_client.tables.get.return_value = TableInfo(
        comment="Fact table",
        columns=[ColumnInfo(name="id", type_text="string", comment="Primary key")],
    )
    mock_workspace_client.external_lineage.list_external_lineage_relationships.return_value = iter([])
    spark = MagicMock(spec=SparkSession)
    spark.sql.return_value.collect.return_value = []

    config = UnityCatalogMetadataConfig(column_upstream_lineage=None, external_lineage=None)
    payload = json.loads(
        get_table_column_metadata(
            mock_workspace_client,
            "main.default.t",
            unity_catalog_metadata_config=config,
            spark=spark,
        )
    )
    assert payload["table_comment"] == "Fact table"
    assert payload["columns"][0]["comment"] == "Primary key"
