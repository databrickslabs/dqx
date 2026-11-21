"""Unit tests for *collect_unity_catalog_metadata*.

Covers the single public entry point for assembling Unity Catalog context before an
AI-assisted rule generation call: location-based dispatch (UC workspace client vs. Spark),
baseline schema reads without enrichment, enriched reads with *UnityCatalogMetadataConfig*,
and the non-UC-location + enrichment-config warning path.
"""

import logging

import pytest
from databricks.sdk.service.catalog import ColumnInfo, TableInfo

from databricks.labs.dqx.config import InputConfig, UnityCatalogMetadataConfig
from databricks.labs.dqx.errors import InvalidConfigError, MissingParameterError
from databricks.labs.dqx.profiler.unity_catalog_metadata import UnityCatalogMetadata, collect_unity_catalog_metadata


UC_COLUMNS = [{"name": "user_id", "type": "string"}, {"name": "age", "type": "int"}]


@pytest.fixture
def uc_workspace_client(mock_workspace_client):
    """Workspace client returning a two-column Unity Catalog table."""
    mock_workspace_client.tables.get.return_value = TableInfo(
        columns=[
            ColumnInfo(name="user_id", type_text="string"),
            ColumnInfo(name="age", type_text="int"),
        ]
    )
    return mock_workspace_client


def test_empty_location_raises(mock_workspace_client):
    with pytest.raises(InvalidConfigError, match="Input location not configured"):
        collect_unity_catalog_metadata(InputConfig(location=""), ws=mock_workspace_client, spark=None)


@pytest.mark.parametrize("location", ["not a location", "users", "main..users"])
def test_unclassifiable_location_raises(mock_workspace_client, location):
    with pytest.raises(InvalidConfigError, match="Invalid input location"):
        collect_unity_catalog_metadata(InputConfig(location=location), ws=mock_workspace_client, spark=None)
    assert not mock_workspace_client.tables.get.called


def test_uc_table_no_config_returns_baseline_schema(uc_workspace_client):
    result = collect_unity_catalog_metadata(
        InputConfig(location="main.default.users"), ws=uc_workspace_client, spark=None
    )
    assert isinstance(result, UnityCatalogMetadata)
    # Baseline path has no `table` key and no enrichment fields.
    assert result.columns == UC_COLUMNS
    assert result.table is None
    assert result.table_comment is None
    uc_workspace_client.tables.get.assert_called_once_with("main.default.users")


def test_backticked_uc_table_still_routes_through_workspace_client(uc_workspace_client):
    result = collect_unity_catalog_metadata(
        InputConfig(location="`my-catalog`.`my-schema`.users"), ws=uc_workspace_client, spark=None
    )
    assert result.columns == UC_COLUMNS
    uc_workspace_client.tables.get.assert_called_once_with("my-catalog.my-schema.users")


def test_three_level_hive_metastore_uses_workspace_client(uc_workspace_client, mock_spark):
    """A 3-level hive_metastore name is a valid UC namespace, so it is read via workspace client, not Spark."""
    collect_unity_catalog_metadata(
        InputConfig(location="hive_metastore.default.users"), ws=uc_workspace_client, spark=mock_spark
    )
    uc_workspace_client.tables.get.assert_called_once_with("hive_metastore.default.users")
    assert not mock_spark.read.options.called


def test_two_level_name_falls_back_to_spark(mock_workspace_client, mock_spark):
    mock_spark.read.options.return_value.table.return_value.schema.fields = []
    result = collect_unity_catalog_metadata(
        InputConfig(location="default.users"), ws=mock_workspace_client, spark=mock_spark
    )
    assert not result.columns
    assert result.table is None
    assert not mock_workspace_client.tables.get.called


def test_spark_backed_schema_requires_a_session(mock_workspace_client):
    with pytest.raises(MissingParameterError, match="required to read the input schema"):
        collect_unity_catalog_metadata(InputConfig(location="default.users"), ws=mock_workspace_client, spark=None)


def test_uc_enrichment_requires_a_session(uc_workspace_client):
    with pytest.raises(MissingParameterError, match="required for Unity Catalog metadata enrichment"):
        collect_unity_catalog_metadata(
            InputConfig(location="main.default.users"),
            ws=uc_workspace_client,
            spark=None,
            config=UnityCatalogMetadataConfig(),
        )


def test_storage_path_falls_back_to_spark(mock_workspace_client, mock_spark):
    mock_spark.read.options.return_value.load.return_value.schema.fields = []
    result = collect_unity_catalog_metadata(
        InputConfig(location="s3://bucket/data", format="parquet"),
        ws=mock_workspace_client,
        spark=mock_spark,
    )
    assert not result.columns
    assert result.table is None
    assert not mock_workspace_client.tables.get.called


def test_uc_table_with_full_config_returns_enriched_schema(uc_workspace_client, mock_spark):
    """With a UC 3-level name and a full UnityCatalogMetadataConfig, every enrichment field
    the config enables is attached to the returned record."""
    mock_spark.sql.return_value.collect.return_value = []  # lineage CTEs empty
    mock_spark.table.return_value.filter.return_value.select.return_value.collect.return_value = []  # tag reads empty
    uc_workspace_client.external_lineage.list_external_lineage_relationships.return_value = iter([])

    result = collect_unity_catalog_metadata(
        InputConfig(location="main.default.users"),
        ws=uc_workspace_client,
        spark=mock_spark,
        config=UnityCatalogMetadataConfig(),
    )
    # Baseline columns are always present; comments / tags / lineage are empty here (no data),
    # so the enriched record is shape-compatible with the baseline.
    assert result.table == "main.default.users"
    assert result.columns == UC_COLUMNS


def test_non_uc_location_with_config_skips_enrichment_with_warning(mock_workspace_client, mock_spark, caplog):
    """Non-UC locations (2-level names, storage paths) cannot be enriched — the warning fires
    and the collector falls through to the baseline Spark read."""
    mock_spark.read.options.return_value.load.return_value.schema.fields = []
    mock_spark.read.options.return_value.table.return_value.schema.fields = []

    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.profiler.unity_catalog_metadata"):
        result = collect_unity_catalog_metadata(
            InputConfig(location="s3://bucket/data", format="parquet"),
            ws=mock_workspace_client,
            spark=mock_spark,
            config=UnityCatalogMetadataConfig(),
        )

    assert not result.columns
    assert result.table is None
    mock_workspace_client.tables.get.assert_not_called()
    assert not mock_spark.sql.called
    assert "Skipping Unity Catalog metadata enrichment" in caplog.text
