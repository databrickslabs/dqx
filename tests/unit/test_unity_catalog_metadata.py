"""Unit tests for the Unity Catalog metadata enrichment module.

Covers Pydantic config validators, collectors (table metadata, tags, column + table upstream
lineage, external lineage), and the top-level *build_schema_json* renderer. No real Spark or
workspace is involved — all externals are mocked with *create_autospec*.
"""

import json
from unittest.mock import create_autospec

import pytest
from pydantic import ValidationError
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.catalog import Catalog

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ColumnInfo,
    ColumnRelationship,
    ExternalLineageExternalMetadataInfo,
    ExternalLineageFileInfo,
    ExternalLineageInfo,
    ExternalLineageObject,
    ExternalLineageRelationshipInfo,
    ExternalLineageTableInfo,
    LineageDirection,
    SystemType,
    TableInfo,
)

from databricks.labs.dqx.config import (
    ColumnUpstreamLineageConfig,
    ExternalLineageConfig,
    UnityCatalogMetadataConfig,
)
from databricks.labs.dqx.profiler.unity_catalog_metadata import (
    build_schema_json,
    collect_column_upstream_lineage,
    collect_external_upstream_lineage,
    collect_table_comments,
    collect_table_tags,
    collect_upstream_table_lineage,
)


# -- Pydantic config validation ----------------------------------------------------------


def test_unity_catalog_metadata_config_defaults_enable_everything():
    config = UnityCatalogMetadataConfig()
    assert config.include_table_comment is True
    assert config.include_column_comments is True
    assert config.include_tags is True
    assert isinstance(config.column_upstream_lineage, ColumnUpstreamLineageConfig)
    assert isinstance(config.external_lineage, ExternalLineageConfig)


def test_unity_catalog_metadata_config_sub_models_optional():
    config = UnityCatalogMetadataConfig(column_upstream_lineage=None, external_lineage=None)
    assert config.column_upstream_lineage is None
    assert config.external_lineage is None


@pytest.mark.parametrize(
    "field, value",
    [
        ("depth", 0),
        ("lookback_days", 0),
        ("max_nodes", 0),
    ],
)
def test_column_upstream_lineage_config_rejects_zero(field, value):
    with pytest.raises(ValidationError):
        ColumnUpstreamLineageConfig(**{field: value})


def test_column_upstream_lineage_config_depth_none_allowed():
    assert ColumnUpstreamLineageConfig(depth=None).depth is None


def test_external_lineage_config_rejects_zero():
    with pytest.raises(ValidationError):
        ExternalLineageConfig(max_relationships=0)


def test_unity_catalog_metadata_config_forbids_unknown_fields():
    with pytest.raises(ValidationError):
        UnityCatalogMetadataConfig(unexpected=True)  # type: ignore[call-arg]


# -- test helpers ------------------------------------------------------------------------


def _dataframe_with_rows(rows: list[Row]) -> DataFrame:
    """Return an autospec'd DataFrame whose chainable ops (filter/select) return self and
    whose ``.collect()`` returns ``rows``. Supports both the DataFrame-API tag reads and the
    raw-SQL lineage reads."""
    df = create_autospec(DataFrame, instance=True)
    df.filter.return_value = df
    df.select.return_value = df
    df.collect.return_value = list(rows)
    return df


def _spark_mock(
    *,
    sql_results: list[list[Row]] | None = None,
    table_results: list[list[Row]] | None = None,
) -> SparkSession:
    """Return an autospec'd SparkSession that serves ``spark.sql(...)`` and
    ``spark.table(...)`` from the two queues (consumed in call order)."""
    spark = create_autospec(SparkSession, instance=True)
    spark.sql.side_effect = [_dataframe_with_rows(r) for r in sql_results or []]
    spark.table.side_effect = [_dataframe_with_rows(r) for r in table_results or []]
    spark.createDataFrame.return_value = create_autospec(DataFrame, instance=True)
    # create_autospec(SparkSession) does not deep-spec the lazy ``catalog`` attribute, so
    # ``spark.catalog.dropTempView`` would otherwise raise AttributeError.
    spark.catalog = create_autospec(Catalog, instance=True)
    return spark


# -- collect_table_comments --------------------------------------------------------------


def test_collect_table_comments_returns_table_and_column_values(mock_workspace_client):
    mock_workspace_client.tables.get.return_value = TableInfo(
        comment="  Source table  ",
        columns=[
            ColumnInfo(name="id", type_text="string", comment="Primary key"),
            ColumnInfo(name="val", type_text="int", comment=None),
            ColumnInfo(name="misc", type_text="string", comment="   "),
        ],
    )
    table_comment, column_comments = collect_table_comments(mock_workspace_client, "cat.sch.tab")
    assert table_comment == "Source table"
    assert column_comments == {"id": "Primary key"}
    # One SDK call satisfies both table + column needs.
    mock_workspace_client.tables.get.assert_called_once_with("cat.sch.tab")


def test_collect_table_comments_returns_none_when_table_comment_missing(mock_workspace_client):
    mock_workspace_client.tables.get.return_value = TableInfo(comment=None, columns=[])
    assert collect_table_comments(mock_workspace_client, "cat.sch.tab") == (None, {})


def test_collect_table_comments_tolerates_sdk_error(mock_workspace_client):
    mock_workspace_client.tables.get.side_effect = RuntimeError("boom")
    assert collect_table_comments(mock_workspace_client, "cat.sch.tab") == (None, {})


# -- collect_table_tags ------------------------------------------------------------------


def test_collect_table_tags_merges_rows():
    spark = _spark_mock(
        table_results=[
            [
                Row(tag_name="pii", tag_value="yes"),
                Row(tag_name="domain", tag_value="finance"),
            ],
            [
                Row(column_name="id", tag_name="pk", tag_value="true"),
                Row(column_name="id", tag_name="pii", tag_value="client"),
                Row(column_name="amount", tag_name="financial", tag_value="true"),
            ],
        ],
    )
    result = collect_table_tags(spark, "cat.sch.tab")
    assert result["table_tags"] == [
        {"key": "pii", "value": "yes"},
        {"key": "domain", "value": "finance"},
    ]
    assert result["column_tags"]["id"] == [
        {"key": "pk", "value": "true"},
        {"key": "pii", "value": "client"},
    ]
    assert result["column_tags"]["amount"] == [{"key": "financial", "value": "true"}]
    assert [call.args[0] for call in spark.table.call_args_list] == [
        "system.information_schema.table_tags",
        "system.information_schema.column_tags",
    ]


def test_collect_table_tags_degrades_on_table_read_error():
    spark = create_autospec(SparkSession, instance=True)
    spark.table.side_effect = RuntimeError("denied")
    assert collect_table_tags(spark, "cat.sch.tab") == {"table_tags": [], "column_tags": {}}


def test_collect_table_tags_rejects_bad_name():
    spark = create_autospec(SparkSession, instance=True)
    # Bad name — collector logs a warning and returns the empty structure without hitting Spark.
    assert collect_table_tags(spark, "two.part") == {"table_tags": [], "column_tags": {}}
    spark.table.assert_not_called()


# -- collect_column_upstream_lineage -----------------------------------------------------


def test_collect_column_upstream_lineage_sql_has_cycle_guard_and_limits():
    spark = _spark_mock(sql_results=[[]])

    collect_column_upstream_lineage(
        spark,
        "cat.sch.tab",
        ["id", "name"],
        config=ColumnUpstreamLineageConfig(depth=3, lookback_days=10, max_nodes=25),
    )
    assert spark.sql.call_count == 1
    sql_text = spark.sql.call_args.args[0]
    assert "WITH RECURSIVE edges" in sql_text
    assert "NOT array_contains(e.path" in sql_text
    assert "LIMIT 25" in sql_text  # per-member and tail LIMITs
    assert "INTERVAL 10 DAYS" in sql_text
    assert "e.depth < 3" in sql_text
    # Anchor table literal is escaped as a SQL string literal with single quotes doubled.
    assert "'cat.sch.tab'" in sql_text
    # Seed columns are inlined as escaped literals (sorted, de-duplicated).
    assert "target_column_name IN ('id', 'name')" in sql_text
    # No temp view registered on the session.
    spark.createDataFrame.assert_not_called()


def test_collect_column_upstream_lineage_escapes_seed_column_quotes():
    """Column names containing single quotes must be escaped, not inlined raw."""
    spark = _spark_mock(sql_results=[[]])
    collect_column_upstream_lineage(
        spark,
        "cat.sch.tab",
        ["weird'name"],
        config=ColumnUpstreamLineageConfig(),
    )
    sql_text = spark.sql.call_args.args[0]
    assert "'weird''name'" in sql_text


def test_collect_column_upstream_lineage_unbounded_depth_omits_predicate():
    spark = _spark_mock(sql_results=[[]])
    collect_column_upstream_lineage(
        spark,
        "cat.sch.tab",
        ["id"],
        config=ColumnUpstreamLineageConfig(depth=None),
    )
    sql_text = spark.sql.call_args.args[0]
    assert "e.depth <" not in sql_text


def test_collect_column_upstream_lineage_tolerates_sql_failure():
    spark = create_autospec(SparkSession, instance=True)
    spark.sql.side_effect = RuntimeError("permission denied")

    result = collect_column_upstream_lineage(
        spark,
        "cat.sch.tab",
        ["id"],
        config=ColumnUpstreamLineageConfig(),
    )
    assert not result


def test_collect_column_upstream_lineage_no_seed_columns_short_circuits():
    spark = create_autospec(SparkSession, instance=True)
    result = collect_column_upstream_lineage(
        spark,
        "cat.sch.tab",
        [],
        config=ColumnUpstreamLineageConfig(),
    )
    assert not result
    spark.sql.assert_not_called()


def test_collect_upstream_table_lineage_builds_recursive_cte():
    spark = _spark_mock(sql_results=[[]])
    collect_upstream_table_lineage(
        spark,
        "cat.sch.tab",
        config=ColumnUpstreamLineageConfig(depth=2, lookback_days=7, max_nodes=10),
    )
    sql_text = spark.sql.call_args.args[0]
    assert "WITH RECURSIVE edges" in sql_text
    assert "LIMIT 10" in sql_text
    assert "INTERVAL 7 DAYS" in sql_text
    assert "e.depth < 2" in sql_text


# -- collect_external_upstream_lineage ---------------------------------------------------


def _external_metadata_info(system_type: SystemType = SystemType.SAP) -> ExternalLineageInfo:
    return ExternalLineageInfo(
        external_metadata_info=ExternalLineageExternalMetadataInfo(
            name="sap_sd_invoices",
            system_type=system_type,
            entity_type="TABLE",
        ),
        external_lineage_info=ExternalLineageRelationshipInfo(
            source=ExternalLineageObject(),
            target=ExternalLineageObject(),
            columns=[
                ColumnRelationship(source="VBELN", target="invoice_id"),
                ColumnRelationship(source="WAERK", target="currency_code"),
            ],
            properties={"module": "SD"},
        ),
    )


def _external_table_info() -> ExternalLineageInfo:
    return ExternalLineageInfo(
        table_info=ExternalLineageTableInfo(catalog_name="src", schema_name="raw", name="orders"),
        external_lineage_info=ExternalLineageRelationshipInfo(
            source=ExternalLineageObject(), target=ExternalLineageObject()
        ),
    )


def _external_file_info() -> ExternalLineageInfo:
    return ExternalLineageInfo(
        file_info=ExternalLineageFileInfo(
            path="s3://bucket/path", securable_name="volumes.ext", securable_type="VOLUME"
        ),
        external_lineage_info=ExternalLineageRelationshipInfo(
            source=ExternalLineageObject(), target=ExternalLineageObject()
        ),
    )


def test_collect_external_upstream_lineage_discriminates_variants():
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.external_lineage.list_external_lineage_relationships.return_value = iter(
        [_external_metadata_info(), _external_table_info(), _external_file_info()]
    )
    result = collect_external_upstream_lineage(ws, "cat.sch.tab", config=ExternalLineageConfig())
    assert [r["source_kind"] for r in result] == ["external_metadata", "table", "file"]
    sap = result[0]
    assert sap["system_type"] == SystemType.SAP.value
    assert sap["entity_type"] == "TABLE"
    assert sap["columns"] == [
        {"source": "VBELN", "target": "invoice_id"},
        {"source": "WAERK", "target": "currency_code"},
    ]
    assert sap["properties"] == {"module": "SD"}
    assert result[1]["source_name"] == "src.raw.orders"
    assert result[2]["source_kind"] == "file"

    kwargs = ws.external_lineage.list_external_lineage_relationships.call_args.kwargs
    assert kwargs["lineage_direction"] is LineageDirection.UPSTREAM
    assert kwargs["page_size"] == 50  # min(1000, max_relationships default 50)


def test_collect_external_upstream_lineage_respects_max_relationships():
    ws = create_autospec(WorkspaceClient, instance=True)
    infos = [_external_metadata_info(), _external_table_info(), _external_file_info()]

    class CountingIter:
        def __init__(self, items):
            self._items = iter(items)
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            self.count += 1
            return next(self._items)

    counter = CountingIter(infos)
    ws.external_lineage.list_external_lineage_relationships.return_value = counter

    result = collect_external_upstream_lineage(ws, "cat.sch.tab", config=ExternalLineageConfig(max_relationships=1))
    assert len(result) == 1
    # Collector pulls one record, appends it, pulls once more to detect the cap, then breaks —
    # i.e. two ``__next__`` calls total for *max_relationships=1*.
    assert counter.count == 2
    kwargs = ws.external_lineage.list_external_lineage_relationships.call_args.kwargs
    assert kwargs["page_size"] == 1


def test_collect_external_upstream_lineage_degrades_on_sdk_error():
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.external_lineage.list_external_lineage_relationships.side_effect = RuntimeError("api disabled")
    assert not collect_external_upstream_lineage(ws, "cat.sch.tab", config=ExternalLineageConfig())


# -- build_schema_json -------------------------------------------------------------------


def test_build_schema_json_omits_empty_keys(mock_workspace_client):
    """Baseline enrichment with no comments/tags/lineage emits only the required keys."""
    mock_workspace_client.tables.get.return_value = TableInfo(columns=[ColumnInfo(name="id", type_text="string")])
    ws = mock_workspace_client
    ws.external_lineage.list_external_lineage_relationships.return_value = iter([])

    spark = _spark_mock(table_results=[[], []], sql_results=[[], []])  # 2 tag reads + 2 lineage reads

    result = json.loads(
        build_schema_json(
            table_full_name="cat.sch.tab",
            column_dicts=[{"name": "id", "type": "string"}],
            ws=ws,
            spark=spark,
            config=UnityCatalogMetadataConfig(),
        )
    )
    assert result["table"] == "cat.sch.tab"
    assert result["columns"] == [{"name": "id", "type": "string"}]
    assert "table_comment" not in result
    assert "table_tags" not in result
    assert "column_upstream_lineage" not in result
    assert "upstream_tables" not in result
    assert "external_lineage" not in result


def test_build_schema_json_skips_disabled_sub_models(mock_workspace_client):
    """Setting column_upstream_lineage=None and external_lineage=None skips both walks entirely."""
    mock_workspace_client.tables.get.return_value = TableInfo(columns=[ColumnInfo(name="id", type_text="string")])
    ws = mock_workspace_client
    spark = _spark_mock(table_results=[[], []])  # table tags, column tags only

    config = UnityCatalogMetadataConfig(column_upstream_lineage=None, external_lineage=None)
    result = json.loads(
        build_schema_json(
            table_full_name="cat.sch.tab",
            column_dicts=[{"name": "id", "type": "string"}],
            ws=ws,
            spark=spark,
            config=config,
        )
    )
    assert "column_upstream_lineage" not in result
    assert "external_lineage" not in result
    assert "upstream_tables" not in result
    assert spark.table.call_count == 2  # only the two tag reads (DataFrame API)
    spark.sql.assert_not_called()  # no lineage CTEs issued
    ws.external_lineage.list_external_lineage_relationships.assert_not_called()


def test_build_schema_json_includes_comments_and_external_lineage(mock_workspace_client):
    mock_workspace_client.tables.get.return_value = TableInfo(
        comment="Invoice fact table",
        columns=[ColumnInfo(name="invoice_id", type_text="string", comment="Invoice primary key")],
    )
    ws = mock_workspace_client
    ws.external_lineage.list_external_lineage_relationships.return_value = iter([_external_metadata_info()])
    spark = _spark_mock(table_results=[[], []], sql_results=[[], []])  # 2 tag reads + 2 lineage reads

    result = json.loads(
        build_schema_json(
            table_full_name="cat.sch.tab",
            column_dicts=[{"name": "invoice_id", "type": "string"}],
            ws=ws,
            spark=spark,
            config=UnityCatalogMetadataConfig(),
        )
    )
    assert result["table_comment"] == "Invoice fact table"
    assert result["columns"][0]["comment"] == "Invoice primary key"
    assert result["external_lineage"][0]["system_type"] == SystemType.SAP.value
