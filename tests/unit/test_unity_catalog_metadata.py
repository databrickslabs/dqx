"""Unit tests for the Unity Catalog metadata enrichment module.

Covers Pydantic config validators, collectors (table metadata, tags, column + table upstream
lineage, external lineage), and the top-level *build_schema_json* renderer. No real Spark or
workspace is involved — all externals are mocked with *create_autospec* / *MagicMock*.
"""

import json
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ColumnInfo,
    ColumnRelationship,
    ExternalLineageExternalMetadataInfo,
    ExternalLineageFileInfo,
    ExternalLineageInfo,
    ExternalLineageRelationshipInfo,
    ExternalLineageTableInfo,
    LineageDirection,
    SystemType,
    TableInfo,
)
from pydantic import ValidationError
from pyspark.sql import SparkSession

from databricks.labs.dqx.config import (
    ColumnUpstreamLineageConfig,
    ExternalLineageConfig,
    UnityCatalogMetadataConfig,
)
from databricks.labs.dqx.errors import InvalidParameterError
from databricks.labs.dqx.profiler.unity_catalog_metadata import (
    _sql_str_literal,
    _split_three_part_name,
    build_schema_json,
    collect_column_upstream_lineage,
    collect_external_upstream_lineage,
    collect_table_metadata,
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


# -- _sql_str_literal --------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        ("plain", "'plain'"),
        ("has 'quote'", "'has ''quote'''"),
        ("has \\ backslash", "'has \\\\ backslash'"),
        ("trailing\\", "'trailing\\\\'"),
    ],
)
def test_sql_str_literal(value, expected):
    assert _sql_str_literal(value) == expected


# -- _split_three_part_name --------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["catalog.schema.table", "`a`.`b`.`c`"],
)
def test_split_three_part_name_valid(name):
    assert len(_split_three_part_name(name)) == 3


@pytest.mark.parametrize(
    "name",
    ["two.part", "one", "a.b.c.d", "..", "a..c"],
)
def test_split_three_part_name_rejects_bad_input(name):
    with pytest.raises(InvalidParameterError):
        _split_three_part_name(name)


# -- collect_table_metadata --------------------------------------------------------------


def test_collect_table_metadata_includes_comments(mock_workspace_client):
    mock_workspace_client.tables.get.return_value = TableInfo(
        comment="Source table",
        columns=[
            ColumnInfo(name="id", type_text="string", comment="Primary key"),
            ColumnInfo(name="val", type_text="int", comment=None),
        ],
    )
    result = collect_table_metadata(
        mock_workspace_client,
        "cat.sch.tab",
        include_table_comment=True,
        include_column_comments=True,
    )
    assert result["comment"] == "Source table"
    assert result["columns"][0] == {"name": "id", "type": "string", "comment": "Primary key"}
    assert result["columns"][1] == {"name": "val", "type": "int"}  # comment key omitted


def test_collect_table_metadata_flags_disable_comments(mock_workspace_client):
    mock_workspace_client.tables.get.return_value = TableInfo(
        comment="Source table",
        columns=[ColumnInfo(name="id", type_text="string", comment="PK")],
    )
    result = collect_table_metadata(
        mock_workspace_client,
        "cat.sch.tab",
        include_table_comment=False,
        include_column_comments=False,
    )
    assert "comment" not in result
    assert "comment" not in result["columns"][0]


def test_collect_table_metadata_tolerates_sdk_error(mock_workspace_client):
    mock_workspace_client.tables.get.side_effect = RuntimeError("boom")
    result = collect_table_metadata(
        mock_workspace_client,
        "cat.sch.tab",
        include_table_comment=True,
        include_column_comments=True,
    )
    assert result == {"name": "cat.sch.tab", "columns": []}


# -- collect_table_tags ------------------------------------------------------------------


def _make_spark_sql_mock(responses: list[list[dict]]):
    """Return a Mock spark where successive spark.sql(...).collect() calls return the given rows."""
    spark = MagicMock(spec=SparkSession)
    sql_mocks = []
    for rows in responses:
        collect_mock = MagicMock()
        collect_mock.collect.return_value = [MagicMock(**{"__getitem__": lambda _, k, r=r: r[k]}) for r in rows]
        sql_mocks.append(collect_mock)
    spark.sql.side_effect = sql_mocks
    return spark


def test_collect_table_tags_merges_rows():
    spark = _make_spark_sql_mock(
        [
            [{"tag_name": "pii", "tag_value": "yes"}, {"tag_name": "domain", "tag_value": "finance"}],
            [
                {"column_name": "id", "tag_name": "pk", "tag_value": "true"},
                {"column_name": "id", "tag_name": "pii", "tag_value": "client"},
                {"column_name": "amount", "tag_name": "financial", "tag_value": "true"},
            ],
        ]
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


def test_collect_table_tags_degrades_on_sql_error():
    spark = MagicMock(spec=SparkSession)
    spark.sql.side_effect = RuntimeError("denied")
    assert collect_table_tags(spark, "cat.sch.tab") == {"table_tags": [], "column_tags": {}}


def test_collect_table_tags_rejects_bad_name():
    spark = MagicMock(spec=SparkSession)
    # Bad name — collector logs a warning and returns the empty structure without hitting Spark.
    assert collect_table_tags(spark, "two.part") == {"table_tags": [], "column_tags": {}}
    spark.sql.assert_not_called()


# -- collect_column_upstream_lineage -----------------------------------------------------


def _make_spark_with_sql_capture():
    """Return a mock SparkSession whose spark.sql call is captured for inspection."""
    spark = MagicMock(spec=SparkSession)
    temp_df = MagicMock()
    spark.createDataFrame.return_value = temp_df
    temp_df.createOrReplaceTempView = MagicMock()
    return spark


def test_collect_column_upstream_lineage_sql_has_cycle_guard_and_limits():
    spark = _make_spark_with_sql_capture()
    spark.sql.return_value.collect.return_value = []

    collect_column_upstream_lineage(
        spark,
        "cat.sch.tab",
        ["id"],
        config=ColumnUpstreamLineageConfig(depth=3, lookback_days=10, max_nodes=25),
    )
    assert spark.sql.call_count == 1
    sql_text = spark.sql.call_args.args[0]
    assert "WITH RECURSIVE edges" in sql_text
    assert "NOT array_contains(e.path" in sql_text
    assert "LIMIT 25" in sql_text  # per-member and tail LIMITs
    assert "INTERVAL 10 DAYS" in sql_text
    assert "e.depth < 3" in sql_text
    # Anchor table literal is escaped via _sql_str_literal (single quotes doubled on bare 'tab').
    assert "'cat.sch.tab'" in sql_text
    spark.catalog.dropTempView.assert_called_once()


def test_collect_column_upstream_lineage_unbounded_depth_omits_predicate():
    spark = _make_spark_with_sql_capture()
    spark.sql.return_value.collect.return_value = []

    collect_column_upstream_lineage(
        spark,
        "cat.sch.tab",
        ["id"],
        config=ColumnUpstreamLineageConfig(depth=None),
    )
    sql_text = spark.sql.call_args.args[0]
    assert "e.depth <" not in sql_text


def test_collect_column_upstream_lineage_drops_view_on_sql_failure():
    spark = _make_spark_with_sql_capture()
    spark.sql.side_effect = RuntimeError("permission denied")

    result = collect_column_upstream_lineage(
        spark,
        "cat.sch.tab",
        ["id"],
        config=ColumnUpstreamLineageConfig(),
    )
    assert result == []
    spark.catalog.dropTempView.assert_called_once()


def test_collect_column_upstream_lineage_no_seed_columns_short_circuits():
    spark = _make_spark_with_sql_capture()
    result = collect_column_upstream_lineage(
        spark,
        "cat.sch.tab",
        [],
        config=ColumnUpstreamLineageConfig(),
    )
    assert result == []
    spark.sql.assert_not_called()


def test_collect_upstream_table_lineage_builds_recursive_cte():
    spark = _make_spark_with_sql_capture()
    spark.sql.return_value.collect.return_value = []
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


def _external_metadata_info(system_type=SystemType.SAP):
    return ExternalLineageInfo(
        external_metadata_info=ExternalLineageExternalMetadataInfo(
            name="sap_sd_invoices",
            system_type=system_type,
            entity_type="TABLE",
        ),
        external_lineage_info=ExternalLineageRelationshipInfo(
            source=None,
            target=None,
            columns=[
                ColumnRelationship(source="VBELN", target="invoice_id"),
                ColumnRelationship(source="WAERK", target="currency_code"),
            ],
            properties={"module": "SD"},
        ),
    )


def _external_table_info():
    return ExternalLineageInfo(
        table_info=ExternalLineageTableInfo(catalog_name="src", schema_name="raw", name="orders"),
        external_lineage_info=ExternalLineageRelationshipInfo(source=None, target=None),
    )


def _external_file_info():
    return ExternalLineageInfo(
        file_info=ExternalLineageFileInfo(
            path="s3://bucket/path", securable_name="volumes.ext", securable_type="VOLUME"
        ),
        external_lineage_info=ExternalLineageRelationshipInfo(source=None, target=None),
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
    # Only one __next__ consumed before the cap breaks the loop.
    assert counter.count == 1
    kwargs = ws.external_lineage.list_external_lineage_relationships.call_args.kwargs
    assert kwargs["page_size"] == 1


def test_collect_external_upstream_lineage_degrades_on_sdk_error():
    ws = create_autospec(WorkspaceClient, instance=True)
    ws.external_lineage.list_external_lineage_relationships.side_effect = RuntimeError("api disabled")
    assert collect_external_upstream_lineage(ws, "cat.sch.tab", config=ExternalLineageConfig()) == []


# -- build_schema_json -------------------------------------------------------------------


def test_build_schema_json_omits_empty_keys(mock_workspace_client):
    """Baseline enrichment with no comments/tags/lineage emits only the required keys."""
    mock_workspace_client.tables.get.return_value = TableInfo(columns=[ColumnInfo(name="id", type_text="string")])
    ws = mock_workspace_client

    spark = _make_spark_sql_mock([[], []])  # empty tag reads
    spark.sql.side_effect = [
        MagicMock(collect=MagicMock(return_value=[])),  # table tags
        MagicMock(collect=MagicMock(return_value=[])),  # column tags
        MagicMock(collect=MagicMock(return_value=[])),  # column upstream lineage
        MagicMock(collect=MagicMock(return_value=[])),  # upstream table lineage
    ]
    spark.createDataFrame = MagicMock()

    ws.external_lineage.list_external_lineage_relationships.return_value = iter([])

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

    spark = MagicMock(spec=SparkSession)
    spark.sql.side_effect = [
        MagicMock(collect=MagicMock(return_value=[])),  # table tags
        MagicMock(collect=MagicMock(return_value=[])),  # column tags
    ]

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
    # Only two spark.sql calls (both tag reads); no lineage CTE invoked.
    assert spark.sql.call_count == 2
    # External lineage API is not called.
    ws.external_lineage.list_external_lineage_relationships.assert_not_called()


def test_build_schema_json_includes_comments_and_external_lineage(mock_workspace_client):
    mock_workspace_client.tables.get.return_value = TableInfo(
        comment="Invoice fact table",
        columns=[
            ColumnInfo(name="invoice_id", type_text="string", comment="Invoice primary key"),
        ],
    )
    ws = mock_workspace_client
    ws.external_lineage.list_external_lineage_relationships.return_value = iter([_external_metadata_info()])

    spark = MagicMock(spec=SparkSession)
    spark.sql.side_effect = [
        MagicMock(collect=MagicMock(return_value=[])),  # table tags
        MagicMock(collect=MagicMock(return_value=[])),  # column tags
        MagicMock(collect=MagicMock(return_value=[])),  # column upstream lineage
        MagicMock(collect=MagicMock(return_value=[])),  # upstream table lineage
    ]
    spark.createDataFrame = MagicMock()

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
