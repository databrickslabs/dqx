"""Integration tests for the Unity Catalog metadata collectors.

Exercises each collector in isolation against a real workspace. UC system tables
(*system.access.table_lineage*, *system.access.column_lineage*,
*system.information_schema.table_tags*, *system.information_schema.column_tags*) are
replaced by per-test stub tables that mirror the real schema and are seeded with
case-specific rows, and the module-level constants
(``_TABLE_LINEAGE_TABLE`` / ``_COLUMN_LINEAGE_TABLE`` / ``_TABLE_TAGS_TABLE`` /
``_COLUMN_TAGS_TABLE``) are monkeypatched to point at them. This removes the lineage /
tag propagation delay otherwise inherent to UC system tables and lets each test assert
on the exact edges and tags it produced.

The collectors still execute the real recursive-CTE and the real DataFrame API against
these stub tables, so SQL and binding regressions are caught end-to-end.

* ``collect_table_comments`` goes through the SDK (not a system table) and uses the real
  ``COMMENT ON TABLE`` / ``ALTER TABLE ... ALTER COLUMN ... COMMENT`` DDL.
* ``collect_external_upstream_lineage`` goes through the SDK's ``ExternalLineageAPI`` and
  uses the ``make_external_metadata`` / ``make_external_lineage_relationship`` fixtures.

LLM-adjacent concerns live in ``test_ai_assisted_unity_catalog_metadata.py``; this suite is
strictly about the collector-level contract.
"""

from types import SimpleNamespace

import pytest
from databricks.sdk.service.catalog import SystemType

from databricks.labs.dqx.config import ColumnUpstreamLineageConfig, ExternalLineageConfig
from databricks.labs.dqx.profiler import unity_catalog_metadata as uc_metadata
from databricks.labs.dqx.profiler.unity_catalog_metadata import (
    collect_column_upstream_lineage,
    collect_external_upstream_lineage,
    collect_table_comments,
    collect_table_tags,
    collect_upstream_table_lineage,
)
from tests.constants import TEST_CATALOG


@pytest.fixture
def stub_system_tables(spark, make_schema, make_random, monkeypatch):
    """Create per-test replacements for the four UC system tables consumed by the collectors
    and point the module constants at them. ``make_schema`` drops the schema ``CASCADE`` so the
    stub tables are cleaned up automatically; ``monkeypatch`` restores the constants after the
    test."""
    schema = make_schema(catalog_name=TEST_CATALOG)
    suffix = make_random(6).lower()

    table_lineage = f"{TEST_CATALOG}.{schema.name}.sys_table_lineage_{suffix}"
    column_lineage = f"{TEST_CATALOG}.{schema.name}.sys_column_lineage_{suffix}"
    table_tags = f"{TEST_CATALOG}.{schema.name}.sys_table_tags_{suffix}"
    column_tags = f"{TEST_CATALOG}.{schema.name}.sys_column_tags_{suffix}"

    spark.sql(
        f"CREATE TABLE {table_lineage} ("
        "source_table_full_name STRING, target_table_full_name STRING, event_time TIMESTAMP)"
    )
    spark.sql(
        f"CREATE TABLE {column_lineage} ("
        "source_table_full_name STRING, source_column STRING, "
        "target_table_full_name STRING, target_column STRING, event_time TIMESTAMP)"
    )
    spark.sql(
        f"CREATE TABLE {table_tags} ("
        "catalog_name STRING, schema_name STRING, table_name STRING, tag_name STRING, tag_value STRING)"
    )
    spark.sql(
        f"CREATE TABLE {column_tags} ("
        "catalog_name STRING, schema_name STRING, table_name STRING, column_name STRING, "
        "tag_name STRING, tag_value STRING)"
    )

    monkeypatch.setattr(uc_metadata, "_TABLE_LINEAGE_TABLE", table_lineage)
    monkeypatch.setattr(uc_metadata, "_COLUMN_LINEAGE_TABLE", column_lineage)
    monkeypatch.setattr(uc_metadata, "_TABLE_TAGS_TABLE", table_tags)
    monkeypatch.setattr(uc_metadata, "_COLUMN_TAGS_TABLE", column_tags)

    return SimpleNamespace(
        schema=schema,
        table_lineage=table_lineage,
        column_lineage=column_lineage,
        table_tags=table_tags,
        column_tags=column_tags,
    )


def test_collect_table_comments_returns_table_and_column_values(ws, spark, make_schema, make_table):
    schema = make_schema(catalog_name=TEST_CATALOG)
    table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("invoice_id", "string"), ("amount", "int"), ("currency_code", "string")],
    )
    spark.sql(f"COMMENT ON TABLE {table.full_name} IS 'Confirmed invoice lines'")
    spark.sql(f"ALTER TABLE {table.full_name} ALTER COLUMN invoice_id COMMENT 'Invoice primary key'")
    spark.sql(f"ALTER TABLE {table.full_name} ALTER COLUMN amount COMMENT 'Positive integer amount'")

    table_comment, column_comments = collect_table_comments(ws, table.full_name)
    assert table_comment == "Confirmed invoice lines"
    assert column_comments == {
        "invoice_id": "Invoice primary key",
        "amount": "Positive integer amount",
    }, "columns without a comment must not appear in the map"


def test_collect_table_tags_returns_table_and_column_tags(spark, stub_system_tables):
    catalog, schema_name, table_name = TEST_CATALOG, stub_system_tables.schema.name, "invoices"
    target = f"{catalog}.{schema_name}.{table_name}"

    spark.sql(
        f"INSERT INTO {stub_system_tables.table_tags} VALUES "
        f"('{catalog}', '{schema_name}', '{table_name}', 'domain', 'finance')"
    )
    spark.sql(
        f"INSERT INTO {stub_system_tables.column_tags} VALUES "
        f"('{catalog}', '{schema_name}', '{table_name}', 'amount', 'financial', 'true'), "
        f"('{catalog}', '{schema_name}', '{table_name}', 'invoice_id', 'pii', 'client')"
    )

    result = collect_table_tags(spark, target)

    assert result["table_tags"] == [{"key": "domain", "value": "finance"}]
    assert result["column_tags"]["amount"] == [{"key": "financial", "value": "true"}]
    assert result["column_tags"]["invoice_id"] == [{"key": "pii", "value": "client"}]
    assert "currency_code" not in result["column_tags"], "columns without tags must not appear"


def test_collect_upstream_table_lineage_finds_direct_predecessor(spark, stub_system_tables):
    source = f"{TEST_CATALOG}.{stub_system_tables.schema.name}.invoices_source"
    derived = f"{TEST_CATALOG}.{stub_system_tables.schema.name}.invoices_derived"

    spark.sql(
        f"INSERT INTO {stub_system_tables.table_lineage} VALUES " f"('{source}', '{derived}', current_timestamp())"
    )

    config = ColumnUpstreamLineageConfig(depth=2, lookback_days=1, max_nodes=50)
    edges = collect_upstream_table_lineage(spark, derived, config=config)

    assert edges == [{"predecessor": derived, "target_table": source, "depth": 1}]


def test_collect_upstream_table_lineage_walks_multiple_hops(spark, stub_system_tables):
    bronze = f"{TEST_CATALOG}.{stub_system_tables.schema.name}.invoices_bronze"
    silver = f"{TEST_CATALOG}.{stub_system_tables.schema.name}.invoices_silver"
    gold = f"{TEST_CATALOG}.{stub_system_tables.schema.name}.invoices_gold"

    spark.sql(
        f"INSERT INTO {stub_system_tables.table_lineage} VALUES "
        f"('{silver}', '{gold}', current_timestamp()), "
        f"('{bronze}', '{silver}', current_timestamp())"
    )

    config = ColumnUpstreamLineageConfig(depth=3, lookback_days=1, max_nodes=50)
    edges = collect_upstream_table_lineage(spark, gold, config=config)

    assert {(e["predecessor"], e["target_table"], e["depth"]) for e in edges} == {
        (gold, silver, 1),
        (silver, bronze, 2),
    }


def test_collect_column_upstream_lineage_returns_seeded_edge(spark, stub_system_tables):
    source = f"{TEST_CATALOG}.{stub_system_tables.schema.name}.invoices_source"
    derived = f"{TEST_CATALOG}.{stub_system_tables.schema.name}.invoices_derived"

    spark.sql(
        f"INSERT INTO {stub_system_tables.column_lineage} VALUES "
        f"('{source}', 'invoice_id', '{derived}', 'invoice_id', current_timestamp()), "
        f"('{source}', 'amount', '{derived}', 'amount', current_timestamp())"
    )

    config = ColumnUpstreamLineageConfig(depth=2, lookback_days=1, max_nodes=50)
    edges = collect_column_upstream_lineage(spark, derived, ["invoice_id"], config=config)

    assert edges == [
        {
            "predecessor": derived,
            "source_table": source,
            "source_column": "invoice_id",
            "target_column": "invoice_id",
            "depth": 1,
        }
    ]


def test_collect_column_upstream_lineage_walks_multiple_hops(spark, stub_system_tables):
    bronze = f"{TEST_CATALOG}.{stub_system_tables.schema.name}.invoices_bronze"
    silver = f"{TEST_CATALOG}.{stub_system_tables.schema.name}.invoices_silver"
    gold = f"{TEST_CATALOG}.{stub_system_tables.schema.name}.invoices_gold"

    spark.sql(
        f"INSERT INTO {stub_system_tables.column_lineage} VALUES "
        f"('{silver}', 'invoice_id', '{gold}', 'invoice_id', current_timestamp()), "
        f"('{bronze}', 'invoice_id', '{silver}', 'invoice_id', current_timestamp())"
    )

    config = ColumnUpstreamLineageConfig(depth=3, lookback_days=1, max_nodes=50)
    edges = collect_column_upstream_lineage(spark, gold, ["invoice_id"], config=config)

    hops = {(e["predecessor"], e["source_table"], e["depth"]) for e in edges}
    assert hops == {(gold, silver, 1), (silver, bronze, 2)}


def test_collect_external_upstream_lineage_returns_sap_metadata(
    ws, make_schema, make_table, make_external_metadata, make_external_lineage_relationship
):
    schema = make_schema(catalog_name=TEST_CATALOG)
    target = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("invoice_id", "string"), ("currency_code", "string")],
    )
    external_metadata = make_external_metadata()
    make_external_lineage_relationship(
        external_metadata_name=external_metadata.name,
        target_table_full_name=target.full_name,
        column_mappings=[("VBELN", "invoice_id"), ("WAERK", "currency_code")],
    )

    results = collect_external_upstream_lineage(ws, target.full_name, config=ExternalLineageConfig())

    sap_records = [
        r for r in results if r["source_kind"] == "external_metadata" and r["source_name"] == external_metadata.name
    ]
    assert sap_records, f"expected an external_metadata record for {external_metadata.name!r}, got {results!r}"
    sap = sap_records[0]
    assert sap["system_type"] == SystemType.SAP.value
    assert sap["entity_type"] == "TABLE"
    assert {"source": "VBELN", "target": "invoice_id"} in sap["columns"]
    assert {"source": "WAERK", "target": "currency_code"} in sap["columns"]
