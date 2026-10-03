"""Integration tests for the Unity Catalog metadata collectors.

Exercises each collector in isolation against a real workspace so we have confidence that
the SDK + Spark queries that back the LLM prompt enrichment keep working end-to-end:

* ``collect_table_metadata`` returns the table-level and per-column UC comments we set via
  ``COMMENT ON TABLE`` / ``ALTER TABLE … ALTER COLUMN … COMMENT``.
* ``collect_table_tags`` returns the table-level and per-column tags we set via
  ``ALTER TABLE … SET TAGS`` and ``ALTER TABLE … ALTER COLUMN … SET TAGS`` as surfaced by
  ``system.information_schema.table_tags`` / ``column_tags``.
* ``collect_upstream_table_lineage`` returns the direct upstream table discovered through
  ``system.access.table_lineage`` after a ``CREATE TABLE AS SELECT``.
* ``collect_external_upstream_lineage`` returns an upstream SAP ``ExternalMetadata`` object
  linked to the UC table through the ``ExternalLineageAPI``.

LLM-adjacent concerns live in ``test_ai_assisted_unity_catalog_metadata.py``; this suite is
strictly about the collector-level contract.
"""

import pytest
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.sdk.service.catalog import (
    ColumnRelationship,
    CreateRequestExternalLineage,
    DeleteRequestExternalLineage,
    ExternalLineageExternalMetadata,
    ExternalLineageObject,
    ExternalLineageTable,
    ExternalMetadata,
    SystemType,
)

from databricks.labs.dqx.config import ColumnUpstreamLineageConfig, ExternalLineageConfig
from databricks.labs.dqx.profiler.unity_catalog_metadata import (
    collect_column_upstream_lineage,
    collect_external_upstream_lineage,
    collect_table_metadata,
    collect_table_tags,
    collect_upstream_table_lineage,
)
from tests.constants import TEST_CATALOG


@pytest.fixture
def _external_metadata(ws, make_random):
    """Create an ``ExternalMetadata`` object; clean up on teardown."""

    def create():
        name = f"dqx_test_sap_{make_random(6).lower()}"
        return ws.external_metadata.create_external_metadata(
            ExternalMetadata(
                name=name,
                system_type=SystemType.SAP,
                entity_type="TABLE",
                description="SAP SD invoice headers (VBRK) — DQX integration test",
            )
        )

    def delete(em):
        if em is None:
            return
        try:
            ws.external_metadata.delete_external_metadata(name=em.name)
        except Exception:
            pass

    yield from factory("external_metadata", create, delete)


@pytest.fixture
def _external_lineage_rel(ws):
    """Create an external-lineage relationship pointing to a UC table; clean up on teardown."""

    def create(em_name: str, target_table_full_name: str):
        catalog, schema, name = target_table_full_name.split(".")
        request = CreateRequestExternalLineage(
            source=ExternalLineageObject(
                external_metadata=ExternalLineageExternalMetadata(name=em_name),
            ),
            target=ExternalLineageObject(
                table=ExternalLineageTable(name=f"{catalog}.{schema}.{name}"),
            ),
            columns=[
                ColumnRelationship(source="VBELN", target="invoice_id"),
                ColumnRelationship(source="WAERK", target="currency_code"),
            ],
        )
        return (ws.external_lineage.create_external_lineage_relationship(request), em_name, target_table_full_name)

    def delete(created):
        if created is None:
            return
        _rel, em_name, target_table_full_name = created
        catalog, schema, name = target_table_full_name.split(".")
        try:
            ws.external_lineage.delete_external_lineage_relationship(
                DeleteRequestExternalLineage(
                    source=ExternalLineageObject(
                        external_metadata=ExternalLineageExternalMetadata(name=em_name),
                    ),
                    target=ExternalLineageObject(
                        table=ExternalLineageTable(name=f"{catalog}.{schema}.{name}"),
                    ),
                )
            )
        except Exception:
            pass

    yield from factory("external_lineage_rel", create, delete)


def test_collect_table_metadata_returns_table_and_column_comments(ws, spark, make_schema, make_table):
    schema = make_schema(catalog_name=TEST_CATALOG)
    table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("invoice_id", "string"), ("amount", "int"), ("currency_code", "string")],
    )
    spark.sql(f"COMMENT ON TABLE {table.full_name} IS 'Confirmed invoice lines'")
    spark.sql(f"ALTER TABLE {table.full_name} ALTER COLUMN invoice_id COMMENT 'Invoice primary key'")
    spark.sql(f"ALTER TABLE {table.full_name} ALTER COLUMN amount COMMENT 'Positive integer amount'")

    result = collect_table_metadata(
        ws,
        table.full_name,
        include_table_comment=True,
        include_column_comments=True,
    )

    assert result["comment"] == "Confirmed invoice lines"
    by_name = {c["name"]: c for c in result["columns"]}
    assert by_name["invoice_id"]["comment"] == "Invoice primary key"
    assert by_name["amount"]["comment"] == "Positive integer amount"
    assert "comment" not in by_name["currency_code"], "columns without a comment must omit the key"


def test_collect_table_tags_returns_table_and_column_tags(ws, spark, make_schema, make_table):
    schema = make_schema(catalog_name=TEST_CATALOG)
    table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("invoice_id", "string"), ("amount", "int"), ("currency_code", "string")],
    )
    spark.sql(f"ALTER TABLE {table.full_name} SET TAGS ('domain' = 'finance')")
    spark.sql(f"ALTER TABLE {table.full_name} ALTER COLUMN amount SET TAGS ('financial' = 'true')")
    spark.sql(f"ALTER TABLE {table.full_name} ALTER COLUMN invoice_id SET TAGS ('pii' = 'client')")

    result = collect_table_tags(spark, table.full_name)

    assert {"key": "domain", "value": "finance"} in result["table_tags"]
    assert result["column_tags"]["amount"] == [{"key": "financial", "value": "true"}]
    assert result["column_tags"]["invoice_id"] == [{"key": "pii", "value": "client"}]
    assert "currency_code" not in result["column_tags"], "columns without tags must not appear"


def test_collect_upstream_table_lineage_finds_direct_predecessor(ws, spark, make_schema, make_table):
    schema = make_schema(catalog_name=TEST_CATALOG)
    source = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("invoice_id", "string"), ("amount", "int")],
    )
    # CTAS from `source` → `derived`; the write registers an edge in system.access.table_lineage.
    derived_full_name = f"{TEST_CATALOG}.{schema.name}.invoices_derived"
    spark.sql(f"CREATE TABLE {derived_full_name} AS SELECT invoice_id, amount FROM {source.full_name}")
    try:
        # system.access.table_lineage is populated asynchronously; use a generous lookback and node cap.
        config = ColumnUpstreamLineageConfig(depth=2, lookback_days=1, max_nodes=50)
        edges = collect_upstream_table_lineage(spark, derived_full_name, config=config)

        upstream_tables = {edge["target_table"] for edge in edges}
        assert source.full_name in upstream_tables, (
            f"expected {source.full_name!r} among upstream edges of {derived_full_name!r}, got {upstream_tables!r}"
        )
        assert all(edge["depth"] >= 1 for edge in edges)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {derived_full_name}")


def test_collect_column_upstream_lineage_executes_against_real_system_tables(ws, spark, make_schema, make_table):
    """Walk column lineage for a seeded column; a freshly created lineage edge may not have
    propagated to ``system.access.column_lineage`` by the time the test runs, so we assert the
    collector returns a well-formed list (empty or populated) rather than requiring a specific
    edge. The purpose is to confirm the recursive CTE parses, runs, and binds against the real
    system table."""
    schema = make_schema(catalog_name=TEST_CATALOG)
    source = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("invoice_id", "string"), ("amount", "int")],
    )
    derived_full_name = f"{TEST_CATALOG}.{schema.name}.invoices_derived"
    spark.sql(f"CREATE TABLE {derived_full_name} AS SELECT invoice_id, amount FROM {source.full_name}")
    try:
        config = ColumnUpstreamLineageConfig(depth=2, lookback_days=1, max_nodes=50)
        edges = collect_column_upstream_lineage(spark, derived_full_name, ["invoice_id"], config=config)

        assert isinstance(edges, list)
        for edge in edges:
            assert set(edge) == {"predecessor", "source_table", "source_column", "target_column", "depth"}
            assert edge["depth"] >= 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {derived_full_name}")


def test_collect_external_upstream_lineage_returns_sap_metadata(
    ws, spark, make_schema, make_table, _external_metadata, _external_lineage_rel
):
    schema = make_schema(catalog_name=TEST_CATALOG)
    target = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[("invoice_id", "string"), ("currency_code", "string")],
    )
    em = _external_metadata()
    _external_lineage_rel(em.name, target.full_name)

    results = collect_external_upstream_lineage(ws, target.full_name, config=ExternalLineageConfig())

    sap_records = [r for r in results if r["source_kind"] == "external_metadata" and r["source_name"] == em.name]
    assert sap_records, f"expected an external_metadata record for {em.name!r}, got {results!r}"
    sap = sap_records[0]
    assert sap["system_type"] == SystemType.SAP.value
    assert sap["entity_type"] == "TABLE"
    assert {"source": "VBELN", "target": "invoice_id"} in sap["columns"]
    assert {"source": "WAERK", "target": "currency_code"} in sap["columns"]
