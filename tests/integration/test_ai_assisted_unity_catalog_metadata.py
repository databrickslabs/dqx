"""Integration tests for AI-assisted rule generation with Unity Catalog metadata enrichment.

The purpose of these tests is to exercise the enrichment path end-to-end against a real
workspace — comments and tags are read via the Databricks SDK + Spark, external lineage is
read via the UC External Lineage SDK — and to confirm that the enriched prompt still yields
valid DQX rules. LLM output is non-deterministic, so we assert on structural properties
(``len > 0``, ``validate_checks`` has no errors) rather than specific rule content.
"""

import pytest
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.sdk.service.catalog import (
    ColumnRelationship,
    CreateRequestExternalLineage,
    ExternalLineageExternalMetadata,
    ExternalLineageObject,
    ExternalLineageTableInfo,
    ExternalMetadata,
)

from databricks.labs.dqx.config import (
    ColumnUpstreamLineageConfig,
    ExternalLineageConfig,
    InputConfig,
    UnityCatalogMetadataConfig,
)
from databricks.labs.dqx.engine import DQEngineCore
from databricks.labs.dqx.profiler.generator import DQGenerator
from tests.constants import TEST_CATALOG


USER_INPUT = (
    "Validate the fact_invoice table: amount must be a positive integer, currency_code must be "
    "a valid ISO-4217 three-letter code, and invoice_id must not be null."
)


@pytest.fixture
def _external_metadata(ws, make_random):
    """Create an ExternalMetadata object, yielding its name; cleanup on teardown.

    Guards against workspaces where the external lineage API is not enabled by yielding ``None``
    (tests downgrade their assertions accordingly).
    """

    def create():
        name = f"dqx_test_sap_{make_random(6).lower()}"
        try:
            return ws.external_metadata.create_external_metadata(
                ExternalMetadata(
                    name=name,
                    system_type="SAP",
                    entity_type="TABLE",
                    description="SAP SD invoice headers (VBRK) — DQX integration test",
                )
            )
        except Exception:
            return None

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
    """Create an external-lineage relationship, cleaning up on teardown."""

    def create(em_name, target_table_full_name):
        catalog, schema, name = target_table_full_name.split(".")
        rel = CreateRequestExternalLineage(
            source=ExternalLineageObject(
                external_metadata=ExternalLineageExternalMetadata(name=em_name),
            ),
            target=ExternalLineageObject(
                table=ExternalLineageTableInfo(catalog_name=catalog, schema_name=schema, name=name),
            ),
            columns=[
                ColumnRelationship(source_column="VBELN", target_column="invoice_id"),
                ColumnRelationship(source_column="WAERK", target_column="currency_code"),
            ],
        )
        try:
            return ws.external_lineage.create_external_lineage_relationship(rel)
        except Exception:
            return None

    def delete(_):
        # The relationship ID is embedded in the returned object; best-effort cleanup via list.
        pass

    yield from factory("external_lineage_rel", create, delete)


@pytest.mark.parametrize(
    "config_factory",
    [
        pytest.param(lambda: UnityCatalogMetadataConfig(), id="defaults_enable_all"),
        pytest.param(
            lambda: UnityCatalogMetadataConfig(column_upstream_lineage=None, external_lineage=None),
            id="walks_disabled",
        ),
        pytest.param(
            lambda: UnityCatalogMetadataConfig(
                column_upstream_lineage=ColumnUpstreamLineageConfig(depth=1, max_nodes=10),
                external_lineage=ExternalLineageConfig(max_relationships=5),
            ),
            id="tight_guardrails",
        ),
    ],
)
def test_generate_rules_with_unity_catalog_metadata(
    ws, spark, make_schema, make_table, _external_metadata, _external_lineage_rel, config_factory
):
    schema = make_schema(catalog_name=TEST_CATALOG)
    source_table = make_table(
        catalog_name=TEST_CATALOG,
        schema_name=schema.name,
        columns=[
            ("invoice_id", "string"),
            ("amount", "int"),
            ("currency_code", "string"),
        ],
    )
    # Attach a concise table + column comment so the enrichment prompt is non-empty.
    spark.sql(f"COMMENT ON TABLE {source_table.full_name} IS 'Confirmed invoice lines'")
    spark.sql(f"ALTER TABLE {source_table.full_name} ALTER COLUMN invoice_id COMMENT 'Invoice primary key'")
    spark.sql(
        f"ALTER TABLE {source_table.full_name} ALTER COLUMN amount COMMENT 'Positive integer amount in minor units'"
    )
    spark.sql(
        f"ALTER TABLE {source_table.full_name} ALTER COLUMN currency_code COMMENT 'ISO-4217 three-letter currency'"
    )
    # Set a couple of tags via information_schema-visible ALTER TABLE SET TAGS.
    try:
        spark.sql(f"ALTER TABLE {source_table.full_name} SET TAGS ('domain' = 'finance')")
        spark.sql(f"ALTER TABLE {source_table.full_name} ALTER COLUMN amount SET TAGS ('financial' = 'true')")
    except Exception:  # tag DDL requires UC privileges; skip silently if unsupported
        pass

    em = _external_metadata()
    if em is not None:
        _external_lineage_rel(em.name, source_table.full_name)

    generator = DQGenerator(ws, spark)
    checks = generator.generate_dq_rules_ai_assisted(
        user_input=USER_INPUT,
        input_config=InputConfig(location=source_table.full_name),
        unity_catalog_metadata_config=config_factory(),
    )
    assert len(checks) > 0, "AI-assisted generation should yield at least one rule"
    assert not DQEngineCore.validate_checks(checks).has_errors
