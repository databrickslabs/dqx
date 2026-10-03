"""Integration tests for AI-assisted rule generation with Unity Catalog metadata enrichment.

The purpose of these tests is to exercise the enrichment path end-to-end against a real
workspace — comments and tags are read via the Databricks SDK + Spark, external lineage is
read via the UC External Lineage SDK — and to confirm that the enriched prompt still yields
valid DQX rules. LLM output is non-deterministic, so we assert on structural properties
(``len > 0``, ``validate_checks`` has no errors) rather than specific rule content.
"""

import pytest

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


@pytest.mark.parametrize(
    "config_factory",
    [
        pytest.param(UnityCatalogMetadataConfig, id="defaults_enable_all"),
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
    ws,
    spark,
    make_schema,
    make_table,
    make_external_metadata,
    make_external_lineage_relationship,
    config_factory,
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
    spark.sql(f"COMMENT ON TABLE {source_table.full_name} IS 'Confirmed invoice lines'")
    spark.sql(f"ALTER TABLE {source_table.full_name} ALTER COLUMN invoice_id COMMENT 'Invoice primary key'")
    spark.sql(
        f"ALTER TABLE {source_table.full_name} ALTER COLUMN amount COMMENT 'Positive integer amount in minor units'"
    )
    spark.sql(
        f"ALTER TABLE {source_table.full_name} ALTER COLUMN currency_code COMMENT 'ISO-4217 three-letter currency'"
    )
    try:
        spark.sql(f"ALTER TABLE {source_table.full_name} SET TAGS ('domain' = 'finance')")
        spark.sql(f"ALTER TABLE {source_table.full_name} ALTER COLUMN amount SET TAGS ('financial' = 'true')")
    except Exception:  # tag DDL requires UC privileges; skip silently if unsupported
        pass

    external_metadata = make_external_metadata()
    make_external_lineage_relationship(
        external_metadata.name,
        source_table.full_name,
        column_mappings=[("VBELN", "invoice_id"), ("WAERK", "currency_code")],
    )

    generator = DQGenerator(ws, spark)
    checks = generator.generate_dq_rules_ai_assisted(
        user_input=USER_INPUT,
        input_config=InputConfig(location=source_table.full_name),
        unity_catalog_metadata_config=config_factory(),
    )
    assert len(checks) > 0, "AI-assisted generation should yield at least one rule"
    assert not DQEngineCore.validate_checks(checks).has_errors
