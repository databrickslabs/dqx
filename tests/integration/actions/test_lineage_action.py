"""Integration test suite for *CollectLineageAction*.

Four scenarios exercise the recursive-CTE walker against live *system.access.table_lineage*:

1. *test_upstream_multi_level* — a bronze₁ + bronze₂ ∪→ silver → gold graph; upstream walk
   from *gold* must surface both *silver* (depth 1) and the two bronze tables (depth 2).
2. *test_downstream_multi_level* — same graph; downstream walk from *bronze₁* must surface
   *silver* (depth 1) and *gold* (depth 2).
3. *test_recursive_job_self_write* — a single table repeatedly rewritten from itself must not
   produce cyclic descendants; a synthesised *a → b → a → c* graph must not surface *c* from
   *a* via the cyclic hop (in-CTE full-path cycle guard).
4. *test_writes_delta_and_emits_extras* — smoke check for the persisted schema and *extras*
   contract.

All four are skipped by default via *DQX_SKIP_LINEAGE_SYSTEM_TABLES=1* — system-table
propagation lag can make them flaky in short-lived environments. Set the env var to *0*/*false*
to opt in.
"""

import os
from collections.abc import Callable, Generator
from datetime import datetime, timezone
from typing import Any

import pytest
from pyspark.sql import SparkSession

from databricks.sdk import WorkspaceClient
from databricks.labs.pytester.fixtures.baseline import factory

from databricks.labs.dqx.actions.base import ActionContext, ActionServices, ActionStatus
from databricks.labs.dqx.actions.delivery import WebhookClient
from databricks.labs.dqx.actions.lineage import (
    CollectLineageAction,
    LINEAGE_TABLE_SCHEMA,
    LineageActionConfig,
    LineageSearchConfig,
    LineageEntitySearchConfig,
)
from databricks.labs.dqx.actions.secrets import SecretResolver
from databricks.labs.dqx.config import OutputConfig


# When system tables have not caught up, propagation-dependent assertions become flaky. Set the
# env var to "0" / "false" to opt-in for the full walk assertions.
_SKIP_LINEAGE = pytest.mark.skipif(
    os.environ.get("DQX_SKIP_LINEAGE_SYSTEM_TABLES", "1") not in ("0", "false", "False"),
    reason="system.access.table_lineage propagation lag can make this test flaky; opt in via "
    "DQX_SKIP_LINEAGE_SYSTEM_TABLES=0.",
)


# ---------------------------------------------------------------------------
# Fixtures — factory-based table lifecycle
# ---------------------------------------------------------------------------


@pytest.fixture
def make_lineage_sink(spark: SparkSession, make_schema, make_random) -> Generator[Callable[..., str], None, None]:
    """Provision a UC location for the lineage sink; DROP TABLE IF EXISTS on teardown."""
    schema = make_schema()

    def create(**_kwargs: Any) -> str:
        return f"{schema.full_name}.dqx_lineage_{make_random(6).lower()}"

    def delete(name: str) -> None:
        spark.sql(f"DROP TABLE IF EXISTS {name}")

    yield from factory("lineage_sink", create, delete)


@pytest.fixture
def make_derived_table(spark: SparkSession, make_schema, make_random) -> Generator[Callable[..., str], None, None]:
    """Factory: *CREATE OR REPLACE TABLE ... USING DELTA AS <select_sql>*.

    Returned callable takes keyword args *select_sql* and optional *schema* / *name_prefix*. The
    fixture guarantees DROP on failure.
    """
    default_schema = make_schema()

    def create(*, select_sql: str, schema: Any | None = None, name_prefix: str = "t") -> str:
        target_schema = schema if schema is not None else default_schema
        table_name = f"{target_schema.full_name}.{name_prefix}_{make_random(6).lower()}"
        spark.sql(f"CREATE OR REPLACE TABLE {table_name} USING DELTA AS {select_sql}")
        return table_name

    def delete(name: str) -> None:
        spark.sql(f"DROP TABLE IF EXISTS {name}")

    yield from factory("derived_table", create, delete)


@pytest.fixture
def make_seed_table(spark: SparkSession, make_schema, make_random) -> Generator[Callable[..., str], None, None]:
    """Factory: create a seeded Delta table with two INT-STRING rows for lineage anchoring."""
    default_schema = make_schema()

    def create(*, schema: Any | None = None, name_prefix: str = "src") -> str:
        target_schema = schema if schema is not None else default_schema
        table_name = f"{target_schema.full_name}.{name_prefix}_{make_random(6).lower()}"
        spark.sql(f"CREATE OR REPLACE TABLE {table_name} (id INT, value STRING) USING DELTA")
        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b')")
        return table_name

    def delete(name: str) -> None:
        spark.sql(f"DROP TABLE IF EXISTS {name}")

    yield from factory("seed_table", create, delete)


def _make_services(spark: SparkSession, ws: WorkspaceClient) -> ActionServices:
    return ActionServices(
        secret_resolver=SecretResolver(ws),
        webhook_client=WebhookClient(),
        ws=ws,
        spark=spark,
    )


def _make_context(input_location: str, output_location: str | None = None) -> ActionContext:
    return ActionContext(
        metrics={"error_row_count": 1},
        run_id="integration-lineage-run",
        run_time=datetime.now(timezone.utc),
        input_location=input_location,
        output_location=output_location,
    )


def _run_action(
    *,
    action: CollectLineageAction,
    context: ActionContext,
    services: ActionServices,
    lineage_location: str,
    spark: SparkSession,
):
    """Execute the action and return *(result, persisted_df)*."""
    result = action.execute(context, services)
    assert result.status == ActionStatus.HEALTHY
    assert result.extras == {"lineage_location": lineage_location}
    assert spark.catalog.tableExists(lineage_location), f"lineage table not created at {lineage_location}"
    return result, spark.read.table(lineage_location)


# ---------------------------------------------------------------------------
# Test 4 — smoke: schema + extras
# ---------------------------------------------------------------------------


@_SKIP_LINEAGE
def test_writes_delta_and_emits_extras(
    spark: SparkSession,
    ws: WorkspaceClient,
    make_seed_table,
    make_derived_table,
    make_lineage_sink,
) -> None:
    """Smoke check: the persisted table has *LINEAGE_TABLE_SCHEMA* and *extras* is populated."""
    source = make_seed_table()
    make_derived_table(select_sql=f"SELECT * FROM {source}")
    lineage_location = make_lineage_sink()

    action = CollectLineageAction(output_config=OutputConfig(location=lineage_location, mode="append"))
    _, persisted = _run_action(
        action=action,
        context=_make_context(source),
        services=_make_services(spark, ws),
        lineage_location=lineage_location,
        spark=spark,
    )
    expected_field_names = {f.name for f in LINEAGE_TABLE_SCHEMA.fields}
    assert set(persisted.columns) == expected_field_names
    for field in LINEAGE_TABLE_SCHEMA.fields:
        assert persisted.schema[field.name].dataType == field.dataType, f"column {field.name} type mismatch"


# ---------------------------------------------------------------------------
# Test 1 — upstream, two levels
# ---------------------------------------------------------------------------


@_SKIP_LINEAGE
def test_upstream_multi_level(
    spark: SparkSession,
    ws: WorkspaceClient,
    make_seed_table,
    make_derived_table,
    make_lineage_sink,
) -> None:
    """gold → silver → {bronze₁, bronze₂}: upstream walk from gold surfaces both depths."""
    bronze1 = make_seed_table(name_prefix="bronze1")
    bronze2 = make_seed_table(name_prefix="bronze2")
    silver = make_derived_table(
        name_prefix="silver",
        select_sql=f"SELECT * FROM {bronze1} UNION ALL SELECT * FROM {bronze2}",
    )
    gold = make_derived_table(name_prefix="gold", select_sql=f"SELECT * FROM {silver}")
    lineage_location = make_lineage_sink()

    action = CollectLineageAction(
        output_config=OutputConfig(location=lineage_location, mode="append"),
        config=LineageActionConfig(
            upstream=LineageSearchConfig(depth=3, lookback_days=30, max_nodes=100),
            downstream=LineageSearchConfig(enabled=False),
            columns=LineageSearchConfig(enabled=False),
            entities=LineageEntitySearchConfig(enabled=False),
        ),
    )
    _, persisted = _run_action(
        action=action,
        context=_make_context(gold),
        services=_make_services(spark, ws),
        lineage_location=lineage_location,
        spark=spark,
    )
    upstream = persisted.where((persisted["edge_type"] == "upstream") & (persisted["source_table"] == gold))
    edges = {(row["depth"], row["target_table"]) for row in upstream.collect()}
    # depth 1 = silver; depth 2 = both bronzes.
    assert (1, silver) in edges
    assert (2, bronze1) in edges
    assert (2, bronze2) in edges


# ---------------------------------------------------------------------------
# Test 2 — downstream, two levels
# ---------------------------------------------------------------------------


@_SKIP_LINEAGE
def test_downstream_multi_level(
    spark: SparkSession,
    ws: WorkspaceClient,
    make_seed_table,
    make_derived_table,
    make_lineage_sink,
) -> None:
    """bronze₁ → silver → gold: downstream walk from bronze₁ surfaces both depths."""
    bronze1 = make_seed_table(name_prefix="bronze1")
    bronze2 = make_seed_table(name_prefix="bronze2")
    silver = make_derived_table(
        name_prefix="silver",
        select_sql=f"SELECT * FROM {bronze1} UNION ALL SELECT * FROM {bronze2}",
    )
    gold = make_derived_table(name_prefix="gold", select_sql=f"SELECT * FROM {silver}")
    lineage_location = make_lineage_sink()

    action = CollectLineageAction(
        output_config=OutputConfig(location=lineage_location, mode="append"),
        config=LineageActionConfig(
            upstream=LineageSearchConfig(enabled=False),
            downstream=LineageSearchConfig(depth=3, lookback_days=30, max_nodes=100),
            columns=LineageSearchConfig(enabled=False),
            entities=LineageEntitySearchConfig(enabled=False),
        ),
    )
    _, persisted = _run_action(
        action=action,
        context=_make_context(bronze1),
        services=_make_services(spark, ws),
        lineage_location=lineage_location,
        spark=spark,
    )
    downstream = persisted.where((persisted["edge_type"] == "downstream") & (persisted["source_table"] == bronze1))
    edges = {(row["depth"], row["target_table"]) for row in downstream.collect()}
    assert (1, silver) in edges
    assert (2, gold) in edges


# ---------------------------------------------------------------------------
# Test 3 — recursive / self-writing pipeline
# ---------------------------------------------------------------------------


@_SKIP_LINEAGE
def test_recursive_job_self_write(
    spark: SparkSession,
    ws: WorkspaceClient,
    make_seed_table,
    make_derived_table,
    make_lineage_sink,
) -> None:
    """Self-writing (t ← t) does not produce cyclic descendants; a → b → a → c must not
    surface *c* from *a* via the cyclic hop (in-CTE full-path guard).
    """
    # Case A: self_table reads and writes itself. Simulate with CREATE OR REPLACE from itself.
    self_table = make_seed_table(name_prefix="cycle")
    spark.sql(f"CREATE OR REPLACE TABLE {self_table} USING DELTA AS SELECT * FROM {self_table}")

    # Case B: build node_a → node_b → node_a → node_c so the recursive CTE, with its per-path
    # visited array, blocks the cycle and never reaches node_c from node_a via node_a→node_b→node_a.
    node_a = make_seed_table(name_prefix="cycA")
    node_b = make_derived_table(name_prefix="cycB", select_sql=f"SELECT * FROM {node_a}")
    spark.sql(f"CREATE OR REPLACE TABLE {node_a} USING DELTA AS SELECT * FROM {node_b}")
    node_c = make_derived_table(name_prefix="cycC", select_sql=f"SELECT * FROM {node_a}")

    lineage_location = make_lineage_sink()
    action = CollectLineageAction(
        output_config=OutputConfig(location=lineage_location, mode="append"),
        config=LineageActionConfig(
            upstream=LineageSearchConfig(enabled=False),
            downstream=LineageSearchConfig(depth=5, lookback_days=30, max_nodes=100),
            columns=LineageSearchConfig(enabled=False),
            entities=LineageEntitySearchConfig(enabled=False),
        ),
    )
    # (1) self-writing self_table: no self-descendant beyond seed.
    _, persisted_self = _run_action(
        action=action,
        context=_make_context(self_table),
        services=_make_services(spark, ws),
        lineage_location=lineage_location,
        spark=spark,
    )
    self_edges = persisted_self.where(
        (persisted_self["edge_type"] == "downstream")
        & (persisted_self["source_table"] == self_table)
        & (persisted_self["target_table"] == self_table)
    ).count()
    assert self_edges == 0, "self-referential lineage must not appear as its own descendant"

    # (2) node_a → node_b → node_a → node_c: node_c reachable from node_a only via the
    # non-cyclic path node_a→node_c, at depth 1.
    _, persisted_ac = _run_action(
        action=action,
        context=_make_context(node_a),
        services=_make_services(spark, ws),
        lineage_location=lineage_location,
        spark=spark,
    )
    edges_from_a = {
        (row["depth"], row["target_table"])
        for row in persisted_ac.where(
            (persisted_ac["edge_type"] == "downstream") & (persisted_ac["source_table"] == node_a)
        ).collect()
    }
    # node_c is reachable from node_a directly (depth 1). It must NOT appear at deeper depth via
    # the cyclic node_a→node_b→node_a→node_c path — that would require re-entering node_a, which
    # the path array blocks.
    reached_c_depths = {depth for depth, target in edges_from_a if target == node_c}
    assert (
        1 in reached_c_depths or not reached_c_depths
    ), "expected node_c reachable from node_a at depth 1 only (or system-table lag hid it entirely)"
    assert all(
        depth <= 1 for depth in reached_c_depths
    ), f"node_c must not appear at depth > 1 via cyclic path; observed depths: {reached_c_depths}"
