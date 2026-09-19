"""Integration test suite for *CollectLineageAction*.

Four scenarios exercise the recursive-CTE walker + join-based column-lineage pipeline:

1. *test_upstream_multi_level* — a bronze₁ + bronze₂ ∪→ silver → gold graph; upstream walk
   from *gold* must surface both *silver* (depth 1) and the two bronze tables (depth 2).
2. *test_downstream_multi_level* — same graph; downstream walk from *bronze₁* must surface
   *silver* (depth 1) and *gold* (depth 2).
3. *test_recursive_job_self_write* — a self-writing table must not produce cyclic descendants,
   and a synthesised *a → b → a → c* graph must not surface *c* from *a* via the cyclic hop
   (in-CTE full-path cycle guard).
4. *test_writes_delta_and_emits_extras* — smoke check for the persisted schema, *extras*,
   and the table / column comments attached on write.

============================================================================================
Intentional stubbing of the `system.access.*` tables
============================================================================================

The lineage action reads from two Databricks system tables:

  * *system.access.table_lineage*
  * *system.access.column_lineage*

Those views are **populated asynchronously by the platform** — Databricks documents an ingest
delay of up to several hours after the underlying source event. Testing against the live tables
therefore forces a choice between multi-hour test timeouts, sporadic false negatives, or an
opt-in flag that leaves the suite silently disabled in CI. None of those options are acceptable
for a test that's meant to run in the default `make integration` invocation.

Instead, this suite creates two Delta tables in a test schema whose columns match the fields
the action reads from the real system tables, then monkey-patches the module-level
``LINEAGE_*`` constants in *databricks.labs.dqx.actions.lineage* so the action queries our
stubs. The code path under test (recursive CTE, DataFrame joins) is identical — only the source
tables differ. Every test is deterministic and runs enabled-by-default without any opt-in flag.
"""

from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

from databricks.sdk import WorkspaceClient
from databricks.labs.pytester.fixtures.baseline import factory

from databricks.labs.dqx.actions import lineage as lineage_module
from databricks.labs.dqx.actions.base import ActionContext, ActionServices, ActionStatus
from databricks.labs.dqx.actions.delivery import WebhookClient
from databricks.labs.dqx.actions.lineage import (
    CollectLineageAction,
    LINEAGE_TABLE_COMMENT,
    LINEAGE_TABLE_SCHEMA,
    LineageActionConfig,
    LineageSearchConfig,
)
from databricks.labs.dqx.actions.secrets import SecretResolver
from databricks.labs.dqx.config import OutputConfig
from databricks.labs.dqx.schema.dq_result_schema import dq_result_item_schema


@dataclass(frozen=True)
class _StubLineageLocations:
    """Fully-qualified names of the two Delta tables that stand in for the system tables."""

    table_lineage: str
    column_lineage: str


@pytest.fixture
def stub_system_tables(spark: SparkSession, make_schema) -> _StubLineageLocations:
    """Create empty Delta tables that mirror the columns the lineage action reads.

    Columns match *system.access.table_lineage* and *system.access.column_lineage* respectively —
    only the fields the action actually reads are declared; other real-table columns are omitted.

    Cleanup piggybacks on *make_schema* (DROP SCHEMA CASCADE) so no explicit teardown is needed.
    """
    schema = make_schema()

    table_lineage = f"{schema.full_name}.stub_table_lineage"
    column_lineage = f"{schema.full_name}.stub_column_lineage"

    spark.sql(
        f"CREATE TABLE {table_lineage} ("
        "  source_table_full_name STRING,"
        "  target_table_full_name STRING,"
        "  entity_type STRING,"
        "  entity_id STRING,"
        "  event_time TIMESTAMP"
        ") USING DELTA"
    )
    spark.sql(
        f"CREATE TABLE {column_lineage} ("
        "  source_table_full_name STRING,"
        "  target_table_full_name STRING,"
        "  source_column STRING,"
        "  target_column STRING,"
        "  event_time TIMESTAMP"
        ") USING DELTA"
    )

    return _StubLineageLocations(
        table_lineage=table_lineage,
        column_lineage=column_lineage,
    )


@pytest.fixture
def patched_lineage_constants(
    monkeypatch: pytest.MonkeyPatch, stub_system_tables: _StubLineageLocations
) -> _StubLineageLocations:
    """Redirect module-level ``LINEAGE_*`` constants to the stub tables for this test.

    Module-level constants are the accepted seam per project guidelines (CLAUDE.md #6): they are
    the *only* place the source tables enter the code, so patching them is enough to redirect
    every read (recursive CTE upstream/downstream walks and column-lineage join) without touching
    internal implementation.
    """
    monkeypatch.setattr(lineage_module, "LINEAGE_TABLE_LINEAGE", stub_system_tables.table_lineage)
    monkeypatch.setattr(lineage_module, "LINEAGE_COLUMN_LINEAGE", stub_system_tables.column_lineage)
    return stub_system_tables


def _seed_table_lineage(
    spark: SparkSession,
    table_lineage: str,
    edges: list[tuple[str, str]],
) -> None:
    """Insert (source, target) edges with *event_time = current_timestamp()* — always within window."""
    if not edges:
        return
    values = ", ".join(f"('{s}', '{t}', NULL, NULL, current_timestamp())" for s, t in edges)
    spark.sql(
        f"INSERT INTO {table_lineage} "
        f"(source_table_full_name, target_table_full_name, entity_type, entity_id, event_time) "
        f"VALUES {values}"
    )


@pytest.fixture
def make_lineage_target(spark: SparkSession, make_schema) -> Generator[Callable[..., str], None, None]:
    """Provision real Delta tables at ``{schema}.<label>`` so *DESCRIBE HISTORY* succeeds.

    The lineage action resolves *target_delta_version* via ``DESCRIBE HISTORY <ident> LIMIT 1``,
    which requires the target to exist as a Delta table (a bare row in *stub_table_lineage* is
    not enough — that only carries the *name* used in the edge, not a real table). All tables
    share one *make_schema()* schema; cleanup is factory-managed and CASCADEd by *make_schema*.
    """
    schema = make_schema()

    def create(*, label: str) -> str:
        name = f"{schema.full_name}.{label}"
        spark.sql(f"CREATE TABLE {name} (id INT) USING DELTA")
        return name

    def delete(name: str) -> None:
        spark.sql(f"DROP TABLE IF EXISTS {name}")

    yield from factory("lineage_target", create, delete)


@pytest.fixture
def make_lineage_sink(spark: SparkSession, make_schema, make_random) -> Generator[Callable[..., str], None, None]:
    """Provision a UC location for the lineage sink; DROP TABLE IF EXISTS on teardown."""
    schema = make_schema()

    def create(**_kwargs: Any) -> str:
        return f"{schema.full_name}.dqx_lineage_{make_random(6).lower()}"

    def delete(name: str) -> None:
        spark.sql(f"DROP TABLE IF EXISTS {name}")

    yield from factory("lineage_sink", create, delete)


# DQX output-table shape used by the tests: two payload columns (*id*, *value*) plus the two
# DQX-appended result-array columns. The result-struct layout is imported verbatim from the
# shared *dq_result_item_schema* so tests always agree with production. Real output tables also
# carry the source-row columns from the input; the shape here is the smallest superset that
# exercises *extract_failed_columns*.
_OUTPUT_TABLE_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=True),
        StructField("value", StringType(), nullable=True),
        StructField("_errors", ArrayType(dq_result_item_schema), nullable=True),
        StructField("_warnings", ArrayType(dq_result_item_schema), nullable=True),
    ]
)


def _issue_row(*, name: str, message: str, columns: list[str], function: str) -> dict[str, Any]:
    """Build one *_errors*/*_warnings* struct row for use in *createDataFrame*.

    Fields match *dq_result_item_schema*; the *skipped* flag is set to *False* to reflect a
    genuine failure (an entry with *skipped=True* would represent a check that ran but did not
    produce an issue).
    """
    return {
        "name": name,
        "message": message,
        "columns": columns,
        "filter": None,
        "function": function,
        "run_time": datetime.now(timezone.utc),
        "run_id": "integration-lineage-run",
        "user_metadata": None,
        "rule_fingerprint": "fp1",
        "rule_set_fingerprint": "rsfp1",
        "skipped": False,
    }


@pytest.fixture
def make_output_table(spark: SparkSession, make_schema, make_random) -> Generator[Callable[..., str], None, None]:
    """Factory: create a DQX-shaped output table with one row carrying a populated *_errors* entry.

    Matches the reference schema at
    https://databrickslabs.github.io/dqx/docs/reference/table_schemas/#table-relationships —
    replicates the input columns and appends the *_errors* and *_warnings* ARRAY<STRUCT> columns.
    Cleanup is factory-managed.
    """
    schema = make_schema()

    def create(*, failed_column: str = "value", name_prefix: str = "out") -> str:
        table_name = f"{schema.full_name}.{name_prefix}_{make_random(6).lower()}"
        rows = [
            {
                "id": 1,
                "value": "a",
                "_errors": [
                    _issue_row(
                        name="test_check",
                        message=f"'{failed_column}' failed the test check",
                        columns=[failed_column],
                        function="is_not_null",
                    )
                ],
                "_warnings": [],
            }
        ]
        spark.createDataFrame(rows, _OUTPUT_TABLE_SCHEMA).write.mode("overwrite").format("delta").saveAsTable(
            table_name
        )
        return table_name

    def delete(name: str) -> None:
        spark.sql(f"DROP TABLE IF EXISTS {name}")

    yield from factory("output_table", create, delete)


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


def _table_only_action(lineage_location: str, *, downstream: bool = False, depth: int = 3) -> CollectLineageAction:
    """Configure the action with only one directional walk enabled; column phase off."""
    return CollectLineageAction(
        output_config=OutputConfig(location=lineage_location, mode="append"),
        config=LineageActionConfig(
            upstream=LineageSearchConfig(enabled=not downstream, depth=depth, lookback_days=30, max_nodes=100),
            downstream=LineageSearchConfig(enabled=downstream, depth=depth, lookback_days=30, max_nodes=100),
            columns=LineageSearchConfig(enabled=False),
        ),
    )


def test_writes_delta_and_emits_extras(
    spark: SparkSession,
    ws: WorkspaceClient,
    patched_lineage_constants: _StubLineageLocations,
    make_lineage_sink,
    make_output_table,
) -> None:
    """Smoke check: the persisted table has *LINEAGE_TABLE_SCHEMA*, *extras* is populated, and
    the table carries the *LINEAGE_TABLE_COMMENT* attached on write.

    The output table carries one row with a populated *_errors* array so
    *extract_failed_columns* has a real DataFrame to explode — this exercises the
    column-lineage branch alongside upstream/downstream.
    """
    source = "cat.sch.stub_source"
    derived = "cat.sch.stub_derived"
    _seed_table_lineage(spark, patched_lineage_constants.table_lineage, [(source, derived)])
    output_location = make_output_table(failed_column="value")
    lineage_location = make_lineage_sink()

    action = CollectLineageAction(output_config=OutputConfig(location=lineage_location, mode="append"))
    _, persisted = _run_action(
        action=action,
        context=_make_context(source, output_location=output_location),
        services=_make_services(spark, ws),
        lineage_location=lineage_location,
        spark=spark,
    )
    expected_field_names = {f.name for f in LINEAGE_TABLE_SCHEMA.fields}
    assert set(persisted.columns) == expected_field_names
    for field in LINEAGE_TABLE_SCHEMA.fields:
        assert persisted.schema[field.name].dataType == field.dataType, f"column {field.name} type mismatch"

    # Table-level comment applied by CollectLineageAction after save.
    table_description = spark.catalog.getTable(lineage_location).description
    assert table_description == LINEAGE_TABLE_COMMENT, table_description

    # Every declared column comment is persisted on the written table (Delta preserves the
    # StructField metadata as column comments).
    described_columns = {
        row["col_name"]: row["comment"] for row in spark.sql(f"DESCRIBE TABLE {lineage_location}").collect()
    }
    for field in LINEAGE_TABLE_SCHEMA.fields:
        expected_comment = field.metadata.get("comment")
        assert described_columns.get(field.name) == expected_comment, (
            f"column {field.name} comment mismatch: expected {expected_comment!r}, "
            f"got {described_columns.get(field.name)!r}"
        )


def test_upstream_multi_level(
    spark: SparkSession,
    ws: WorkspaceClient,
    patched_lineage_constants: _StubLineageLocations,
    make_lineage_sink,
    make_lineage_target,
    make_output_table,
) -> None:
    """gold → silver → {bronze₁, bronze₂}: upstream walk from gold surfaces both depths and each
    target row carries the current Delta version resolved from *DESCRIBE HISTORY*.
    """
    bronze1 = make_lineage_target(label="bronze1")
    bronze2 = make_lineage_target(label="bronze2")
    silver = make_lineage_target(label="silver")
    gold = make_lineage_target(label="gold")

    _seed_table_lineage(
        spark,
        patched_lineage_constants.table_lineage,
        [(bronze1, silver), (bronze2, silver), (silver, gold)],
    )
    output_location = make_output_table(failed_column="value")

    lineage_location = make_lineage_sink()
    _, persisted = _run_action(
        action=_table_only_action(lineage_location, downstream=False),
        context=_make_context(gold, output_location=output_location),
        services=_make_services(spark, ws),
        lineage_location=lineage_location,
        spark=spark,
    )
    upstream = persisted.where((persisted["edge_type"] == "upstream") & (persisted["source_table"] == gold))
    edges = {(row["depth"], row["target_table"]) for row in upstream.collect()}

    assert (1, silver) in edges, edges
    assert (2, bronze1) in edges, edges
    assert (2, bronze2) in edges, edges

    versions = {
        row["target_table"]: row["target_delta_version"]
        for row in upstream.select("target_table", "target_delta_version").collect()
    }
    assert set(versions) == {silver, bronze1, bronze2}, versions
    for table, version in versions.items():
        assert version is not None, f"target_delta_version not populated for {table}: {versions}"
        assert version >= 0, f"unexpected version for {table}: {version}"


def test_downstream_multi_level(
    spark: SparkSession,
    ws: WorkspaceClient,
    patched_lineage_constants: _StubLineageLocations,
    make_lineage_sink,
) -> None:
    """bronze₁ → silver → gold: downstream walk from bronze₁ surfaces both depths."""
    bronze1 = "cat.sch.bronze1"
    bronze2 = "cat.sch.bronze2"
    silver = "cat.sch.silver"
    gold = "cat.sch.gold"
    _seed_table_lineage(
        spark,
        patched_lineage_constants.table_lineage,
        [(bronze1, silver), (bronze2, silver), (silver, gold)],
    )

    lineage_location = make_lineage_sink()
    _, persisted = _run_action(
        action=_table_only_action(lineage_location, downstream=True),
        context=_make_context(bronze1),
        services=_make_services(spark, ws),
        lineage_location=lineage_location,
        spark=spark,
    )
    downstream = persisted.where((persisted["edge_type"] == "downstream") & (persisted["source_table"] == bronze1))
    edges = {(row["depth"], row["target_table"]) for row in downstream.collect()}
    assert (1, silver) in edges
    assert (2, gold) in edges


def test_recursive_job_self_write(
    spark: SparkSession,
    ws: WorkspaceClient,
    patched_lineage_constants: _StubLineageLocations,
    make_lineage_sink,
) -> None:
    """Self-writing (self_table ← self_table) does not produce cyclic descendants; and in a
    synthesised node_a → node_b → node_a → node_c graph, node_c must not surface from node_a
    via the cyclic hop — the in-CTE array-path guard rejects re-visiting a table already on
    the current path.
    """
    self_table = "cat.sch.self_table"
    node_a = "cat.sch.node_a"
    node_b = "cat.sch.node_b"
    node_c = "cat.sch.node_c"
    _seed_table_lineage(
        spark,
        patched_lineage_constants.table_lineage,
        [
            (self_table, self_table),  # self-write
            (node_a, node_b),
            (node_b, node_a),  # cycle back to a
            (node_a, node_c),  # direct edge a → c
        ],
    )

    lineage_location = make_lineage_sink()
    action = _table_only_action(lineage_location, downstream=True, depth=5)

    # (1) self-writing self_table: no self-descendant must appear (seed row filtered by
    # `key_other <> :table_name`).
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

    # (2) node_a → node_b → node_a → node_c: node_c reachable from node_a only via the direct
    # non-cyclic edge, i.e. at depth 1.
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
    # Depth-1 neighbours: node_b (a→b) and node_c (a→c). node_a itself must never appear as its
    # own descendant.
    assert (1, node_b) in edges_from_a
    assert (1, node_c) in edges_from_a
    assert not any(
        target == node_a for _, target in edges_from_a
    ), "node_a must not appear as its own descendant via the cycle b→a"
    # node_c must not be re-reached via the cyclic a→b→a→c path at deeper depth — the array-path
    # guard rejects the b→a hop, so no descendant path reintroduces node_c.
    reached_c_depths = {depth for depth, target in edges_from_a if target == node_c}
    assert reached_c_depths == {1}, f"node_c must only appear at depth 1; observed depths: {reached_c_depths}"
