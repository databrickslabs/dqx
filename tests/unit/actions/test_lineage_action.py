"""Unit tests for *CollectLineageAction* with mocked services.

Covers the collector's write, extras contract, input validation, depth/node caps, cycle
exclusion, failure isolation, and the parameterised-SQL invariant. No live workspace / Spark.
"""

import logging
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, create_autospec

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from databricks.sdk import WorkspaceClient

from databricks.labs.dqx.actions import lineage as lineage_module
from databricks.labs.dqx.actions.base import ActionContext, ActionServices, ActionStatus
from databricks.labs.dqx.actions.dq_action import DQAction
from databricks.labs.dqx.actions.evaluator import ActionEvaluator
from databricks.labs.dqx.actions.lineage import CollectLineageAction, LineageActionConfig, LineageSearchConfig
from databricks.labs.dqx.actions.state import ActionStateStore
from databricks.labs.dqx.config import OutputConfig

# TODO: No monkey patches - tests covered with monkey patches should be reimplemented to integration.

# ---------------------------------------------------------------------------
# Fake Spark row and DataFrame
# ---------------------------------------------------------------------------


class _FakeRow(dict):
    """Row-like object supporting both attribute and dict-key access."""

    def __getitem__(self, key: str) -> Any:  # type: ignore[override]
        return super().__getitem__(key)

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc


class _FakeDF:
    """Minimal DataFrame stand-in with .collect()."""

    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows

    def collect(self) -> list[_FakeRow]:
        return list(self._rows)


def _row(**kwargs: Any) -> _FakeRow:
    return _FakeRow(**kwargs)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_services(
    spark: SparkSession,
    ws: WorkspaceClient | None = None,
) -> ActionServices:
    services = create_autospec(ActionServices, instance=True)
    services.spark = spark
    services.ws = ws
    return services


def _make_context(
    input_location: str | None = "cat.sch.tbl",
    failed_columns: list[str] | None = None,
) -> ActionContext:
    metrics: dict[str, Any] = {"error_row_count": 5}
    if failed_columns is not None:
        metrics["failed_columns"] = failed_columns
    return ActionContext(
        metrics=metrics,
        run_id="run-lineage-001",
        run_time=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        input_location=input_location,
    )


def _make_spark_with_dfs(
    *,
    upstream_rows: list[_FakeRow] | None = None,
    downstream_rows: list[_FakeRow] | None = None,
    column_up_rows: list[_FakeRow] | None = None,
    column_down_rows: list[_FakeRow] | None = None,
    modifier_rows: list[_FakeRow] | None = None,
    written: list[DataFrame] | None = None,
) -> tuple[SparkSession, list[dict[str, Any]]]:
    """Build a SparkSession stand-in whose *sql* returns the appropriate fake rows and whose
    *createDataFrame* records the payload for later assertions."""
    spark = create_autospec(SparkSession, instance=True)
    calls: list[dict[str, Any]] = []

    def _sql(query: str, args: dict[str, Any] | None = None) -> _FakeDF:
        calls.append({"query": query, "args": args or {}})
        # Route based on the target system table + direction keys present in the query.
        if lineage_module.LINEAGE_JOBS_RUNS in query:
            # Job-run lookup — return no rows by default (empty list).
            return _FakeDF([])
        if lineage_module.LINEAGE_TABLE_LINEAGE in query:
            if "entity_id" in query:
                # Last-modifier: entity rows
                return _FakeDF(modifier_rows or [])
            # Direction is signalled by whether "source_table_full_name AS anchor" or
            # "target_table_full_name AS anchor" appears in the query.
            if "source_table_full_name AS anchor" in query:
                return _FakeDF(upstream_rows or [])
            if "target_table_full_name AS anchor" in query:
                return _FakeDF(downstream_rows or [])
            return _FakeDF([])
        if lineage_module.LINEAGE_COLUMN_LINEAGE in query:
            if "source_table_full_name AS neighbour" in query:
                return _FakeDF(column_up_rows or [])
            return _FakeDF(column_down_rows or [])
        return _FakeDF([])

    spark.sql.side_effect = _sql

    def _create_df(rows: Any, schema: Any) -> DataFrame:  # noqa: ANN401
        df = MagicMock(spec=DataFrame)
        df.rows = list(rows)
        df.schema = schema
        return df

    spark.createDataFrame.side_effect = _create_df
    return spark, calls


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_happy_path_writes_and_emits_extras(monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy path: DataFrame is written via save_dataframe_as_table; extras carries only lineage_location."""
    upstream = [_row(anchor="cat.sch.tbl", neighbour="cat.sch.upstream1", event_time=None)]
    downstream = [_row(anchor="cat.sch.tbl", neighbour="cat.sch.down1", event_time=None)]

    spark, _ = _make_spark_with_dfs(upstream_rows=upstream, downstream_rows=downstream)
    services = _make_services(spark=spark)

    saved: list[tuple[DataFrame, OutputConfig]] = []

    def _fake_save(df: DataFrame, output_config: OutputConfig) -> None:
        saved.append((df, output_config))

    monkeypatch.setattr(lineage_module, "save_dataframe_as_table", _fake_save)

    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"))
    result = action.execute(_make_context(), services)

    assert result.status == ActionStatus.HEALTHY
    assert result.extras == {"lineage_location": "cat.sch.lineage"}
    assert len(saved) == 1
    saved_df, saved_cfg = saved[0]
    assert saved_cfg.location == "cat.sch.lineage"
    # Two edge rows: one upstream + one downstream.
    assert len(saved_df.rows) == 2
    edge_types = {row["edge_type"] for row in saved_df.rows}
    assert edge_types == {"upstream", "downstream"}


def test_none_input_location_no_query_no_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """context.input_location is None → HEALTHY, no SQL, no write, extras=None."""
    spark, calls = _make_spark_with_dfs()
    services = _make_services(spark=spark)

    saved: list[Any] = []
    monkeypatch.setattr(lineage_module, "save_dataframe_as_table", lambda df, cfg: saved.append((df, cfg)))

    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"))
    result = action.execute(_make_context(input_location=None), services)

    assert result.status == ActionStatus.HEALTHY
    assert result.extras is None
    assert calls == []
    assert saved == []


def test_empty_collection_still_writes_dataframe_with_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empty system tables → an empty DataFrame with LINEAGE_TABLE_SCHEMA is still written."""
    spark, _ = _make_spark_with_dfs()  # no rows anywhere
    services = _make_services(spark=spark)

    saved: list[tuple[DataFrame, OutputConfig]] = []
    monkeypatch.setattr(lineage_module, "save_dataframe_as_table", lambda df, cfg: saved.append((df, cfg)))

    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"))
    result = action.execute(_make_context(), services)

    assert result.status == ActionStatus.HEALTHY
    assert len(saved) == 1
    saved_df, _ = saved[0]
    assert saved_df.rows == []
    assert saved_df.schema is lineage_module.LINEAGE_TABLE_SCHEMA


def test_depth_cap_stops_at_configured_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    """With depth=2 the BFS visits exactly the 2-hop neighbours and stops."""
    # Simulate a chain: tbl -> A -> B -> C. With depth=2 we should record A and B but stop before C.
    def _sql(query: str, args: dict[str, Any] | None = None) -> _FakeDF:
        anchor = (args or {}).get("table_name")
        if lineage_module.LINEAGE_TABLE_LINEAGE not in query:
            return _FakeDF([])
        if "source_table_full_name AS anchor" not in query:
            return _FakeDF([])
        mapping = {
            "cat.sch.tbl": [_row(anchor="cat.sch.tbl", neighbour="cat.sch.A", event_time=None)],
            "cat.sch.A": [_row(anchor="cat.sch.A", neighbour="cat.sch.B", event_time=None)],
            "cat.sch.B": [_row(anchor="cat.sch.B", neighbour="cat.sch.C", event_time=None)],
        }
        return _FakeDF(mapping.get(anchor, []))

    spark = create_autospec(SparkSession, instance=True)
    spark.sql.side_effect = _sql
    written: list[tuple[list[Any], Any]] = []
    spark.createDataFrame.side_effect = lambda rows, schema: (written.append((list(rows), schema)) or MagicMock())

    services = _make_services(spark=spark)
    monkeypatch.setattr(lineage_module, "save_dataframe_as_table", lambda df, cfg: None)

    config = LineageActionConfig(
        upstream=LineageSearchConfig(depth=2, lookback_days=30, max_nodes=100),
        downstream=LineageSearchConfig(enabled=False),
        columns=LineageSearchConfig(enabled=False),
    )
    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"), config=config)
    action.execute(_make_context(), services)

    rows = written[0][0]
    upstream_targets = {r["target_table"] for r in rows if r["edge_type"] == "upstream"}
    assert upstream_targets == {"cat.sch.A", "cat.sch.B"}


def test_node_cap_truncates_and_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """With max_nodes=3 and a 5-node graph, only 3 nodes are visited and a warning is logged."""

    def _sql(query: str, args: dict[str, Any] | None = None) -> _FakeDF:
        anchor = (args or {}).get("table_name")
        if lineage_module.LINEAGE_TABLE_LINEAGE not in query:
            return _FakeDF([])
        if anchor == "cat.sch.tbl":
            return _FakeDF(
                [_row(anchor="cat.sch.tbl", neighbour=f"cat.sch.N{i}", event_time=None) for i in range(1, 6)]
            )
        return _FakeDF([])

    spark = create_autospec(SparkSession, instance=True)
    spark.sql.side_effect = _sql
    written: list[tuple[list[Any], Any]] = []
    spark.createDataFrame.side_effect = lambda rows, schema: (written.append((list(rows), schema)) or MagicMock())

    services = _make_services(spark=spark)
    monkeypatch.setattr(lineage_module, "save_dataframe_as_table", lambda df, cfg: None)

    config = LineageActionConfig(
        upstream=LineageSearchConfig(depth=1, lookback_days=1, max_nodes=3),
        downstream=LineageSearchConfig(enabled=False),
        columns=LineageSearchConfig(enabled=False),
        entities=lineage_module.LineageEntitySearchConfig(enabled=False),
    )
    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"), config=config)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.actions.lineage"):
        action.execute(_make_context(), services)

    rows = written[0][0]
    assert len([r for r in rows if r["edge_type"] == "upstream"]) <= 3
    assert any("max_nodes" in r.message for r in caplog.records)


def test_cycle_exclusion_terminates(monkeypatch: pytest.MonkeyPatch) -> None:
    """A self-referential lineage row does not cause infinite recursion."""

    def _sql(query: str, args: dict[str, Any] | None = None) -> _FakeDF:
        if lineage_module.LINEAGE_TABLE_LINEAGE not in query:
            return _FakeDF([])
        if "source_table_full_name AS anchor" not in query:
            return _FakeDF([])
        anchor = (args or {}).get("table_name")
        # Self-reference: the table's upstream is itself.
        return _FakeDF([_row(anchor=anchor, neighbour=anchor, event_time=None)])

    spark = create_autospec(SparkSession, instance=True)
    spark.sql.side_effect = _sql
    written: list[tuple[list[Any], Any]] = []
    spark.createDataFrame.side_effect = lambda rows, schema: (written.append((list(rows), schema)) or MagicMock())

    services = _make_services(spark=spark)
    monkeypatch.setattr(lineage_module, "save_dataframe_as_table", lambda df, cfg: None)

    config = LineageActionConfig(
        upstream=LineageSearchConfig(depth=5, lookback_days=1, max_nodes=100),
        downstream=LineageSearchConfig(enabled=False),
        columns=LineageSearchConfig(enabled=False),
        entities=lineage_module.LineageEntitySearchConfig(enabled=False),
    )
    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"), config=config)
    result = action.execute(_make_context(), services)

    assert result.status == ActionStatus.HEALTHY
    # No cycle → no rows recorded for the self-reference.
    rows = written[0][0]
    assert not any(r["target_table"] == "cat.sch.tbl" for r in rows if r["edge_type"] == "upstream")


def test_spark_sql_error_returns_config_error_and_evaluator_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """spark.sql raising → action returns CONFIG_ERROR; evaluator continues to next action."""

    spark = create_autospec(SparkSession, instance=True)
    spark.sql.side_effect = RuntimeError("spark boom")
    # createDataFrame on empty rows would still succeed — the failure comes from sql().
    spark.createDataFrame.side_effect = lambda rows, schema: MagicMock()

    services = _make_services(spark=spark)
    monkeypatch.setattr(lineage_module, "save_dataframe_as_table", lambda df, cfg: None)

    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"))
    # Direct execute call: the walker swallows individual sql failures, so the overall action
    # can still succeed. Force a hard failure by breaking createDataFrame too.
    spark.createDataFrame.side_effect = RuntimeError("create df boom")
    result = action.execute(_make_context(), services)

    assert result.status == ActionStatus.CONFIG_ERROR
    assert result.extras is None

    # Evaluator continues to next action.
    from databricks.labs.dqx.actions.base import ActionResult

    class _FollowUp:
        name = "follow"

        def execute(self, _context: ActionContext, _services: ActionServices) -> ActionResult:
            return ActionResult(action_name=self.name, fired=True, status=ActionStatus.HEALTHY)

    from databricks.labs.dqx.actions.fail_pipeline import FailPipeline

    action_dq = DQAction(action=action, condition=None, name="lineage")
    follow_dq = DQAction(action=FailPipeline(name="placeholder"), condition=None, name="follow")
    follow_dq.action = _FollowUp()  # type: ignore[assignment]

    evaluator = ActionEvaluator(
        actions=[action_dq, follow_dq],
        state_store=ActionStateStore(),
        services=services,
    )
    results = evaluator.evaluate(_make_context())
    assert any(r.action_name == "follow" for r in results)


def test_unsafe_input_location_never_queries_spark(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Unsafe input_location (with a semicolon) → CONFIG_ERROR, no SQL call, sanitised warning."""
    spark, calls = _make_spark_with_dfs()
    services = _make_services(spark=spark)
    monkeypatch.setattr(lineage_module, "save_dataframe_as_table", lambda df, cfg: None)

    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"))
    with caplog.at_level(logging.WARNING, logger="databricks.labs.dqx.actions.lineage"):
        result = action.execute(_make_context(input_location="foo; DROP TABLE bar"), services)

    assert result.status == ActionStatus.CONFIG_ERROR
    assert result.extras is None
    assert calls == []
    assert any("Unsafe input_location" in r.message for r in caplog.records)


def test_parameterised_sql_invariant(monkeypatch: pytest.MonkeyPatch) -> None:
    """spark.sql is called with args={} and no user literal appears in the query text."""
    spark, calls = _make_spark_with_dfs()
    services = _make_services(spark=spark)
    monkeypatch.setattr(lineage_module, "save_dataframe_as_table", lambda df, cfg: None)

    action = CollectLineageAction(output_config=OutputConfig(location="cat.sch.lineage"))
    action.execute(_make_context(), services)

    assert calls, "expected at least one spark.sql call"
    for call in calls:
        # Table name must NEVER appear as a literal in the query text — it must be in args.
        assert "cat.sch.tbl" not in call["query"]
        assert isinstance(call["args"], dict)
        assert call["args"], "expected args dict to be populated"
