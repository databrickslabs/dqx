"""Unit tests for the LLM prompt-size breakdown value types and measurement function."""

import json

import pytest

from databricks.labs.dqx.llm.prompt_breakdown import (
    PromptBreakdown,
    PromptSource,
    measure_schema_prompt,
)


# -- PromptSource / PromptBreakdown ------------------------------------------------------


def test_prompt_source_defaults_leave_optional_fields_none():
    source = PromptSource(kind="user_input", chars=120)
    assert source.name is None
    assert source.depth is None
    assert source.detail is None


def test_prompt_breakdown_invariant_holds_when_sources_sum_to_total():
    breakdown = PromptBreakdown(
        total_chars=330,
        sources=(
            PromptSource(kind="user_input", chars=120),
            PromptSource(kind="available_functions", chars=110),
            PromptSource(kind="source_table", name="main.default.users", chars=100),
        ),
    )
    assert breakdown.total_chars == sum(s.chars for s in breakdown.sources)


def test_prompt_breakdown_invariant_raises_on_mismatch():
    with pytest.raises(ValueError, match="invariant violated"):
        PromptBreakdown(
            total_chars=99,
            sources=(PromptSource(kind="user_input", chars=10),),
        )


def test_prompt_breakdown_render_sorts_by_chars_desc_and_appends_total():
    breakdown = PromptBreakdown(
        total_chars=1000,
        sources=(
            PromptSource(kind="user_input", chars=100),
            PromptSource(kind="source_table", name="main.default.users", chars=700),
            PromptSource(kind="upstream_table", name="main.sales.invoices", depth=2, chars=200),
        ),
    )
    rendered = breakdown.render()
    lines = rendered.splitlines()
    assert lines[0] == "LLM prompt size breakdown:"
    source_table_idx = next(i for i, line in enumerate(lines) if "source_table" in line)
    user_input_idx = next(i for i, line in enumerate(lines) if "user_input" in line)
    upstream_idx = next(i for i, line in enumerate(lines) if "upstream_table" in line)
    assert source_table_idx < upstream_idx < user_input_idx
    assert lines[-1].startswith("TOTAL")
    assert "1,000" in lines[-1]


def test_prompt_breakdown_render_renders_percentages():
    breakdown = PromptBreakdown(
        total_chars=400,
        sources=(
            PromptSource(kind="user_input", chars=100),
            PromptSource(kind="source_table", name="t", chars=300),
        ),
    )
    rendered = breakdown.render()
    assert "25.0%" in rendered
    assert "75.0%" in rendered


def test_prompt_breakdown_merge_concatenates_sources_and_sums_totals():
    left = PromptBreakdown(
        total_chars=200,
        sources=(
            PromptSource(kind="user_input", chars=120),
            PromptSource(kind="available_functions", chars=80),
        ),
    )
    right = PromptBreakdown(
        total_chars=500,
        sources=(
            PromptSource(kind="source_table", name="main.default.users", chars=300),
            PromptSource(kind="upstream_table", name="main.sales.invoices", depth=1, chars=200),
        ),
    )
    merged = left.merge(right)
    assert merged.total_chars == 700
    assert tuple(s.kind for s in merged.sources) == (
        "user_input",
        "available_functions",
        "source_table",
        "upstream_table",
    )
    # Immutability: original breakdowns unchanged.
    assert left.total_chars == 200
    assert right.total_chars == 500


def test_prompt_breakdown_empty_degenerates_cleanly():
    breakdown = PromptBreakdown(total_chars=0, sources=())
    rendered = breakdown.render()
    assert "TOTAL" in rendered


# -- measure_schema_prompt ---------------------------------------------------------------


def test_measure_schema_prompt_baseline_json_emits_single_source_table_entry():
    schema_json = json.dumps({"columns": [{"name": "id", "type": "string"}]})
    breakdown = measure_schema_prompt(schema_json)
    assert breakdown.total_chars == len(schema_json)
    assert len(breakdown.sources) == 1
    assert breakdown.sources[0].kind == "source_table"
    assert breakdown.sources[0].chars == len(schema_json)


def test_measure_schema_prompt_enriched_json_emits_per_upstream_entries():
    payload = {
        "table": "main.default.users",
        "columns": [{"name": "id", "type": "string"}],
        "upstream_tables": [
            {"name": "main.sales.invoices", "depth": 2, "comment": "Invoice fact"},
            {"name": "main.hr.employees", "depth": 1},
        ],
    }
    schema_json = json.dumps(payload)
    breakdown = measure_schema_prompt(schema_json)
    assert breakdown.total_chars == len(schema_json)

    upstreams = [s for s in breakdown.sources if s.kind == "upstream_table"]
    assert {(s.name, s.depth) for s in upstreams} == {
        ("main.sales.invoices", 2),
        ("main.hr.employees", 1),
    }
    seeds = [s for s in breakdown.sources if s.kind == "source_table"]
    assert len(seeds) == 1
    assert seeds[0].name == "main.default.users"


def test_measure_schema_prompt_external_lineage_entries():
    payload = {
        "table": "main.default.users",
        "columns": [],
        "external_lineage": [
            {"source_kind": "external_metadata", "source_name": "sap_sd_invoices"},
            {"source_kind": "file", "source_name": "s3://bucket/data"},
        ],
    }
    schema_json = json.dumps(payload)
    breakdown = measure_schema_prompt(schema_json)
    externals = [s for s in breakdown.sources if s.kind == "external_lineage"]
    assert {s.name for s in externals} == {"sap_sd_invoices", "s3://bucket/data"}


def test_measure_schema_prompt_column_lineage_edges_aggregated():
    payload = {
        "table": "main.default.users",
        "columns": [],
        "column_upstream_lineage": [
            {"predecessor": "p1", "source_table": "t1", "source_column": "c", "target_column": "c", "depth": 1},
            {"predecessor": "p2", "source_table": "t2", "source_column": "c", "target_column": "c", "depth": 1},
        ],
    }
    schema_json = json.dumps(payload)
    breakdown = measure_schema_prompt(schema_json)
    edge_entries = [s for s in breakdown.sources if s.kind == "column_lineage_edges"]
    assert len(edge_entries) == 1
    assert edge_entries[0].detail == {"edge_count": 2}


def test_measure_schema_prompt_total_chars_invariant_across_shapes():
    """For every shape *measure_schema_prompt* produces, total_chars equals len(schema_json)."""
    for payload in (
        {"columns": [{"name": "id"}]},
        {"table": "t", "columns": [], "upstream_tables": [{"name": "u", "depth": 1}]},
        {"table": "t", "columns": [], "external_lineage": [{"source_kind": "table", "source_name": "x.y.z"}]},
        {"table": "t", "columns": [], "column_upstream_lineage": [{"source_table": "t1"}]},
    ):
        schema_json = json.dumps(payload)
        breakdown = measure_schema_prompt(schema_json)
        assert breakdown.total_chars == len(schema_json)


def test_measure_schema_prompt_malformed_json_returns_opaque_single_entry():
    schema_json = "not valid json <<>>"
    breakdown = measure_schema_prompt(schema_json)
    assert breakdown.total_chars == len(schema_json)
    assert len(breakdown.sources) == 1
    assert breakdown.sources[0].kind == "source_table"


def test_measure_schema_prompt_empty_string_returns_zero_entry():
    breakdown = measure_schema_prompt("")
    assert breakdown.total_chars == 0
    assert len(breakdown.sources) == 1


# -- PromptBreakdown.from_prompt_parts --------------------------------------------------


def test_from_prompt_parts_includes_engine_and_schema_sources():
    """The composed breakdown must cover every input *detect_business_rules_with_llm* sends."""
    available_functions_json = json.dumps([{"name": "is_not_null"}])
    schema_json = json.dumps({"table": "cat.sch.tbl", "columns": [{"name": "id", "type": "int"}]})

    breakdown = PromptBreakdown.from_prompt_parts(
        available_functions_json=available_functions_json,
        user_input="describe table",
        schema_info=schema_json,
        stats_json=None,
    )

    kinds = [s.kind for s in breakdown.sources]
    assert "user_input" in kinds
    assert "available_functions" in kinds
    assert "source_table" in kinds
    assert "summary_stats" not in kinds
    # Invariant: total_chars matches sum of source chars.
    assert breakdown.total_chars == sum(s.chars for s in breakdown.sources)


def test_from_prompt_parts_includes_summary_stats_when_supplied():
    stats_json = json.dumps({"columns": {"id": {"count": 10}}})

    breakdown = PromptBreakdown.from_prompt_parts(
        available_functions_json="[]",
        user_input="",
        schema_info=json.dumps({"columns": []}),
        stats_json=stats_json,
    )

    kinds = [s.kind for s in breakdown.sources]
    # user_input is empty → omitted.
    assert "user_input" not in kinds
    assert "available_functions" in kinds
    assert "summary_stats" in kinds


def test_from_prompt_parts_renders_to_table():
    """Rendered table is what *detect_business_rules_with_llm* logs at INFO; must not raise."""
    breakdown = PromptBreakdown.from_prompt_parts(
        available_functions_json="[]",
        user_input="hi",
        schema_info=json.dumps({"columns": []}),
        stats_json=None,
    )
    rendered = breakdown.render()
    assert "LLM prompt size breakdown" in rendered
    assert "user_input" in rendered
    assert "available_functions" in rendered
