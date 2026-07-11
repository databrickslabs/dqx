"""Unit tests for ``backend.services.genie_space_service``.

Pins the serialized_space v2 JSON contract (the REST payload shapes the
Genie API expects), the permission boundary (the only row-level object is
the entitlement-gated ``v_dq_failing_rows`` view — never the raw
quarantine table), the config-hash idempotency semantics including
retry-on-PATCH-failure, and the status setting transitions — all against
a mocked ``ws.api_client.do`` and an in-memory settings fake (no live
workspace).
"""

from __future__ import annotations

import json
import re
from unittest.mock import MagicMock, create_autospec

import pytest

from databricks_labs_dqx_app.backend.services import genie_space_service as gs
from databricks_labs_dqx_app.backend.services.app_settings_service import AppSettingsService

CATALOG = "main"
SCHEMA = "dq"

MV_FQN = f"{CATALOG}.{SCHEMA}.mv_dq_scores"
RESULTS_FQN = f"{CATALOG}.{SCHEMA}.v_dq_check_results"
ATTRIBUTION_FQN = f"{CATALOG}.{SCHEMA}.v_dq_check_attribution"
FAILING_FQN = f"{CATALOG}.{SCHEMA}.v_dq_failing_rows"

_HEX32 = re.compile(r"^[0-9a-f]{32}$")


def build() -> dict:
    return gs.build_serialized_space(CATALOG, SCHEMA)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def settings() -> MagicMock:
    """Dict-backed AppSettingsService fake (spec-bound so misuse fails loudly)."""
    store: dict[str, str] = {}
    mock = create_autospec(AppSettingsService, instance=True)
    mock.get_setting.side_effect = store.get
    mock.save_setting.side_effect = lambda key, value, **kw: store.__setitem__(key, value)
    mock.store = store
    return mock


@pytest.fixture
def ws() -> MagicMock:
    """WorkspaceClient mock whose raw REST surface (api_client.do) is scriptable."""
    ws = MagicMock(name="WorkspaceClient")
    ws.api_client.do.return_value = {"spaces": []}
    return ws


def do_calls(ws: MagicMock) -> list[tuple]:
    return ws.api_client.do.call_args_list


# ---------------------------------------------------------------------------
# _lines
# ---------------------------------------------------------------------------


def test_lines_round_trips_the_sql() -> None:
    sql = "SELECT 1\nFROM t\nWHERE x = 1"
    lines = gs._lines(sql)
    assert "".join(lines) == sql
    assert all(ln.endswith("\n") for ln in lines[:-1])
    assert not lines[-1].endswith("\n")


# ---------------------------------------------------------------------------
# serialized_space v2 contract
# ---------------------------------------------------------------------------


def test_serialized_space_top_level_shape() -> None:
    space = build()
    assert space["version"] == 2
    assert set(space.keys()) == {"version", "config", "data_sources", "instructions", "benchmarks"}
    assert set(space["instructions"].keys()) == {
        "text_instructions",
        "example_question_sqls",
        "sql_snippets",
        "join_specs",
        "sql_functions",
    }
    assert space["instructions"]["join_specs"] == []
    assert space["instructions"]["sql_functions"] == []


def test_data_sources_are_exactly_the_four_score_objects() -> None:
    space = build()
    tables = space["data_sources"]["tables"]
    assert [t["identifier"] for t in tables] == sorted([ATTRIBUTION_FQN, RESULTS_FQN, FAILING_FQN])
    mvs = space["data_sources"]["metric_views"]
    assert [m["identifier"] for m in mvs] == [MV_FQN]
    for src in [*tables, *mvs]:
        assert src["description"] and all(isinstance(d, str) for d in src["description"])


def test_space_never_references_ungated_row_level_objects() -> None:
    """The only row-level object is the entitlement-gated view — the raw
    quarantine table (and dqlake's row objects) must never appear."""
    dump = json.dumps(build())
    for forbidden in ("dq_quarantine_records", "row_values", "failed_rows_latest", "record_key"):
        assert forbidden not in dump
    assert FAILING_FQN in dump


def test_failing_rows_source_description_grounded_in_real_columns() -> None:
    space = build()
    by_id = {t["identifier"]: t for t in space["data_sources"]["tables"]}
    desc = "".join(by_id[FAILING_FQN]["description"])
    # The REAL view columns (entitlement_service.failing_rows_view_ddl),
    # requesting_user excluded there by design.
    for col in ("quarantine_id", "run_id", "source_table_fqn", "row_data", "errors", "warnings", "created_at"):
        assert col in desc
    assert "requesting_user" not in desc
    assert "VARIANT" in desc
    assert "to_json(row_data)" in desc
    # Entitlement honesty: empty may mean unverified, not clean.
    assert "DQX Studio" in desc


def test_sample_questions_restore_the_row_level_ones() -> None:
    space = build()
    entries = space["config"]["sample_questions"]
    assert sorted(q["question"][0] for q in entries) == sorted(gs.SAMPLE_QUESTIONS)
    assert [e["id"] for e in entries] == sorted(e["id"] for e in entries)
    assert all(_HEX32.fullmatch(e["id"]) for e in entries)
    # dqlake's row-level chips are restored over the gated view (P4.2).
    assert "Show me the rows that failed." in gs.SAMPLE_QUESTIONS
    assert "What are the failing rows with the most rules failed?" in gs.SAMPLE_QUESTIONS
    # The one deliberate addition.
    assert "How many draft runs happened recently?" in gs.SAMPLE_QUESTIONS


def test_text_instructions_single_entry_with_newline_terminated_paragraphs() -> None:
    space = build()
    text_instructions = space["instructions"]["text_instructions"]
    assert len(text_instructions) == 1
    content = text_instructions[0]["content"]
    assert content == list(gs.TEXT_INSTRUCTIONS)
    assert len(content) == 10
    assert all(p.endswith("\n") for p in content)
    joined = "".join(content)
    assert "MEASURE()" in joined
    assert "published" in joined and "draft" in joined
    assert "(Data product: <name> — tables: ...)" in joined


def test_text_instructions_row_source_and_draft_scoping() -> None:
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    # Row source: whole record per row, wrapper columns stay internal,
    # published-run scoping, and honesty about the entitlement gate.
    assert "v_dq_failing_rows" in joined
    assert "to_json(row_data)" in joined
    assert "quarantine_id" in joined
    assert "one row per failing record" in joined
    assert "opened that table in DQX Studio" in joined
    # Strengthened draft scoping (user requirement).
    assert "Never include draft-run data" in joined
    assert "explicitly asks for drafts" in joined


def test_every_sample_question_has_a_curated_sql() -> None:
    curated = {e["question"][0] for e in gs._curated_sqls(CATALOG, SCHEMA)}
    assert curated == set(gs.SAMPLE_QUESTIONS)


def test_curated_sqls_are_grounded_on_our_objects_and_run_mode() -> None:
    for entry in gs._curated_sqls(CATALOG, SCHEMA):
        sql = "".join(entry["sql"])
        question = entry["question"][0]
        # Only our objects, catalog/schema backtick-quoted (object name is a
        # trusted bare constant — quote_object_fqn convention).
        assert f"`{CATALOG}`.`{SCHEMA}`.mv_dq_scores" in sql or f"`{CATALOG}`.`{SCHEMA}`.v_dq_check_results" in sql
        # run_mode discipline: published everywhere except the draft question.
        if question == "How many draft runs happened recently?":
            assert "`run_mode` = 'draft'" in sql
        else:
            assert "`run_mode` = 'published'" in sql
        # Table-scoped queries are parameterized trusted assets.
        if ":table_name" in sql:
            assert entry["parameters"] == [
                {
                    "name": "table_name",
                    "description": ["Fully-qualified name of the table to scope to (catalog.schema.table)."],
                    "type_hint": "STRING",
                }
            ]
        assert entry["usage_guidance"]


def test_curated_sqls_use_real_columns_not_dqlake_names() -> None:
    dump = json.dumps(gs._curated_sqls(CATALOG, SCHEMA))
    # Our schema's names…
    assert "`input_location`" in dump
    assert "`check_name`" in dump
    assert "MEASURE(`score`)" in dump
    assert "MEASURE(`failed_tests`)" in dump
    # …not dqlake's display-cased ones.
    for stale in ("`Run Ts`", "`Pass Rate`", "`Failed Tests`", "`Table`", "record_key", "dim_rule"):
        assert stale not in dump


def test_failing_rows_question_returns_whole_records_from_the_gated_view() -> None:
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    sql = "".join(by_q["Show me the rows that failed."]["sql"])
    assert f"`{CATALOG}`.`{SCHEMA}`.v_dq_failing_rows" in sql
    assert "to_json(fr.`row_data`) AS failing_record" in sql
    assert "fr.`source_table_fqn` = :table_name" in sql
    # Latest-published-run scoping via the run_id subselect (the gated view
    # has no run_mode of its own).
    assert f"SELECT `run_id` FROM `{CATALOG}`.`{SCHEMA}`.v_dq_check_results" in sql
    assert "`run_mode` = 'published'" in sql
    assert "ORDER BY `run_time` DESC LIMIT 1" in sql
    # Wrapper columns are never selected.
    assert "SELECT to_json(fr.`row_data`) AS failing_record\n" == by_q["Show me the rows that failed."]["sql"][0]


def test_top_failing_rows_question_counts_rules_from_the_variant_arrays() -> None:
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    sql = "".join(by_q["What are the failing rows with the most rules failed?"]["sql"])
    assert f"`{CATALOG}`.`{SCHEMA}`.v_dq_failing_rows" in sql
    assert "to_json(fr.`row_data`) AS failing_record" in sql
    # errors/warnings are VARIANT arrays of failure structs — count = the
    # combined array sizes (cast VARIANT -> ARRAY<VARIANT>, live-validated).
    assert "array_size(CAST(fr.`errors` AS ARRAY<VARIANT>))" in sql
    assert "array_size(CAST(fr.`warnings` AS ARRAY<VARIANT>))" in sql
    assert "ORDER BY rules_failed DESC" in sql
    assert "ORDER BY `run_time` DESC LIMIT 1" in sql


def test_columns_question_explodes_the_attribution_array() -> None:
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    sql = "".join(by_q["Which columns have the most failures?"]["sql"])
    assert "explode(`columns`)" in sql
    assert f"`{CATALOG}`.`{SCHEMA}`.v_dq_check_results" in sql
    assert "error_count" in sql and "warning_count" in sql


def test_diagnose_family_reuses_one_decomposition_sql() -> None:
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    diagnose = by_q["What is driving my changes in score over time?"]["sql"]
    assert by_q["Why did my DQ score change since the last run?"]["sql"] == diagnose
    assert by_q["What is the biggest factor affecting my DQ score?"]["sql"] == diagnose
    joined = "".join(diagnose)
    for marker in ("run_totals", "prev_ts", "'new rule'", "'more data'", "'fail rate changed'", "FULL OUTER JOIN"):
        assert marker in joined
    # Null-safe join — dimension is NULL for untagged checks.
    assert "<=>" in joined


def test_benchmarks_reuse_curated_sql_verbatim_and_cover_all_questions() -> None:
    space = build()
    benchmarks = space["benchmarks"]["questions"]
    assert [b["id"] for b in benchmarks] == sorted(b["id"] for b in benchmarks)
    by_q = {b["question"][0]: b for b in benchmarks}
    curated = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    # Every curated question is benchmarked.
    assert set(curated).issubset(set(by_q))
    # Rephrasings reuse the flagship SQL verbatim.
    flagship_sql = curated["What is driving my changes in score over time?"]["sql"]
    assert by_q["Why did my DQ score change since the last run?"]["answer"][0]["content"] == flagship_sql
    # The restored row-level flagship + rephrasings (P4.2).
    rows_sql = curated["Show me the rows that failed."]["sql"]
    assert by_q["List the failing records for this table."]["answer"][0]["content"] == rows_sql
    assert by_q["Which records failed the data-quality checks?"]["answer"][0]["content"] == rows_sql
    for b in benchmarks:
        assert b["answer"][0]["format"] == "SQL"


def test_sql_snippets_shape_and_grounding() -> None:
    space = build()
    snippets = space["instructions"]["sql_snippets"]
    assert set(snippets.keys()) == {"measures", "filters", "expressions"}
    for kind, items in snippets.items():
        assert [s["id"] for s in items] == sorted(s["id"] for s in items), kind
    # Per-part qualification (the same form as the curated SQLs). Wrapping
    # the whole dotted FQN in one backtick pair — dqlake's form — makes it a
    # single identifier and fails to resolve (live-confirmed
    # UNRESOLVED_COLUMN).
    mv_quoted = f"`{CATALOG}`.`{SCHEMA}`.mv_dq_scores"
    v_quoted = f"`{CATALOG}`.`{SCHEMA}`.v_dq_check_results"
    fr_quoted = f"`{CATALOG}`.`{SCHEMA}`.v_dq_failing_rows"
    measures = {m["alias"]: m for m in snippets["measures"]}
    assert measures["pass_rate"]["sql"] == [f"MEASURE({mv_quoted}.`score`)"]
    assert measures["failed_tests"]["sql"] == [f"MEASURE({mv_quoted}.`failed_tests`)"]
    # Published-by-default building blocks for EVERY table that has a
    # run_mode framing: the metric view, the shaping view, and the gated
    # failing-rows view (via the run_id subselect — it has no run_mode).
    filters = {f["display_name"]: f for f in snippets["filters"]}
    assert set(filters) == {"published runs", "published results", "published failing rows"}
    assert filters["published runs"]["sql"] == [f"{mv_quoted}.`run_mode` = 'published'"]
    assert filters["published results"]["sql"] == [f"{v_quoted}.`run_mode` = 'published'"]
    assert filters["published failing rows"]["sql"] == [
        f"{fr_quoted}.`run_id` IN (SELECT `run_id` FROM {v_quoted} WHERE `run_mode` = 'published')"
    ]
    for f in filters.values():
        assert "DEFAULT" in f["instruction"][0]
    (severity_rank,) = snippets["expressions"]
    assert severity_rank["sql"][0].startswith(f"CASE {mv_quoted}.`severity` ")
    assert "WHEN 'Critical' THEN 0" in severity_rank["sql"][0]


def test_sql_snippets_never_wrap_the_whole_fqn_in_one_backtick_pair() -> None:
    space = build()
    dump = json.dumps(space["instructions"]["sql_snippets"])
    assert f"`{MV_FQN}`" not in dump


def test_column_configs_enable_prompt_matching_sorted_by_name() -> None:
    space = build()
    by_id = {t["identifier"]: t for t in space["data_sources"]["tables"]}
    results_cols = by_id[RESULTS_FQN]["column_configs"]
    assert [c["column_name"] for c in results_cols] == sorted(c["column_name"] for c in results_cols)
    assert {c["column_name"] for c in results_cols} == {
        "check_name",
        "criticality",
        "dimension",
        "input_location",
        "run_mode",
        "severity",
    }
    attribution_cols = by_id[ATTRIBUTION_FQN]["column_configs"]
    assert {c["column_name"] for c in attribution_cols} == {
        "check_name",
        "dimension",
        "severity",
        "source_table_fqn",
    }
    failing_cols = by_id[FAILING_FQN]["column_configs"]
    # Only the string filter column users name by value — the ids and
    # VARIANT payloads get no entity matching.
    assert {c["column_name"] for c in failing_cols} == {"source_table_fqn"}
    for c in [*results_cols, *attribution_cols, *failing_cols]:
        assert c["enable_format_assistance"] is True
        assert c["enable_entity_matching"] is True


def test_create_payload_shape() -> None:
    payload = gs.build_create_payload(CATALOG, SCHEMA, warehouse_id="wh-1", parent_path="/Users/sp")
    assert set(payload.keys()) == {"serialized_space", "warehouse_id", "parent_path", "title", "description"}
    assert payload["warehouse_id"] == "wh-1"
    assert payload["parent_path"] == "/Users/sp"
    assert payload["title"] == gs.SPACE_TITLE
    assert payload["description"] == gs.SPACE_DESCRIPTION
    assert json.loads(payload["serialized_space"])["version"] == 2


# ---------------------------------------------------------------------------
# config_hash
# ---------------------------------------------------------------------------


def test_config_hash_is_stable_across_builds() -> None:
    assert gs.config_hash(CATALOG, SCHEMA) == gs.config_hash(CATALOG, SCHEMA)


def test_config_hash_changes_with_the_target_objects() -> None:
    assert gs.config_hash(CATALOG, SCHEMA) != gs.config_hash("other", SCHEMA)
    assert gs.config_hash(CATALOG, SCHEMA) != gs.config_hash(CATALOG, "other")


def test_random_ids_do_not_perturb_the_hash() -> None:
    """Two real builds get different random ids but must hash identically."""
    a = gs.build_serialized_space(CATALOG, SCHEMA)
    b = gs.build_serialized_space(CATALOG, SCHEMA)
    assert a != b  # random ids differ
    assert gs.config_hash(CATALOG, SCHEMA) == gs.config_hash(CATALOG, SCHEMA)


# ---------------------------------------------------------------------------
# ensure_dq_genie_space — idempotency / status / REST pinning
# ---------------------------------------------------------------------------


def ensure(settings: MagicMock, ws: MagicMock) -> str | None:
    return gs.ensure_dq_genie_space(
        settings=settings,
        ws=ws,
        warehouse_id="wh-1",
        parent_path="/Users/sp",
        catalog=CATALOG,
        schema=SCHEMA,
    )


def test_creates_space_and_persists_id_hash_status(settings: MagicMock, ws: MagicMock) -> None:
    ws.api_client.do.side_effect = [
        {"spaces": []},  # GET list — nothing to reuse
        {"space_id": "space-123"},  # POST create
    ]
    assert ensure(settings, ws) == "space-123"

    list_call, create_call = do_calls(ws)
    assert list_call.args == ("GET", "/api/2.0/genie/spaces")
    assert list_call.kwargs == {"query": {"page_size": 100}}
    assert create_call.args == ("POST", "/api/2.0/genie/spaces")
    body = create_call.kwargs["body"]
    assert body["warehouse_id"] == "wh-1"
    assert body["parent_path"] == "/Users/sp"
    assert body["title"] == gs.SPACE_TITLE
    assert json.loads(body["serialized_space"])["version"] == 2

    assert settings.store[gs.SETTING_SPACE_ID] == "space-123"
    assert settings.store[gs.SETTING_CONFIG_HASH] == gs.config_hash(CATALOG, SCHEMA)
    assert settings.store[gs.SETTING_STATUS] == gs.STATUS_READY


def test_reuses_existing_space_by_title_prefix_newest_first(settings: MagicMock, ws: MagicMock) -> None:
    ws.api_client.do.return_value = {
        "spaces": [
            {"space_id": "old", "title": f"{gs.SPACE_TITLE} 2026-01-01 00:00:00"},
            {"space_id": "new", "title": f"{gs.SPACE_TITLE} 2026-06-01 00:00:00"},
            {"space_id": "unrelated", "title": "Some other space"},
        ]
    }
    assert ensure(settings, ws) == "new"
    # Found by prefix — no POST create happened.
    assert all(c.args[0] != "POST" for c in do_calls(ws))
    assert settings.store[gs.SETTING_SPACE_ID] == "new"
    assert settings.store[gs.SETTING_STATUS] == gs.STATUS_READY


def test_find_pages_through_the_space_list(settings: MagicMock, ws: MagicMock) -> None:
    ws.api_client.do.side_effect = [
        {"spaces": [{"space_id": "x", "title": "nope"}], "next_page_token": "t2"},
        {"spaces": [{"space_id": "match", "title": f"{gs.SPACE_TITLE} 2026-05-01"}]},
    ]
    assert gs._find_space_id_by_title(ws, gs.SPACE_TITLE) == "match"
    first, second = do_calls(ws)
    assert first.kwargs == {"query": {"page_size": 100}}
    assert second.kwargs == {"query": {"page_size": 100, "page_token": "t2"}}


def test_noop_when_id_present_and_hash_unchanged(settings: MagicMock, ws: MagicMock) -> None:
    settings.store[gs.SETTING_SPACE_ID] = "space-123"
    settings.store[gs.SETTING_CONFIG_HASH] = gs.config_hash(CATALOG, SCHEMA)
    assert ensure(settings, ws) == "space-123"
    ws.api_client.do.assert_not_called()
    settings.save_setting.assert_not_called()


def test_patches_in_place_when_hash_drifted(settings: MagicMock, ws: MagicMock) -> None:
    settings.store[gs.SETTING_SPACE_ID] = "space-123"
    settings.store[gs.SETTING_CONFIG_HASH] = "stale-hash"
    ws.api_client.do.return_value = {}
    assert ensure(settings, ws) == "space-123"

    (patch_call,) = do_calls(ws)
    assert patch_call.args == ("PATCH", "/api/2.0/genie/spaces/space-123")
    assert json.loads(patch_call.kwargs["body"]["serialized_space"])["version"] == 2

    assert settings.store[gs.SETTING_CONFIG_HASH] == gs.config_hash(CATALOG, SCHEMA)
    assert settings.store[gs.SETTING_STATUS] == gs.STATUS_READY


def test_patch_failure_keeps_old_hash_so_next_boot_retries(settings: MagicMock, ws: MagicMock) -> None:
    settings.store[gs.SETTING_SPACE_ID] = "space-123"
    settings.store[gs.SETTING_CONFIG_HASH] = "stale-hash"
    ws.api_client.do.side_effect = RuntimeError("transient flap")
    assert ensure(settings, ws) == "space-123"
    # Hash NOT advanced — the next provision sees a mismatch and retries.
    assert settings.store[gs.SETTING_CONFIG_HASH] == "stale-hash"
    # The space still answers — keep it usable.
    assert settings.store[gs.SETTING_STATUS] == gs.STATUS_READY


def test_create_failure_sets_error_status_and_returns_none(settings: MagicMock, ws: MagicMock) -> None:
    ws.api_client.do.side_effect = [
        {"spaces": []},
        RuntimeError("permission denied"),
    ]
    assert ensure(settings, ws) is None
    assert settings.store[gs.SETTING_STATUS] == gs.STATUS_ERROR
    assert gs.SETTING_SPACE_ID not in settings.store
    assert gs.SETTING_CONFIG_HASH not in settings.store


def test_create_without_space_id_in_response_sets_error(settings: MagicMock, ws: MagicMock) -> None:
    ws.api_client.do.side_effect = [{"spaces": []}, {}]
    assert ensure(settings, ws) is None
    assert settings.store[gs.SETTING_STATUS] == gs.STATUS_ERROR


def test_ensure_never_raises_even_when_settings_blow_up(ws: MagicMock) -> None:
    settings = create_autospec(AppSettingsService, instance=True)
    settings.get_setting.side_effect = RuntimeError("settings store down")
    settings.save_setting.side_effect = RuntimeError("settings store down")
    assert (
        gs.ensure_dq_genie_space(
            settings=settings,
            ws=ws,
            warehouse_id="wh-1",
            parent_path="/Users/sp",
            catalog=CATALOG,
            schema=SCHEMA,
        )
        is None
    )


def test_list_failure_degrades_to_create(settings: MagicMock, ws: MagicMock) -> None:
    ws.api_client.do.side_effect = [
        RuntimeError("list unavailable"),
        {"space_id": "space-9"},
    ]
    assert ensure(settings, ws) == "space-9"
    assert settings.store[gs.SETTING_SPACE_ID] == "space-9"
