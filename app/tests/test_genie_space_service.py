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
ASOF_FQN = f"{CATALOG}.{SCHEMA}.v_dq_check_results_asof"
ATTRIBUTION_FQN = f"{CATALOG}.{SCHEMA}.v_dq_check_attribution"
FAILING_FQN = f"{CATALOG}.{SCHEMA}.v_dq_failing_rows"
DIM_RULES_FQN = f"{CATALOG}.{SCHEMA}.dim_dq_rules"
DIM_TABLES_FQN = f"{CATALOG}.{SCHEMA}.dim_dq_monitored_tables"

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


def test_data_sources_are_exactly_the_score_objects_plus_metadata_dims() -> None:
    space = build()
    tables = space["data_sources"]["tables"]
    # Five score objects (four views) + the two metadata dims (P8.1).
    assert [t["identifier"] for t in tables] == sorted(
        [ATTRIBUTION_FQN, RESULTS_FQN, ASOF_FQN, FAILING_FQN, DIM_RULES_FQN, DIM_TABLES_FQN]
    )
    mvs = space["data_sources"]["metric_views"]
    assert [m["identifier"] for m in mvs] == [MV_FQN]
    for src in [*tables, *mvs]:
        assert src["description"] and all(isinstance(d, str) for d in src["description"])


def test_asof_source_description_teaches_the_partition_discipline() -> None:
    # The load-bearing rules: filter to exactly one include_drafts
    # partition (NOT include_drafts default), group by as_of_time, and
    # never treat the repeated rows as single-run facts.
    space = build()
    by_id = {t["identifier"]: t for t in space["data_sources"]["tables"]}
    desc = "".join(by_id[ASOF_FQN]["description"])
    for phrase in ("include_drafts", "as_of_time", "NOT include_drafts", "latest such run"):
        assert phrase in desc, phrase


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


def test_metadata_dim_sources_present_and_grounded() -> None:
    # P8.1: the two SP-owned metadata dims are attached as data sources so
    # Genie can answer authoring/ownership questions from UC (Lakebase is
    # unreachable). Each description is grounded in its real columns.
    space = build()
    by_id = {t["identifier"]: t for t in space["data_sources"]["tables"]}
    assert DIM_RULES_FQN in by_id
    assert DIM_TABLES_FQN in by_id
    rules_desc = "".join(by_id[DIM_RULES_FQN]["description"])
    for col in ("rule_id", "name", "description", "dimension", "default_severity", "mode", "status", "steward"):
        assert col in rules_desc, col
    tables_desc = "".join(by_id[DIM_TABLES_FQN]["description"])
    for col in ("binding_id", "table_fqn", "steward", "status", "schedule_cron", "version"):
        assert col in tables_desc, col


def test_dim_rules_default_severity_column_config_disambiguates_from_applied() -> None:
    # The default_severity column comment must steer stewards to the APPLIED
    # severity on the score objects for run-time severity questions, and be
    # explicit that this column is only the rule's own authored default.
    space = build()
    by_id = {t["identifier"]: t for t in space["data_sources"]["tables"]}
    configs = {c["column_name"]: c for c in by_id[DIM_RULES_FQN]["column_configs"]}
    assert set(configs) == {"default_severity", "dimension", "mode", "name", "status", "steward"}
    desc = "".join(configs["default_severity"]["description"])
    assert "DEFAULT" in desc
    assert "NOT what actually ran" in desc
    assert "v_dq_check_attribution" in desc
    assert "v_dq_check_results" in desc
    assert "APPLIED" in desc


def test_text_instructions_clarify_applied_vs_default_severity() -> None:
    # P8.1: an unqualified "severity" means the APPLIED/effective severity on
    # the score objects, NOT dim_dq_rules.default_severity (only surfaced for
    # defaults/authoring/drift questions).
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "APPLIED severity" in joined
    assert "dim_dq_rules.default_severity" in joined
    assert "severity_override" in joined
    assert "rule defaults" in joined
    assert "drift" in joined


def test_metadata_dim_curated_questions_read_the_dim() -> None:
    # The authoring/ownership sample questions have curated SQL over
    # dim_dq_rules (no run_mode, no score-object join).
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    for question in ("Which rules does a steward own?", "What is the description of a rule?"):
        sql = "".join(by_q[question]["sql"])
        assert f"`{CATALOG}`.`{SCHEMA}`.dim_dq_rules" in sql
        assert "run_mode" not in sql
    assert ":steward" in "".join(by_q["Which rules does a steward own?"]["sql"])
    assert ":rule_name" in "".join(by_q["What is the description of a rule?"]["sql"])


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
    # Terse dqlake-style set — one topic per bullet.
    assert len(content) == 11
    assert all(p.endswith("\n") for p in content)
    joined = "".join(content)
    assert "MEASURE()" in joined
    assert "published" in joined and "draft" in joined
    assert "(Data product: <name> — tables: ...)" in joined
    # P5.2: rule identity across renames — group by registry_rule_id where
    # present, display the newest run's check_name.
    assert "registry_rule_id" in joined and "newest run" in joined


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


def test_text_instructions_failing_records_are_per_run() -> None:
    # P5.5: failing records never stack across runs — the row source scopes
    # to the SINGLE latest published run, and the steward is told records
    # are per-run (name a specific run to see another).
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "Failing records are per-run" in joined
    assert "latest published run" in joined
    assert "specific" in joined


def test_text_instructions_change_diagnosis_enumerates_the_contributor_taxonomy() -> None:
    # P5.5 (live user report): the space concluded "a single event or issue
    # led to the lower score" and only surfaced the newly added rule after
    # two follow-up prompts. The diagnosis element must require computing
    # the contributors, and it must enumerate the COMPLETE taxonomy by
    # name — the categories the curated decomposition's reason column
    # emits — so the space names what it finds, unprompted.
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "compute the contributors before concluding" in joined
    assert "unprompted" in joined
    assert "never settle" in joined
    assert "category and magnitude" in joined
    # The taxonomy, category by category.
    assert "rules added" in joined
    assert "rules removed" in joined
    assert "rules renamed" in joined
    assert "a rename, not an add" in joined
    assert "rule definitions changed" in joined
    assert "failure-rate changes" in joined
    assert "test-volume changes" in joined
    # Identity discipline (P5.2) and the dimension/severity rollup ask.
    assert "registry_rule_id where present" in joined
    assert "which dimension or severity moved most" in joined
    # The newly-added-rule reading survives the rewrite.
    assert "not one that passed before" in joined
    # ...and so does the stable-history honesty clause.
    assert "stable over the available history" in joined


def test_text_instructions_prefer_paragraphs_over_bullets() -> None:
    # P5.5 (live user feedback): prose answers as short paragraphs; bullets
    # only for genuine multi-item breakdowns.
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "short paragraphs" in joined
    assert "genuine multi-item" in joined


def test_text_instructions_rates_always_as_percentages() -> None:
    # P6.2: the views emit scores/rates as fractions of 1 — Genie must
    # convert and present them as percentages with one decimal, never a
    # bare fraction.
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "percentage" in joined
    assert "one decimal" in joined
    assert '"91.5%"' in joined
    assert "0.915" in joined
    assert "never" in joined


def test_text_instructions_headline_then_blank_line_then_breakdown() -> None:
    # P6.2: answers open with a one-sentence headline carrying the key
    # finding and its number, then a blank line, then the breakdown
    # paragraphs (the UI renders blank-line-separated paragraphs).
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "headline" in joined
    assert "one sentence" in joined
    assert "key finding" in joined
    assert "blank line" in joined


def test_every_sample_question_has_a_curated_sql() -> None:
    curated = {e["question"][0] for e in gs._curated_sqls(CATALOG, SCHEMA)}
    assert curated == set(gs.SAMPLE_QUESTIONS)


# Authoring/ownership questions read the metadata dims (dim_dq_rules /
# dim_dq_monitored_tables), which carry no run_mode and never join the
# run-facing score objects — the run_mode/score-object discipline below
# applies only to the results-facing questions.
_METADATA_DIM_QUESTIONS = {
    "Which rules does a steward own?",
    "What is the description of a rule?",
    "How many rules have been added recently?",
    "How many rules are running?",
}


def test_curated_sqls_are_grounded_on_our_objects_and_run_mode() -> None:
    for entry in gs._curated_sqls(CATALOG, SCHEMA):
        sql = "".join(entry["sql"])
        question = entry["question"][0]
        if question in _METADATA_DIM_QUESTIONS:
            # Metadata dim — no run_mode, no score-object join; grounded on
            # dim_dq_rules with the catalog/schema backtick-quoted per part.
            assert f"`{CATALOG}`.`{SCHEMA}`.dim_dq_rules" in sql
            assert "run_mode" not in sql
            assert entry["usage_guidance"]
            continue
        # Only our objects, catalog/schema backtick-quoted (object name is a
        # trusted bare constant — quote_object_fqn convention).
        assert (
            f"`{CATALOG}`.`{SCHEMA}`.mv_dq_scores" in sql or f"`{CATALOG}`.`{SCHEMA}`.v_dq_check_results" in sql
        )  # v_dq_check_results_asof matches the v_dq_check_results prefix
        # run_mode discipline: published everywhere except the draft
        # question. The as-of average reads v_dq_check_results_asof, whose
        # published-only scoping is the include_drafts partition selector
        # (run_mode filtering happened inside the view's partition build).
        if question == "How many draft runs happened recently?":
            assert "`run_mode` = 'draft'" in sql
        elif f"`{CATALOG}`.`{SCHEMA}`.v_dq_check_results_asof" in sql:
            assert "NOT `include_drafts`" in sql
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
    for marker in ("run_totals", "prev_ts", "FULL OUTER JOIN"):
        assert marker in joined
    # Null-safe comparisons — attribution columns are NULL for untagged runs.
    assert "<=>" in joined


def test_diagnose_reason_column_carries_the_full_contributor_taxonomy() -> None:
    # P5.5 follow-up: the reason column must categorize every rule with the
    # complete change-contributor taxonomy so Genie NARRATES categories the
    # SQL hands it rather than inferring them (teaching the query beats
    # exhorting the model).
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    joined = "".join(by_q["What is driving my changes in score over time?"]["sql"])
    for reason in (
        "'rule added'",
        "'rule removed'",
        "'rule definition changed'",
        "'rule renamed'",
        "'failure rate worsened'",
        "'failure rate improved'",
        "'more data'",
        "'less data'",
        "'unchanged'",
    ):
        assert reason in joined
    # dqlake's coarse categories are fully superseded.
    for stale in ("'new rule'", "'fail rate changed'"):
        assert stale not in joined


def test_diagnose_pairs_rules_by_registry_identity() -> None:
    # Identity across renames (P5.2): rows pair on registry_rule_id where
    # present (check_name only as the legacy fallback), so a renamed rule
    # matches its prior self instead of reading as one removal plus one
    # addition. That requires v_dq_check_results — the metric view carries
    # no registry_rule_id.
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    joined = "".join(by_q["What is driving my changes in score over time?"]["sql"])
    assert f"`{CATALOG}`.`{SCHEMA}`.v_dq_check_results" in joined
    assert "COALESCE(`registry_rule_id`, `check_name`) AS rule_key" in joined
    assert "ON c.rule_key = p.rule_key" in joined
    # The prior name rides along so a rename stays visible even when a
    # numeric reason outranks it; severity rides along for the rollups.
    assert "prev_rule_name" in joined
    assert "AS severity" in joined
    # Definition change compares the mapped columns arrays, but only when
    # both runs actually carry them (legacy runs have NULL attribution).
    assert "to_json(`columns`)" in joined
    assert "c.cols IS NOT NULL AND p.cols IS NOT NULL" in joined


def test_diagnose_surfaces_the_run_instants_being_compared() -> None:
    # P7.2 rider (live user ask): when Genie explains a change it must NAME
    # the run date/time the change appeared. The decomposition therefore
    # surfaces the two run instants it compares as output columns — both in
    # the rule-grain taxonomy SQL and the by-dimension variant — so the
    # citation is a column read, not an extra query.
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    for question in (
        "What is driving my changes in score over time?",
        "Why has my score by dimension changed?",
    ):
        joined = "".join(by_q[question]["sql"])
        assert "AS curr_run_ts" in joined
        assert "AS prev_run_ts" in joined


def test_diagnose_surfaces_rule_to_column_mapping_changes() -> None:
    # B2-90: a rule mapped to more columns runs more checks and can lower the
    # score with no per-check failure-rate spike. The decomposition must
    # expose the rule's full mapped-column SET across both runs (unioned over
    # its check rows, so a for_each_column fan-out is visible) plus the
    # added/removed columns and the column/check counts, so Genie can name
    # the structural change instead of misreading it as generic "more data".
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    joined = "".join(by_q["What is driving my changes in score over time?"]["sql"])
    # The full mapped-column set is unioned across the rule's check rows.
    assert "array_distinct(flatten(collect_list(`columns`))) AS col_set" in joined
    assert "COUNT(DISTINCT `check_name`) AS check_count" in joined
    # The concrete composition surfaces as output columns.
    for col in (
        "AS curr_columns",
        "AS prev_columns",
        "AS added_columns",
        "AS removed_columns",
        "AS curr_column_count",
        "AS prev_column_count",
        "AS curr_check_count",
        "AS prev_check_count",
    ):
        assert col in joined, col
    # added/removed are a set difference over the two mapped-column sets.
    assert "array_except(c.col_set, p.col_set)" in joined
    assert "array_except(p.col_set, c.col_set)" in joined


def test_diagnose_definition_change_keys_off_the_mapped_column_set() -> None:
    # The 'rule definition changed' reason must fire when the mapped-column
    # SET moved (array_except non-empty in either direction) — the raw
    # c.cols <> p.cols compare missed a for_each_column expansion because each
    # check maps a single column and MAX(to_json(columns)) never reflected the
    # set growing. Both runs must still carry attribution (the NULL guard).
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    joined = "".join(by_q["What is driving my changes in score over time?"]["sql"])
    assert "c.cols IS NOT NULL AND p.cols IS NOT NULL" in joined
    assert "size(array_except(c.col_set, p.col_set)) > 0" in joined
    assert "size(array_except(p.col_set, c.col_set)) > 0" in joined
    # The fragile raw-json compare is gone.
    assert "c.cols <> p.cols" not in joined


def test_text_instructions_name_added_removed_columns_for_definition_changes() -> None:
    # B2-90 prose half: when the contributor is a definition change, the
    # answer must name the columns added/removed and the mapped-column count
    # move, and explain that applying a rule to more columns runs more checks
    # and can lower the score without a failure-rate spike.
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "added_columns / removed_columns" in joined
    assert "prev_column_count to curr_column_count" in joined
    assert "applying a rule to more columns runs more checks" in joined


def test_text_instructions_change_answers_name_the_run_instant() -> None:
    # The prose half of the P7.2 rider: every change explanation anchors to
    # WHEN — the run date/time of the two runs compared — citing the
    # decomposition's curr_run_ts / prev_run_ts columns.
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "curr_run_ts" in joined
    assert "prev_run_ts" in joined
    assert "run date and time" in joined


def test_genie_rule_queries_label_from_rule_name() -> None:
    # The by-rule/applied/drift SQL must display MAX(`rule_name`) (the
    # underlying rule name), not MAX(`check_name`) (the per-column suffixed
    # name).  Identity grouping and rollup shape must stay intact.
    sql_text = str(gs._curated_sqls(CATALOG, SCHEMA))
    assert "MAX(`rule_name`) AS rule_name" in sql_text
    assert "COALESCE(`registry_rule_id`, `check_name`)" in sql_text
    assert "COUNT(DISTINCT `check_name`)" in sql_text


def test_genie_instructions_explain_column_fanout() -> None:
    # TEXT_INSTRUCTIONS must include the per-column fan-out explanation so
    # Genie presents a multi-column rule ONCE by its rule_name and treats the
    # per-column checks as a rollup.
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "one check per column" in joined
    assert "registry_rule_id" in joined


def test_rule_context_curated_sqls_scope_to_one_rule_by_rule_name() -> None:
    # Item 19 B: the rule-kind chip questions (previously with NO curated SQL)
    # now resolve to SQL scoped to ONE rule via :rule_name, keyed on rule_name
    # (never check_name) and grouped/compared on registry_rule_id across runs.
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    rule_questions = [
        "What is this rule's overall pass rate?",
        "How many tables is this rule applied to?",
        "How many failures does this rule have right now?",
        "Which tables is this rule failing on most?",
        "Which table is hurting this rule's score the most?",
        "Which columns does this rule fail on most?",
        "How has this rule's pass rate changed over recent runs?",
    ]
    for question in rule_questions:
        entry = by_q[question]
        sql = "".join(entry["sql"])
        assert ":rule_name" in sql, question
        assert "`rule_name` = :rule_name" in sql, question
        assert "`check_name` = :rule_name" not in sql, question
        assert entry["parameters"] == [
            {
                "name": "rule_name",
                "description": ["Name of the registry rule to scope to."],
                "type_hint": "STRING",
            }
        ], question
        assert entry["usage_guidance"]
    # The rule-context routing paragraph is taught in TEXT_INSTRUCTIONS.
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "(Rule: <name>)" in joined
    assert "ONE registry rule" in joined
    # build_serialized_space carries rule_name / registry_rule_id as metric
    # dimensions in the metric-view description.
    space = build()
    mv_desc = "".join(space["data_sources"]["metric_views"][0]["description"])
    assert "rule_name" in mv_desc
    assert "registry_rule_id" in mv_desc


def test_breach_curated_sql_and_instructions() -> None:
    # Item 19 D: breach awareness. A check breaches when its run pass rate
    # falls below the pass_threshold frozen into that run (NULL-gated).
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    sql = "".join(by_q["Which checks breached their pass threshold?"]["sql"])
    assert f"`{CATALOG}`.`{SCHEMA}`.v_dq_check_results" in sql
    assert "`pass_threshold` IS NOT NULL" in sql
    assert "`pass_threshold` / 100.0" in sql
    assert "`run_mode` = 'published'" in sql
    joined = "".join(gs.TEXT_INSTRUCTIONS)
    assert "breach" in joined
    assert "pass_threshold" in joined


def test_rules_have_reads_the_metric_view_via_measure_rule_count() -> None:
    # Item 59 acceptance smoke test: 'how many rules do I have' MUST resolve to
    # MEASURE(rule_count) over the metric view (the distinct rules that ran),
    # not a dim_dq_rules count.
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    sql = "".join(by_q["How many rules do I have?"]["sql"])
    assert f"`{CATALOG}`.`{SCHEMA}`.mv_dq_scores" in sql
    assert "MEASURE(`rule_count`)" in sql
    assert "MEASURE(`failed_rule_count`)" in sql
    assert "`run_mode` = 'published'" in sql
    assert "dim_dq_rules" not in sql


def test_registry_count_curated_sqls_read_the_dim() -> None:
    # Item 19 C: authoring-count questions read dim_dq_rules (no run_mode).
    by_q = {e["question"][0]: e for e in gs._curated_sqls(CATALOG, SCHEMA)}
    for question in (
        "How many rules have been added recently?",
        "How many rules are running?",
    ):
        sql = "".join(by_q[question]["sql"])
        assert f"`{CATALOG}`.`{SCHEMA}`.dim_dq_rules" in sql
        assert "run_mode" not in sql
    assert "INTERVAL 7 DAYS" in "".join(by_q["How many rules have been added recently?"]["sql"])


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
    # failing-rows view. The failing-rows filter resolves each table's
    # SINGLE latest published run (correlated subselect — P5.5: failing
    # records are per-run and never stack across runs).
    filters = {f["display_name"]: f for f in snippets["filters"]}
    assert set(filters) == {"published runs", "published results", "latest published failing rows"}
    assert filters["published runs"]["sql"] == [f"{mv_quoted}.`run_mode` = 'published'"]
    assert filters["published results"]["sql"] == [f"{v_quoted}.`run_mode` = 'published'"]
    assert filters["latest published failing rows"]["sql"] == [
        f"{fr_quoted}.`run_id` = (SELECT `run_id` FROM {v_quoted} "
        f"WHERE `input_location` = {fr_quoted}.`source_table_fqn` "
        "AND `run_mode` = 'published' ORDER BY `run_time` DESC LIMIT 1)"
    ]
    for f in filters.values():
        assert "DEFAULT" in f["instruction"][0]
    assert "per-run" in filters["latest published failing rows"]["instruction"][0]
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
        "rule_name",
        "run_mode",
        "severity",
    }
    asof_cols = by_id[ASOF_FQN]["column_configs"]
    assert [c["column_name"] for c in asof_cols] == sorted(c["column_name"] for c in asof_cols)
    assert {c["column_name"] for c in asof_cols} == {
        "check_name",
        "dimension",
        "input_location",
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
