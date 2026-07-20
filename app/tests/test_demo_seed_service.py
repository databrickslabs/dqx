"""Unit tests for the DemoSeedService orchestrator.

All dependencies are mocked (``create_autospec`` / ``MagicMock``): these tests
assert the orchestration wiring (rules created, bindings approved, products
approved, terminal status written) rather than warehouse behaviour. The
``weeks=0`` short-circuit builds every governed object and writes a terminal
status but skips the validation-gate real-run and the weekly history loop, so
the whole pipeline is exercisable without a live SQL warehouse.
"""

import json

from datetime import datetime, timezone
from unittest.mock import create_autospec, MagicMock

import pytest

from databricks_labs_dqx_app.backend.demo import manifest, redate
from databricks_labs_dqx_app.backend.demo.seed_service import DemoSeedService
from databricks_labs_dqx_app.backend.demo.status import DemoStatusStore
from databricks_labs_dqx_app.backend.services.registry_service import RegistryService


def _svc(**over):
    from databricks.sdk import WorkspaceClient
    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor
    from databricks_labs_dqx_app.backend.services.monitored_table_service import MonitoredTableService
    from databricks_labs_dqx_app.backend.services.apply_rules_service import ApplyRulesService
    from databricks_labs_dqx_app.backend.services.materializer import Materializer
    from databricks_labs_dqx_app.backend.services.rules_catalog_service import RulesCatalogService
    from databricks_labs_dqx_app.backend.services.monitored_table_versions import MonitoredTableVersionService
    from databricks_labs_dqx_app.backend.services.data_product_service import DataProductService
    from databricks_labs_dqx_app.backend.services.binding_run_service import BindingRunService
    from databricks_labs_dqx_app.backend.services.score_cache_service import ScoreCacheService

    deps = dict(
        demo_sql=create_autospec(SqlExecutor, instance=True),
        app_sql=create_autospec(SqlExecutor, instance=True),
        oltp=MagicMock(),
        sp_ws=create_autospec(WorkspaceClient, instance=True),
        registry=create_autospec(RegistryService, instance=True),
        monitored_tables=create_autospec(MonitoredTableService, instance=True),
        apply_rules=create_autospec(ApplyRulesService, instance=True),
        materializer=create_autospec(Materializer, instance=True),
        rules_catalog=create_autospec(RulesCatalogService, instance=True),
        version_service=create_autospec(MonitoredTableVersionService, instance=True),
        data_products=create_autospec(DataProductService, instance=True),
        binding_run=create_autospec(BindingRunService, instance=True),
        score_cache=create_autospec(ScoreCacheService, instance=True),
        status=create_autospec(DemoStatusStore, instance=True),
    )
    deps.update(over)
    return DemoSeedService(**deps), deps


def _mapped_rule_keys() -> set[str]:
    """Rule keys that land in the build_rules map (all except pending-approval)."""
    return {spec.key for spec in manifest.RULES if spec.key not in manifest.PENDING_APPROVAL_RULE_KEYS}


def _use_create_path(deps, rule_id: str = "r1") -> None:
    """Configure the registry mock so _build_rules takes the genuine create path.

    ``find_approved_rule_for_definition`` returns None (no pre-existing approved
    rule), so every manifest rule is created via ``create_rule -> submit ->
    approve``. ``create_rule`` returns ``(rule, warning)`` and ``approve``
    returns the published rule.
    """
    deps["registry"].find_approved_rule_for_definition.return_value = None
    # Pending rules dedupe on structural fingerprint (idempotent re-seed guard);
    # None means "no existing active rule", so the create path runs.
    deps["registry"].get_active_rule_by_fingerprint.return_value = None
    deps["registry"].create_rule.return_value = (MagicMock(rule_id=rule_id), None)
    deps["registry"].submit.return_value = MagicMock(rule_id=rule_id)
    deps["registry"].approve.return_value = MagicMock(rule_id=rule_id, version=2)


def test_run_creates_all_rules_and_writes_terminal_status():
    svc, deps = _svc()
    _use_create_path(deps)
    # make binding registration + run + score reads no-op-friendly
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    # short-circuit the wait+validate+weekly loop via a 0-week run for the unit test
    svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)
    # every manifest rule is created + submitted, but the pending-approval
    # rule(s) are NOT approved — they stay awaiting a human decision.
    approved_count = len(manifest.RULES) - len(manifest.PENDING_APPROVAL_RULE_KEYS)
    assert deps["registry"].create_rule.call_count == len(manifest.RULES)
    assert deps["registry"].submit.call_count == len(manifest.RULES)
    assert deps["registry"].approve.call_count == approved_count
    # profiler-suggestion primitive is NOT used (it hardcodes dqx_native)
    assert not deps["registry"].match_or_create_approved_rule.called
    # terminal status written
    assert deps["status"].set.called
    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state in {"succeeded", "failed"}


def test_build_rules_uses_real_mode_and_polarity_per_spec():
    # BUG D regression: SQL-mode rules must be created with mode="sql" and
    # polarity="pass", NOT forced to dqx_native (which would materialize to
    # function: '' and fail at runtime with "function '' is not defined").
    # FEATURE H: lowcode rules likewise keep mode="lowcode" + polarity="pass"
    # (their compiled predicate describes a passing row); dqx_native carries no
    # polarity.
    svc, deps = _svc()
    _use_create_path(deps)
    svc._build_rules("admin@example.com")

    by_mode: dict[str, str] = {}
    for call in deps["registry"].create_rule.call_args_list:
        mode = call.kwargs["mode"]
        polarity = call.kwargs["polarity"]
        author_kind = call.kwargs["author_kind"]
        assert call.kwargs["source"] == "demo"
        by_mode[mode] = author_kind
        if mode in {"sql", "lowcode"}:
            assert polarity == "pass", f"{mode} demo predicates describe a passing row"
        else:
            assert polarity is None, f"{mode} rules must not carry a polarity"
    # all three modes are present in the demo set
    assert "sql" in by_mode
    assert "dqx_native" in by_mode
    assert "lowcode" in by_mode


def test_definition_for_moves_scalars_into_parameters():
    # BUG G: a dqx_native rule's scalar check arguments must surface as typed
    # RuleDefinition.parameters (the authoring UI reads scalars from there); the
    # compiled body.arguments must hold ONLY {{slot}} column placeholders.
    spec = manifest.RULES_BY_KEY["country_set"]  # is_in_list with an `allowed` scalar
    definition = DemoSeedService._definition_for(spec)

    param_names = {p.name for p in definition.parameters}
    assert "allowed" in param_names, "the scalar `allowed` must be a RuleParameter"
    allowed = next(p for p in definition.parameters if p.name == "allowed")
    assert allowed.type == "list"
    assert isinstance(allowed.value, list) and "US" in allowed.value
    # body carries only the column placeholder, no scalar
    assert definition.body["arguments"] == {"column": "{{country}}"}
    assert "allowed" not in definition.body["arguments"]


def test_definition_for_compiles_lowcode_body_with_predicate():
    # FEATURE H: a lowcode spec carries only lowcode_ast in its manifest body;
    # _definition_for must compile it into the stored body the materializer runs
    # (predicate present, re-editable lowcode_ast preserved).
    spec = manifest.RULES_BY_KEY["pct_range"]
    assert spec.mode == "lowcode"
    definition = DemoSeedService._definition_for(spec)

    assert "lowcode_ast" in definition.body, "the re-editable AST must be preserved for the visual builder"
    assert definition.body.get("predicate"), "a single-row lowcode rule compiles to a predicate"
    assert "{{pct}}" in definition.body["predicate"], "predicate references the declared slot placeholder"
    # low-code slots fill placeholders, not function args
    assert all(s.arg_key is None for s in definition.slots)


def test_definition_for_compiles_lowcode_uniqueness_to_group_by_query():
    # The canonical uniqueness shape is an aggregated count = 1 with a group-by;
    # it compiles to a sql_query + merge_columns, not a bare predicate.
    spec = manifest.RULES_BY_KEY["unique"]
    assert spec.mode == "lowcode"
    definition = DemoSeedService._definition_for(spec)

    assert definition.body.get("sql_query"), "grouped uniqueness compiles to a sql_query"
    assert definition.body.get("merge_columns") == ["{{key}}"]
    assert definition.body.get("group_by") == "{{key}}"


def test_build_rules_is_idempotent_when_rule_already_approved():
    # An already-approved rule with the same fingerprint is reused, never re-created.
    svc, deps = _svc()
    deps["registry"].find_approved_rule_for_definition.return_value = MagicMock(rule_id="existing")
    # no pre-existing pending twin, so the pending rule still takes the create path.
    deps["registry"].get_active_rule_by_fingerprint.return_value = None
    # the pending-approval rule always takes the create path, so create_rule
    # must still return a (rule, warning) tuple.
    deps["registry"].create_rule.return_value = (MagicMock(rule_id="pending"), None)
    rule_map = svc._build_rules("admin@example.com")
    # the pending-approval rule always takes the create+submit path (it has no
    # approved twin to reuse), so create_rule IS called for it.
    assert deps["registry"].create_rule.call_count == len(manifest.PENDING_APPROVAL_RULE_KEYS)
    assert set(rule_map) == _mapped_rule_keys()
    assert all(rid == "existing" for rid in rule_map.values())


def test_build_rules_embeds_each_created_rule_for_suggestions():
    # The suggest-rules feature retrieves from the dq_rule_embeddings corpus,
    # which the HTTP approve route populates via embed_and_store. The seeder
    # drives approve directly (no route), so it must embed here too — otherwise
    # demo rules never enter the corpus and never surface as suggestions.
    embeddings = MagicMock()
    svc, deps = _svc(embeddings=embeddings)
    _use_create_path(deps)

    svc._build_rules("admin@example.com")

    # one embed per APPROVED manifest rule — the pending-approval rule(s) are
    # never approved, so they are never embedded into the suggestion corpus.
    embedded_count = len(manifest.RULES) - len(manifest.PENDING_APPROVAL_RULE_KEYS)
    assert embeddings.embed_and_store.call_count == embedded_count


def test_build_rules_embeds_reused_rules_too():
    # A rule reused from a prior seed (same fingerprint) may never have been
    # embedded — embed it as well so re-running the seed heals a missing corpus row.
    embeddings = MagicMock()
    svc, deps = _svc(embeddings=embeddings)
    deps["registry"].find_approved_rule_for_definition.return_value = MagicMock(rule_id="existing")
    deps["registry"].get_active_rule_by_fingerprint.return_value = None
    # the pending-approval rule takes the create path even when others are reused.
    deps["registry"].create_rule.return_value = (MagicMock(rule_id="pending"), None)

    svc._build_rules("admin@example.com")

    embedded_count = len(manifest.RULES) - len(manifest.PENDING_APPROVAL_RULE_KEYS)
    assert embeddings.embed_and_store.call_count == embedded_count


def test_pending_approval_rule_is_idempotent_when_already_present():
    # A no-wipe re-seed must NOT mint a duplicate pending draft: if an active
    # (draft/pending/approved) rule with the same fingerprint already exists,
    # the pending rule is skipped (create_rule not called for it).
    svc, deps = _svc()
    deps["registry"].find_approved_rule_for_definition.return_value = None
    # An existing active rule with the pending rule's fingerprint is found.
    deps["registry"].get_active_rule_by_fingerprint.return_value = MagicMock(rule_id="existing-pending")
    deps["registry"].create_rule.return_value = (MagicMock(rule_id="r1"), None)

    svc._build_rules("admin@example.com")

    # create_rule is called for every APPROVED manifest rule, but NOT for the
    # pending one (it was found via fingerprint and skipped).
    approved_count = len(manifest.RULES) - len(manifest.PENDING_APPROVAL_RULE_KEYS)
    assert deps["registry"].create_rule.call_count == approved_count


def test_build_rules_survives_embed_failure():
    # embed_and_store is best-effort; even an unexpected raise must never abort
    # the ~30min seed.
    embeddings = MagicMock()
    embeddings.embed_and_store.side_effect = RuntimeError("vector search down")
    svc, deps = _svc(embeddings=embeddings)
    _use_create_path(deps)

    rule_map = svc._build_rules("admin@example.com")  # must not raise
    assert set(rule_map) == _mapped_rule_keys()


def test_build_rules_without_embeddings_service_is_a_noop():
    # embeddings defaults to None (e.g. a minimal test graph) — the seeder must
    # simply skip embedding, never crash.
    svc, deps = _svc()  # no embeddings passed
    _use_create_path(deps)
    rule_map = svc._build_rules("admin@example.com")
    assert set(rule_map) == _mapped_rule_keys()


def test_wipe_first_calls_reset_service():
    reset = MagicMock()
    svc, deps = _svc(reset_service=reset)
    _use_create_path(deps)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    svc.run(user_email="admin@example.com", wipe_first=True, weeks=0)
    reset.reset_all_data.assert_called_once()


def _tag_ddls(sql_mock) -> list[str]:
    """The SET TAG statements a SqlExecutor mock received."""
    return [c.args[0] for c in sql_mock.execute.call_args_list if c.args and c.args[0].startswith("SET TAG ON COLUMN")]


def test_build_source_data_assigns_governed_tags_via_set_tag_ddl():
    # Governed dotted-key tags (class.location, class.credit_card) are assigned
    # with `SET TAG ON COLUMN <fqn>.<col> `<dotted.key>`` — the dotted key
    # backtick-quoted so it's treated literally. Runs as SQL (only the `sql`
    # scope), NOT the Unity Catalog entity-tag-assignments OBO API.
    svc, deps = _svc()
    svc._build_source_data()

    ddls = _tag_ddls(deps["demo_sql"])
    assert len(ddls) == len(manifest.COLUMN_TAGS)
    for tag in manifest.COLUMN_TAGS:
        # the governed key appears backtick-quoted with its dot intact
        assert any(f"`{tag.tag}`" in d and f"`{tag.column}`" in d and tag.table in d for d in ddls), (
            f"no SET TAG DDL for {tag.tag} on {tag.table}.{tag.column}; got {ddls!r}"
        )
    # the SP entity-tag API is no longer used for tagging
    assert not deps["sp_ws"].entity_tag_assignments.create.called


def test_column_tags_use_obo_sql_when_set_not_the_sp():
    # Governed class.* tags need ASSIGN on the tag policy, which the app SP
    # usually lacks but the deploying admin holds. When the route hands the
    # seeder the caller's OBO SqlExecutor via set_tagging_sql, the SET TAG DDL
    # must run through IT (OBO), not the SP-owned demo executor.
    from databricks_labs_dqx_app.backend.sql_executor import SqlExecutor

    svc, deps = _svc()
    obo_sql = create_autospec(SqlExecutor, instance=True)
    svc.set_tagging_sql(obo_sql)

    svc._build_source_data()

    assert len(_tag_ddls(obo_sql)) == len(manifest.COLUMN_TAGS)
    # the SP-owned demo executor ran the table DDL but NOT the SET TAG statements
    assert _tag_ddls(deps["demo_sql"]) == []


def test_column_tags_fall_back_to_sp_when_no_obo():
    # CLI / tests / no-OBO deploy: with no tagging_sql set, the SET TAG DDL runs
    # on the SP-owned demo executor (best-effort as before).
    svc, deps = _svc()  # tagging_sql defaults to None
    svc._build_source_data()
    assert len(_tag_ddls(deps["demo_sql"])) == len(manifest.COLUMN_TAGS)


def test_build_source_data_swallows_tag_assignment_errors():
    # A missing ASSIGN privilege / undefined governed tag must NOT abort the
    # ~30min seed — the failure is logged best-effort and seeding continues.
    svc, deps = _svc()

    def _raise_on_set_tag(sql, *_a, **_k):
        if sql.startswith("SET TAG ON COLUMN"):
            raise RuntimeError("no ASSIGN privilege")

    deps["demo_sql"].execute.side_effect = _raise_on_set_tag
    _use_create_path(deps)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")

    # run(weeks=0) invokes _build_source_data; the raised error must be swallowed.
    svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)

    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state == "succeeded"


def test_run_marks_failed_status_and_reraises_on_error():
    svc, deps = _svc()
    deps["registry"].find_approved_rule_for_definition.return_value = None
    deps["registry"].create_rule.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)
    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state == "failed"


def test_run_approves_bindings_and_products_not_relying_on_auto_publish():
    # The app has approvals enabled; the seeder must drive approval explicitly.
    svc, deps = _svc()
    _use_create_path(deps)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)
    # bindings materialized + frozen (approved), products submitted + approved
    assert deps["materializer"].materialize_binding.called
    assert deps["version_service"].freeze_new_version.called
    assert deps["data_products"].approve.called


def test_weekly_trend_uses_passed_rule_map_never_refingerprints():
    # FIX 1 (root cause): the weekly trend must apply the SAME authoritative
    # rule_map that _build_rules produced (and _build_bindings used) — it must
    # NEVER re-resolve rule ids by structural fingerprint per week. Re-resolving
    # could return a different/missing id than _build_rules created (e.g. after
    # the card-rule version bump, or a fingerprint near-collision), rewriting a
    # binding with an incomplete/mismatched rule set and dropping rules +
    # desyncing the applied rule_id from the registry id the Results tab queries.
    svc, deps = _svc()
    authoritative = {spec.key: f"auth-{spec.key}" for spec in manifest.RULES}
    deps["oltp"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    # SELECT 1 existence probes (off-target re-date check) return no stragglers;
    # the metrics existence poll / status reads return a row.
    deps["app_sql"].query_dicts.side_effect = lambda sql, *_a, **_k: (
        [] if "SELECT 1" in sql else [{"status": "SUCCESS"}]
    )

    binding_map = {b.table: f"b-{b.table}" for b in manifest.BINDINGS}
    svc._build_weekly_trend(binding_map, authoritative, ["p1"], 1, "admin@example.com", datetime.now(timezone.utc))

    # the trend NEVER re-derives rule ids by fingerprint
    assert not deps["registry"].find_approved_rule_for_definition.called
    # every applied rule id comes from the authoritative map
    applied_ids: set[str] = set()
    for call in deps["apply_rules"].save_applied_rules.call_args_list:
        for desired in call.args[1]:
            applied_ids.add(desired.rule_id)
    assert applied_ids  # bindings were applied
    assert applied_ids <= set(authoritative.values())


def test_desired_rules_yields_all_five_orders_rules_from_authoritative_map():
    # FIX 1: with the authoritative map, orders' week-0 active set materializes
    # all five of its rules (each with a real rule_id) — no rule is dropped.
    svc, _deps = _svc()
    authoritative = {spec.key: f"auth-{spec.key}" for spec in manifest.RULES}
    orders = next(b for b in manifest.BINDINGS if b.table == "orders")

    desired = svc._desired_rules(orders, authoritative, week=0)

    week0_keys = set(manifest.active_mapping(orders, 0))
    assert len(desired) == len(week0_keys) == 5
    assert all(d.rule_id.startswith("auth-") for d in desired)


def _metrics_rows(input_rows: int, unique_failed: int):
    """dq_metrics rows for one run: input_row_count + a unique-check breakdown."""
    check_metrics = json.dumps(
        [{"check_name": manifest.RULES_BY_KEY["unique"].name, "error_count": unique_failed, "warning_count": 0}]
    )
    return [
        {"metric_name": "input_row_count", "metric_value": str(input_rows)},
        {"metric_name": "check_metrics", "metric_value": check_metrics},
    ]


def test_assert_no_misfire_raises_when_unique_count_outside_band():
    # FIX 2: a unique check whose failed-row count is outside UNIQUE_EXPECT_ROWS
    # is a misfire even below the catastrophic rate.
    svc, deps = _svc()
    _low, high = manifest.UNIQUE_EXPECT_ROWS
    out_of_band = high + 1
    input_rows = 50_000  # rate stays far below _MISFIRE_RATE
    deps["app_sql"].query_dicts.return_value = _metrics_rows(input_rows, out_of_band)

    with pytest.raises(RuntimeError, match="uniqueness"):
        svc._assert_no_misfire("customers", "run1")


def test_assert_no_misfire_passes_when_unique_count_inside_band():
    svc, deps = _svc()
    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2
    deps["app_sql"].query_dicts.return_value = _metrics_rows(50_000, in_band)

    # inside the band and below the catastrophic rate — must not raise
    svc._assert_no_misfire("customers", "run1")


def test_assert_no_misfire_logs_when_unique_check_name_missing(caplog):
    # FIX 2: when a binding has the uniqueness rule applied but no check with the
    # reserved unique-rule name appears in metrics, the band silently never
    # fires. That skip must be observable (logged), not passing mutely — but it
    # must NOT hard-fail (the catastrophic-rate gate still protects correctness).
    svc, deps = _svc()
    # products has the unique rule applied at week 0 (no lifecycle window), so a
    # run with NO unique check in its metrics is a loggable silent skip.
    deps["app_sql"].query_dicts.return_value = [
        {"metric_name": "input_row_count", "metric_value": "5000"},
        {
            "metric_name": "check_metrics",
            "metric_value": json.dumps([{"check_name": "some_other_check", "error_count": 5, "warning_count": 0}]),
        },
    ]
    with caplog.at_level("WARNING", logger="databricks_labs_dqx_app.backend.demo.seed_service"):
        svc._assert_no_misfire("products", "run1")  # must not raise
    assert "uniqueness rule applied" in caplog.text
    assert "band was skipped" in caplog.text


def test_weekly_trend_strips_polluting_now_history_but_keeps_cache_current():
    # BUG: the final "truthful now" refresh_all_for_tables updates dq_score_cache
    # but ScoreCacheService ALSO appends one un-re-dated dq_score_history row per
    # scope at real wall-clock now() — those clustered real-now points pollute the
    # back-dated weekly trend. The seed must (a) still call refresh_all_for_tables
    # so the current cache is truthful, and (b) issue a history cleanup that
    # deletes every history row appended after the shared "now" cutoff.
    svc, deps = _svc()
    _use_create_path(deps)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="r1")
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    deps["app_sql"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    deps["oltp"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"

    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2

    def _query_dicts(sql, *_a, **_k):
        if "SELECT 1" in sql:
            return []  # SELECT 1 existence probe (off-target re-date check + gate delete-verify): no rows
        if "metric_name" in sql:
            return _metrics_rows(50_000, in_band)
        return [{"status": "SUCCESS"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    svc.run(user_email="admin@example.com", wipe_first=False, weeks=1)

    # (a) the current cache is still refreshed truthfully
    assert deps["score_cache"].refresh_all_for_tables.called

    # (b) a history cleanup deletes rows appended after the "now" cutoff, so no
    # un-re-dated real-now history row survives to pollute the trend
    executed = [call.args[0] for call in deps["oltp"].execute.call_args_list]
    cleanup = [s for s in executed if s.startswith("DELETE FROM") and "dq_score_history" in s and "computed_at >" in s]
    assert cleanup, "expected a dq_score_history post-cutoff cleanup DELETE"

    # (c) an orphan-metrics sweep deletes dq_metrics rows whose run_id has no
    # dq_validation_runs row — the deleted-gate-run late-metrics leftovers that
    # otherwise orphan a real-now point on the dimension/severity charts
    app_executed = [call.args[0] for call in deps["app_sql"].execute.call_args_list]
    orphan_sweep = [
        s for s in app_executed if s.startswith("DELETE FROM") and "dq_metrics" in s and "run_id NOT IN" in s
    ]
    assert orphan_sweep, "expected an orphan dq_metrics anti-join sweep DELETE"

    # terminal status succeeded
    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state == "succeeded"


def test_orphan_sweep_runs_before_final_cache_refresh():
    # BUG: the overview/homepage headline scores disagreed with the trend's final
    # point. The final refresh_all_for_tables picks "latest published run per
    # table" by run_time DESC; a not-yet-swept gate run sits at real wall-clock
    # (newer than the final week's instant), so the cache latched the headline
    # onto a gate run that _delete_orphan_metrics then deleted — leaving the
    # cache scoring a run that no longer exists. The sweep MUST run BEFORE the
    # final refresh so the refresh only ever sees the clean, re-dated weekly runs.
    svc, deps = _svc()
    _use_create_path(deps)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="r1")
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    deps["app_sql"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    deps["oltp"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"

    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2

    events: list[str] = []

    def _query_dicts(sql, *_a, **_k):
        if "SELECT 1" in sql:
            return []
        if "metric_name" in sql:
            return _metrics_rows(50_000, in_band)
        return [{"status": "SUCCESS"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    def _app_execute(sql, *_a, **_k):
        if sql.startswith("DELETE FROM") and "dq_metrics" in sql and "run_id NOT IN" in sql:
            events.append("orphan_sweep")

    deps["app_sql"].execute.side_effect = _app_execute
    deps["score_cache"].refresh_all_for_tables.side_effect = lambda *_a, **_k: events.append("final_refresh") or (0, 0)

    svc.run(user_email="admin@example.com", wipe_first=False, weeks=1)

    assert "orphan_sweep" in events, "expected an orphan-metrics sweep"
    assert "final_refresh" in events, "expected a final cache refresh"
    # refresh_all_for_tables is only called once (the final truthful refresh), so
    # a simple index compare proves the sweep runs first — the refresh then never
    # sees the deleted gate run
    assert events.index("orphan_sweep") < events.index("final_refresh"), (
        f"orphan sweep must precede the final cache refresh; got {events!r}"
    )


def test_history_cleanup_cutoff_is_final_week_instant_not_now():
    # BUG: a now()-based cutoff only strips the FINAL refresh's real-now history
    # appends; the per-week appends (one straggler per trend step) predate it and
    # survive as clustered real-now points on the homepage global trend. The
    # cutoff must be the newest LEGITIMATE instant — the final week's instant
    # (now - 30min) — so EVERY un-re-dated real-now append (per-week + final) is
    # strictly after it and deleted, while all back-dated weekly points survive.
    svc, deps = _svc()
    _use_create_path(deps)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="r1")
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    deps["app_sql"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    deps["oltp"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"

    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2

    def _query_dicts(sql, *_a, **_k):
        if "SELECT 1" in sql:
            return []
        if "metric_name" in sql:
            return _metrics_rows(50_000, in_band)
        return [{"status": "SUCCESS"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    weeks = 3
    now = datetime(2026, 7, 14, 12, 0, 0, tzinfo=timezone.utc)
    svc._build_weekly_trend(
        {b.table: f"b-{b.table}" for b in manifest.BINDINGS},
        {spec.key: "r1" for spec in manifest.RULES},
        ["p1"],
        weeks,
        "admin@example.com",
        now,
    )

    # the final-week instant is the intended cutoff (now - 30min), NOT a value
    # near real wall-clock now — every genuine point is at-or-before it
    expected_cutoff = redate.iso(svc._week_instant(now, weeks - 1, weeks))
    executed = [call.args[0] for call in deps["oltp"].execute.call_args_list]
    cleanup = [s for s in executed if s.startswith("DELETE FROM") and "dq_score_history" in s and "computed_at >" in s]
    assert cleanup, "expected a dq_score_history post-cutoff cleanup DELETE"
    assert any(expected_cutoff in s for s in cleanup), (
        f"cleanup cutoff must be the final-week instant {expected_cutoff!r}; got {cleanup!r}"
    )


def test_tighten_card_rule_bumps_version_via_edit_submit_approve():
    # BUG E: the "tightened card validation" beat must produce a REAL registry
    # rule version increment — an edit-in-place + re-approve of the card rule.
    svc, deps = _svc()
    approved = MagicMock(rule_id="card-rid", version=2)
    deps["registry"].approve.return_value = approved

    svc._tighten_card_rule({"card_format": "card-rid"}, "admin@example.com")

    # genuine revision path: update the approved rule, submit, then approve (bumps version)
    deps["registry"].update_draft.assert_called_once()
    assert deps["registry"].update_draft.call_args.args[0] == "card-rid"
    deps["registry"].submit.assert_called_once_with("card-rid", "admin@example.com")
    deps["registry"].approve.assert_called_once_with("card-rid", "admin@example.com")
    # the edit carries the tightened description (metadata-only, fingerprint-stable)
    metadata = deps["registry"].update_draft.call_args.kwargs["user_metadata"]
    assert manifest.CARD_RULE_TIGHTENED_DESCRIPTION in json.dumps(metadata)


def test_tighten_card_rule_failure_does_not_abort_seed():
    # BUG E: the version bump is best-effort — a failure is logged and swallowed.
    svc, deps = _svc()
    deps["registry"].update_draft.side_effect = RuntimeError("edit rejected")
    # must not raise
    svc._tighten_card_rule({"card_format": "card-rid"}, "admin@example.com")


def test_weekly_trend_tightens_card_rule_at_tighten_week():
    # BUG E: the weekly loop must invoke the card-rule version bump exactly at
    # TIGHTEN_WEEK. Run enough weeks to cross it.
    svc, deps = _svc()
    _use_create_path(deps)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="card-rid")
    deps["registry"].approve.return_value = MagicMock(rule_id="card-rid", version=2)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    deps["app_sql"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    deps["oltp"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"

    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2

    def _query_dicts(sql, *_a, **_k):
        if "SELECT 1" in sql:
            return []  # SELECT 1 existence probe (off-target re-date check + gate delete-verify): no rows
        if "metric_name" in sql:
            return _metrics_rows(50_000, in_band)
        return [{"status": "SUCCESS"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    svc.run(user_email="admin@example.com", wipe_first=False, weeks=manifest.TIGHTEN_WEEK + 1)

    # update_draft is only called by the tighten path (bindings use apply_rules)
    assert deps["registry"].update_draft.called
    assert deps["registry"].update_draft.call_args.args[0] == "card-rid"


def test_redate_version_freezes_spreads_freezes_across_trend_window():
    # Item 2: version freezes are written at seed-time "now"; left unmoved they
    # all sit AFTER every back-dated run, so annotate_trend_versions resolves
    # every trend point to version 0 and the results-over-time version markers
    # never appear. _redate_version_freezes must re-date each binding's freezes
    # into the trend window (strictly increasing, in version order) so version
    # bumps land mid-timeline and each version's first appearance gets a marker.
    svc, deps = _svc()
    deps["oltp"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    now = datetime(2026, 7, 14, 12, 0, 0, tzinfo=timezone.utc)
    weeks = 4
    # two bindings, each with two real freezes (v1 at build, v2 at week 0)
    svc._freeze_log = [("b-orders", 1), ("b-orders", 2), ("b-customers", 1), ("b-customers", 2)]

    svc._redate_version_freezes(now, weeks)

    executed = [call.args[0] for call in deps["oltp"].execute.call_args_list]
    version_updates = [s for s in executed if "dq_monitored_table_versions" in s and s.startswith("UPDATE")]
    # every logged freeze is re-dated (2 bindings x 2 versions)
    assert len(version_updates) == 4
    first_iso = redate.iso(svc._week_instant(now, 0, weeks))
    last_iso = redate.iso(svc._week_instant(now, weeks - 1, weeks))
    # v1 anchors at the first-week instant; the last freeze stays strictly before
    # the final-week instant so the final run still resolves to the top version
    assert any(first_iso in s and "version = 1" in s and "b-orders" in s for s in version_updates)
    assert all(last_iso not in s for s in version_updates)


def test_redate_version_freezes_noop_without_freezes_or_weeks():
    svc, deps = _svc()
    deps["oltp"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    svc._freeze_log = [("b1", 1)]
    svc._redate_version_freezes(datetime(2026, 7, 14, tzinfo=timezone.utc), 0)
    assert not deps["oltp"].execute.called


def test_validation_gate_submits_all_runs_before_waiting():
    # FIX J (perf): run_binding only SUBMITS an async serverless Job (returns
    # immediately); the wait is what serializes. The gate must submit EVERY
    # binding's run first, THEN wait — so the tables' Jobs run concurrently on
    # Databricks rather than one table at a time.
    svc, deps = _svc()
    deps["app_sql"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    events: list[str] = []

    def _run_binding(binding_id, *_a, **_k):
        events.append(f"submit:{binding_id}")
        return MagicMock(run_id=f"run-{binding_id}")

    deps["binding_run"].run_binding.side_effect = _run_binding

    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2

    def _query_dicts(sql, *_a, **_k):
        if "SELECT 1" in sql:
            return []  # SELECT 1 existence probe (off-target re-date check + gate delete-verify): no rows
        if "metric_name" in sql:
            return _metrics_rows(50_000, in_band)
        events.append("wait")
        return [{"status": "SUCCESS"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    binding_map = {"customers": "b-customers", "orders": "b-orders", "products": "b-products"}
    svc._validation_gate(binding_map, "admin@example.com")

    submits = [i for i, e in enumerate(events) if e.startswith("submit:")]
    waits = [i for i, e in enumerate(events) if e == "wait"]
    # every binding was submitted (one per table)
    assert len(submits) == len(binding_map)
    # ALL submits happen before the FIRST wait — the fan-out invariant
    assert max(submits) < min(waits)


def test_weekly_trend_submits_all_bindings_before_waiting_or_redating():
    # FIX J (perf): within a week the trend loop must submit every binding's run
    # first, THEN wait for all, THEN re-date each — never submit/wait/redate one
    # table at a time (which serializes the async Jobs).
    svc, deps = _svc()
    _use_create_path(deps)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="r1")
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["app_sql"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    deps["oltp"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"

    events: list[str] = []

    def _run_binding(binding_id, *_a, **_k):
        events.append("submit")
        return MagicMock(run_id="run1")

    deps["binding_run"].run_binding.side_effect = _run_binding

    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2

    def _query_dicts(sql, *_a, **_k):
        if "SELECT 1" in sql:
            return []  # SELECT 1 existence probe (off-target re-date check + gate delete-verify): no rows
        if "metric_name" in sql:
            return _metrics_rows(50_000, in_band)
        # The metrics-existence poll (FIX E) reads run_id from dq_metrics; it must
        # see a present row so the re-date proceeds. It is NOT a run-status wait.
        if "dq_metrics" in sql:
            return [{"run_id": "run1"}]
        events.append("wait")
        return [{"status": "SUCCESS"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    def _redate(sql, *_a, **_k):
        # a re-date is an UPDATE on dq_validation_runs / dq_metrics
        if sql.startswith("UPDATE") and "dq_validation_runs" in sql:
            events.append("redate")

    deps["app_sql"].execute.side_effect = _redate

    binding_map = {"customers": "b-customers", "orders": "b-orders", "products": "b-products"}
    rule_map = {spec.key: "r1" for spec in manifest.RULES}
    svc._build_weekly_trend(binding_map, rule_map, ["p1"], 1, "admin@example.com", datetime.now(timezone.utc))

    submits = [i for i, e in enumerate(events) if e == "submit"]
    waits = [i for i, e in enumerate(events) if e == "wait"]
    redates = [i for i, e in enumerate(events) if e == "redate"]
    assert len(submits) == len(binding_map)
    # ALL submits precede the FIRST wait, and every wait precedes the FIRST
    # re-date — the submit-all -> wait-all -> redate-all fan-out shape.
    assert max(submits) < min(waits)
    assert max(waits) < min(redates)


def test_weekly_trend_sweeps_orphans_before_each_weeks_score_refresh():
    # BUG: customers/orders trend went FLAT — the same wrong score frozen into all
    # 9 weekly points — because the validation gate's baseline runs left orphan
    # dq_metrics at real wall-clock (newer than every back-dated instant), so each
    # week's refresh_for_tables picked the GATE run's baseline score via its
    # "latest run by run_time DESC" window instead of THAT week's re-dated run.
    # The per-week orphan sweep must run AFTER the runs are waited for and BEFORE
    # the first per-table score refresh, every week.
    svc, deps = _svc()
    _use_create_path(deps)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="r1")
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    deps["app_sql"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    deps["oltp"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"

    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2
    events: list[str] = []

    def _query_dicts(sql, *_a, **_k):
        if "SELECT 1" in sql:
            return []
        if "metric_name" in sql:
            return _metrics_rows(50_000, in_band)
        if "dq_metrics" in sql:
            return [{"run_id": "run1"}]
        return [{"status": "SUCCESS"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    def _app_execute(sql, *_a, **_k):
        if sql.startswith("DELETE FROM") and "dq_metrics" in sql and "run_id NOT IN" in sql:
            events.append("sweep")

    deps["app_sql"].execute.side_effect = _app_execute
    deps["score_cache"].refresh_for_tables.side_effect = lambda *_a, **_k: events.append("refresh") or 1

    weeks = 2
    binding_map = {"customers": "b-customers", "orders": "b-orders"}
    rule_map = {spec.key: "r1" for spec in manifest.RULES}
    svc._build_weekly_trend(binding_map, rule_map, ["p1"], weeks, "admin@example.com", datetime.now(timezone.utc))

    # one sweep per week (before that week's per-table refreshes) PLUS the final
    # pre-refresh sweep => weeks + 1. (The final refresh_all_for_tables is a
    # different method, not captured by the refresh_for_tables mock here.)
    assert events.count("sweep") == weeks + 1, f"expected a sweep per week plus a final sweep; got {events!r}"
    # the FIRST op each week is the sweep — no refresh precedes the first sweep
    assert events[0] == "sweep", f"first weekly op must be the orphan sweep; got {events!r}"
    # every refresh is preceded by the sweep of its week: between consecutive
    # sweeps there is at least one sweep before any refresh in that block
    first_refresh = events.index("refresh")
    assert events.index("sweep") < first_refresh, (
        f"each week's orphan sweep must precede its score refresh; got {events!r}"
    )
    # the expected interleave for 2 weeks x 2 tables: sweep,refresh,refresh per
    # week, then a trailing final sweep
    assert events == ["sweep", "refresh", "refresh", "sweep", "refresh", "refresh", "sweep"], (
        f"unexpected sweep/refresh interleave; got {events!r}"
    )


def _runs_table_query_dicts(table_rows: list[dict[str, str]]):
    """A query-aware ``dq_validation_runs`` mock over a simulated multi-row table.

    The real table holds MORE THAN ONE row per run_id (an app-written RUNNING
    placeholder plus a runner-appended terminal row). The seed's poll SQL filters
    ``status IN (...)`` so the DB returns only the terminal row(s). This helper
    replays that server-side filter over *table_rows* so the test proves the
    production query is correct rather than hand-feeding a pre-filtered result.
    """

    def _query(sql: str, *_a, **_k):
        rows = list(table_rows)
        if "status IN (" in sql:
            rows = [row for row in rows if f"'{row['status']}'" in sql.split("status IN (", 1)[1]]
            return rows[:1] if "LIMIT 1" in sql else rows
        return rows

    return _query


def test_wait_for_run_returns_terminal_when_both_rows_present_running_first():
    # BUG 1 regression: dq_validation_runs holds TWO rows per run_id — an
    # app-written RUNNING placeholder and a runner-appended terminal row. The old
    # poll had NO status filter and read ``rows[0]`` of an unordered set, so it
    # could latch onto the stale RUNNING placeholder even after SUCCESS was
    # appended, causing a false timeout on a run that actually finished. The fixed
    # poll asks directly for a terminal row and returns it promptly.
    svc, deps = _svc()
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["app_sql"].query_dicts.side_effect = _runs_table_query_dicts([{"status": "RUNNING"}, {"status": "SUCCESS"}])
    assert svc._wait_for_run("run1") == "SUCCESS"
    assert deps["app_sql"].query_dicts.call_count == 1  # no extra polling / no timeout


def test_wait_for_run_returns_terminal_when_terminal_row_first():
    # Same regression, opposite physical row order: the terminal row precedes the
    # RUNNING placeholder. Order must not matter.
    svc, deps = _svc()
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["app_sql"].query_dicts.side_effect = _runs_table_query_dicts([{"status": "SUCCESS"}, {"status": "RUNNING"}])
    assert svc._wait_for_run("run1") == "SUCCESS"


def test_wait_for_run_keeps_polling_while_only_running_then_returns_terminal(monkeypatch):
    # A still-only-RUNNING table (no terminal row yet) yields an empty terminal
    # query -> keep polling; once the runner appends a terminal row alongside the
    # placeholder, return it.
    from databricks_labs_dqx_app.backend.demo import seed_service as ss

    monkeypatch.setattr(ss, "_RUN_POLL_SECONDS", 0)  # module-level constant: no real sleep
    svc, deps = _svc()
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    # The terminal-filtered poll returns nothing while only the RUNNING
    # placeholder exists, then the terminal row once the runner appends it.
    deps["app_sql"].query_dicts.side_effect = [
        [],  # placeholder only — no terminal row yet
        [],  # still running
        [{"status": "SUCCESS"}],  # terminal row appears
    ]
    assert svc._wait_for_run("run1") == "SUCCESS"
    assert deps["app_sql"].query_dicts.call_count == 3


def test_wait_for_run_detects_failed_terminal_row_behind_placeholder():
    # A FAILED/CANCELED terminal row must still be detected and returned even when
    # a RUNNING placeholder is also present — the caller may need to know the run
    # failed rather than seeing an indefinite RUNNING.
    svc, deps = _svc()
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["app_sql"].query_dicts.side_effect = _runs_table_query_dicts([{"status": "RUNNING"}, {"status": "FAILED"}])
    assert svc._wait_for_run("run1") == "FAILED"


def test_redate_run_waits_for_metrics_rows_before_updating(monkeypatch):
    # FIX E (root cause): the runner appends the terminal dq_validation_runs
    # (SUCCESS) row BEFORE it writes the run's dq_metrics rows, and _wait_for_run
    # gates only on dq_validation_runs. So _redate_run must first wait for the
    # dq_metrics rows to EXIST for this run_id, otherwise its metrics UPDATE
    # matches zero rows and the week is stranded at wall-clock now (the isolated
    # 'now' point in the Score-by-Severity chart). Assert the existence poll runs
    # and that both re-date UPDATEs are then issued.
    from databricks_labs_dqx_app.backend.demo import seed_service as ss

    monkeypatch.setattr(ss, "_METRICS_POLL_SECONDS", 0)  # no real sleep in the poll loop
    svc, deps = _svc()
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    # The existence poll sees no rows first, rows on the second poll; then the
    # post-re-date off-target check ('<>' query) reports everything on target.
    existence_calls = {"n": 0}

    def _query_dicts(sql, *_a, **_k):
        assert "dq_metrics" in sql
        if "SELECT 1" in sql:
            return []  # off-target check: no rows off the target instant
        existence_calls["n"] += 1
        return [] if existence_calls["n"] == 1 else [{"run_id": "run1"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    svc._redate_run("run1", "2026-05-01 09:30:00")

    assert existence_calls["n"] == 2  # polled until the metrics rows appeared
    updates = [c.args[0] for c in deps["app_sql"].execute.call_args_list if c.args[0].startswith("UPDATE")]
    assert any("dq_metrics" in s for s in updates)
    assert any("dq_validation_runs" in s for s in updates)


def test_redate_run_logs_loudly_when_metrics_stay_off_target(monkeypatch, caplog):
    # FIX E: if metric rows for the run stay off the target instant past the
    # bounded window (e.g. they never all materialise), the re-date loop must NOT
    # silently give up — it emits a loud demo-layer warning so a stranded week
    # (a potential stray 'now' trend point) is never invisible.
    from databricks_labs_dqx_app.backend.demo import seed_service as ss

    monkeypatch.setattr(ss, "_METRICS_TIMEOUT_SECONDS", 0)  # deadline already passed
    monkeypatch.setattr(ss, "_METRICS_POLL_SECONDS", 0)
    svc, deps = _svc()
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    # existence poll finds rows, but the off-target check ('<>') always reports a
    # straggler — so the loop can never confirm a clean re-date and, with the
    # deadline already passed, must warn.
    deps["app_sql"].query_dicts.side_effect = lambda sql, *_a, **_k: [{"run_id": "run1"}]

    with caplog.at_level("WARNING", logger="databricks_labs_dqx_app.backend.demo.seed_service"):
        svc._redate_run("run1", "2026-05-01 09:30:00")
    assert "still has dq_metrics rows off the target instant" in caplog.text


def test_weekly_trend_redates_every_run_id_once_per_binding_per_week():
    # FIX E: every week's run for every binding must be re-dated exactly once, to
    # THAT week's target instant — so no later week is left un-re-dated at real
    # wall-clock. Give each binding a distinct run_id and assert _redate_run's
    # metrics UPDATE fires once per (binding, week) with the week's target_iso.
    svc, deps = _svc()
    _use_create_path(deps)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="r1")
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["oltp"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"

    run_seq = {"n": 0}

    def _run_binding(*_a, **_k):
        run_seq["n"] += 1
        return MagicMock(run_id=f"run-{run_seq['n']}")

    deps["binding_run"].run_binding.side_effect = _run_binding
    # run-status poll + metrics-existence poll both return a present, terminal row.
    deps["app_sql"].query_dicts.side_effect = lambda sql, *_a, **_k: (
        [] if "SELECT 1" in sql else ([{"run_id": "present"}] if "dq_metrics" in sql else [{"status": "SUCCESS"}])
    )

    weeks = 3
    now = datetime(2026, 7, 14, 12, 0, 0, tzinfo=timezone.utc)
    binding_map = {"customers": "b-customers", "orders": "b-orders", "products": "b-products"}
    rule_map = {spec.key: "r1" for spec in manifest.RULES}
    svc._build_weekly_trend(binding_map, rule_map, ["p1"], weeks, "admin@example.com", now)

    metrics_updates = [
        c.args[0]
        for c in deps["app_sql"].execute.call_args_list
        if c.args[0].startswith("UPDATE") and "dq_metrics" in c.args[0]
    ]
    # one metrics re-date per (binding, week) — nothing left un-re-dated
    assert len(metrics_updates) == len(binding_map) * weeks
    # every week's target instant is represented in the re-dates
    week_isos = {redate.iso(DemoSeedService._week_instant(now, w, weeks)) for w in range(weeks)}
    for target_iso in week_isos:
        assert any(target_iso in s for s in metrics_updates), f"week instant {target_iso} was never re-dated"


def test_weekly_trend_tightens_card_rule_exactly_once_no_version_cluster():
    # FIX D: the ONE deliberate rule-content evolution (the card-rule tighten) must
    # fire exactly ONCE across the whole run — at TIGHTEN_WEEK — never every week,
    # which is what produced the user-flagged "clustered version changes". Binding
    # re-approvals (which bump binding versions) happen only when a binding's
    # active rule-key set CHANGES (a lifecycle week), not every week.
    svc, deps = _svc()
    _use_create_path(deps)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="card-rid")
    deps["registry"].approve.return_value = MagicMock(rule_id="card-rid", version=2)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["oltp"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["app_sql"].query_dicts.side_effect = lambda sql, *_a, **_k: (
        [] if "SELECT 1" in sql else ([{"run_id": "present"}] if "dq_metrics" in sql else [{"status": "SUCCESS"}])
    )

    rule_map = {spec.key: "card-rid" for spec in manifest.RULES}
    binding_map = {b.table: f"b-{b.table}" for b in manifest.BINDINGS}
    weeks = manifest.WEEKS_DEFAULT  # spans TIGHTEN_WEEK
    svc._build_weekly_trend(binding_map, rule_map, ["p1"], weeks, "admin@example.com", datetime.now(timezone.utc))

    # the content edit (update_draft) fired exactly once — no per-week clustering
    assert deps["registry"].update_draft.call_count == 1


def test_weekly_trend_reapproves_bindings_only_on_lifecycle_change():
    # FIX D: binding version bumps (each _approve_binding call) must happen only
    # when a binding's active rule-key set CHANGES, not every week — otherwise
    # every week would re-approve every binding and cluster binding versions.
    # customers has three lifecycle transitions (tier_set@2, unique@3,
    # not_future retired@6) plus its week-0 initial apply => 4 approvals over the
    # full run, NOT one-per-week.
    svc, deps = _svc()
    _use_create_path(deps)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="r1")
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["oltp"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["app_sql"].query_dicts.side_effect = lambda sql, *_a, **_k: (
        [] if "SELECT 1" in sql else ([{"run_id": "present"}] if "dq_metrics" in sql else [{"status": "SUCCESS"}])
    )

    applied_per_table: dict[str, int] = {}

    def _save(binding_id, *_a, **_k):
        applied_per_table[binding_id] = applied_per_table.get(binding_id, 0) + 1

    deps["apply_rules"].save_applied_rules.side_effect = _save

    rule_map = {spec.key: "r1" for spec in manifest.RULES}
    binding_map = {b.table: f"b-{b.table}" for b in manifest.BINDINGS}
    weeks = manifest.WEEKS_DEFAULT
    svc._build_weekly_trend(binding_map, rule_map, ["p1"], weeks, "admin@example.com", datetime.now(timezone.utc))

    # customers: week-0 apply + 3 lifecycle transitions (wk2, wk3, wk6) = 4, far
    # fewer than one-per-week (which would be `weeks`).
    assert applied_per_table["b-customers"] == 4
    assert applied_per_table["b-customers"] < weeks


def test_weekly_trend_redates_history_for_every_scope_each_week():
    # BUG 2 guard: each weekly refresh appends exactly ONE dq_score_history row
    # per scope (table / product / global) at real now(); the per-week
    # _redate_history must shift EACH appended row to that week's instant. Assert
    # a history re-date UPDATE is issued for every scope, every week — so no
    # scope's weekly point is left stranded at wall-clock now.
    svc, deps = _svc()
    _use_create_path(deps)
    deps["registry"].find_approved_rule_for_definition.side_effect = lambda _d: MagicMock(rule_id="r1")
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["data_products"].create.return_value = MagicMock(product_id="p1")
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="run1")
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["oltp"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["app_sql"].query_dicts.side_effect = lambda sql, *_a, **_k: (
        [] if "SELECT 1" in sql else [{"status": "SUCCESS"}]
    )

    weeks = 2
    binding_map = {"customers": "b-customers", "orders": "b-orders", "products": "b-products"}
    rule_map = {spec.key: "r1" for spec in manifest.RULES}
    svc._build_weekly_trend(binding_map, rule_map, ["p1"], weeks, "admin@example.com", datetime.now(timezone.utc))

    history_updates = [
        call.args[0]
        for call in deps["oltp"].execute.call_args_list
        if call.args and call.args[0].startswith("UPDATE") and "dq_score_history" in call.args[0]
    ]
    table_redates = [s for s in history_updates if "scope_type = 'table'" in s]
    product_redates = [s for s in history_updates if "scope_type = 'product'" in s]
    global_redates = [s for s in history_updates if "scope_type = 'global'" in s]
    assert len(table_redates) == len(binding_map) * weeks  # every table, every week
    assert len(product_redates) == 1 * weeks  # the one product, every week
    assert len(global_redates) == 1 * weeks  # global, every week


def test_week_instant_matches_dqlake_cadence_irregular_monotonic_and_final_recent():
    # FIX C: the trend's calendar span reproduces dqlake's exact seed_demo cadence
    # (fixed GAP_DAYS / HOURS / minute-offset lists) — irregular gaps + varied
    # hours, oldest-first, widening well beyond a uniform 7-day cadence, with the
    # final week landing on dqlake's final_instant (now - 30 minutes), NOT exactly
    # now. Deterministic (no randomness).
    from datetime import timedelta

    now = datetime(2026, 7, 14, 12, 0, 0, tzinfo=timezone.utc)
    weeks = manifest.WEEKS_DEFAULT
    pts = [DemoSeedService._week_instant(now, w, weeks) for w in range(weeks)]

    # final week is dqlake's final_instant: a single recent instant, now - 30min
    assert pts[-1] == now - timedelta(minutes=30)
    # strictly oldest-first (monotonic increasing) and all strictly before now
    assert all(pts[i] < pts[i + 1] for i in range(len(pts) - 1))
    assert all(p <= now for p in pts)
    # gaps between consecutive points are NOT all equal (irregular cadence)
    gaps = {(pts[i + 1] - pts[i]).days for i in range(len(pts) - 1)}
    assert len(gaps) > 1, "expected irregular day gaps, not a uniform cadence"
    # the span is materially wider than a tidy weeks*7 uniform cadence would give
    assert (now - pts[0]).days > weeks * 7

    # Reproduces dqlake's seed_demo.py week_ts construction EXACTLY.
    gap_days = (11, 4, 9, 14, 6, 3, 12, 8, 5, 13, 7, 10)
    hours = (9, 14, 6, 19, 11, 2, 16, 22, 8, 13, 4, 20)
    gaps_ref = [gap_days[i % len(gap_days)] for i in range(weeks - 1)]
    days_ago = [0] * weeks
    acc = 0
    for i in range(weeks - 2, -1, -1):
        acc += gaps_ref[weeks - 2 - i]
        days_ago[i] = acc
    expected = [
        now - timedelta(days=days_ago[i], hours=hours[i % len(hours)], minutes=(i * 17) % 60) for i in range(weeks)
    ]
    expected[weeks - 1] = now - timedelta(minutes=30)
    assert pts == expected


def test_week_instant_is_deterministic():
    # FIX 3: index into fixed lists — same inputs must yield the same instant.
    now = datetime(2026, 7, 14, 12, 0, 0, tzinfo=timezone.utc)
    a = DemoSeedService._week_instant(now, 3, manifest.WEEKS_DEFAULT)
    b = DemoSeedService._week_instant(now, 3, manifest.WEEKS_DEFAULT)
    assert a == b


def test_validation_gate_deletes_gate_runs_from_both_tables():
    # FIX 1: the validation-gate runs execute at real "now" and are never
    # re-dated, so they must be deleted from dq_metrics AND dq_validation_runs
    # after the misfire assertions pass — otherwise a gate run can outrank the
    # re-dated (past) weekly runs in the score cache's latest-run selection.
    svc, deps = _svc()
    deps["app_sql"].fqn.side_effect = lambda table: f"dqx.dqx_studio.{table}"
    deps["binding_run"].run_binding.return_value = MagicMock(run_id="gate-run")
    # wait-for-run sees SUCCESS; misfire read sees an in-band unique count.
    low, high = manifest.UNIQUE_EXPECT_ROWS
    in_band = (low + high) // 2

    def _query_dicts(sql, *_a, **_k):
        if "SELECT 1" in sql:
            return []  # SELECT 1 existence probe (off-target re-date check + gate delete-verify): no rows
        if "metric_name" in sql:
            return _metrics_rows(50_000, in_band)
        return [{"status": "SUCCESS"}]

    deps["app_sql"].query_dicts.side_effect = _query_dicts

    binding_map = {"customers": "b-customers", "orders": "b-orders"}
    svc._validation_gate(binding_map, "admin@example.com")

    executed = [call.args[0] for call in deps["app_sql"].execute.call_args_list]
    deletes = [sql for sql in executed if sql.startswith("DELETE FROM")]
    # one DELETE per gate run against each of the two tables
    assert any(s.startswith("DELETE FROM") and "dq_metrics" in s and "gate-run" in s for s in deletes)
    assert any(s.startswith("DELETE FROM") and "dq_validation_runs" in s and "gate-run" in s for s in deletes)


# ---------------------------------------------------------------------------
# Item 31: the SSN rule is submitted-for-approval only — never approved, never
# embedded, never mapped to a binding.
# ---------------------------------------------------------------------------


def test_pending_approval_rule_is_submitted_but_never_approved_or_mapped():
    # The SSN rule (in PENDING_APPROVAL_RULE_KEYS) is created + submitted so it
    # lands in the Review & Approve queue, but is NEVER approved and NEVER
    # returned in the rule_map (so no binding can map it) — it stays an
    # UNMAPPED, pending_approval library draft.
    assert manifest.PENDING_APPROVAL_RULE_KEYS, "expected at least one pending-approval demo rule"
    svc, deps = _svc()
    _use_create_path(deps)

    created_keys: list[str] = []
    submitted_ids: list[str] = []
    approved_ids: list[str] = []

    def _create(*_a, **kwargs):
        # derive the rule key from the reserved name tag in user_metadata
        meta = kwargs["user_metadata"]
        name = json.dumps(meta)
        rid = f"rid-{len(created_keys)}"
        created_keys.append(name)
        return (MagicMock(rule_id=rid), None)

    deps["registry"].create_rule.side_effect = _create
    deps["registry"].submit.side_effect = lambda rid, _u: submitted_ids.append(rid) or MagicMock(rule_id=rid)
    deps["registry"].approve.side_effect = lambda rid, _u: approved_ids.append(rid) or MagicMock(rule_id=rid, version=2)

    rule_map = svc._build_rules("admin@example.com")

    # the pending rule is not in the map (so bindings can never resolve it)
    for key in manifest.PENDING_APPROVAL_RULE_KEYS:
        assert key not in rule_map, f"pending-approval rule {key!r} must not be in the rule_map"
    # every rule (incl. the pending one) was created + submitted...
    assert deps["registry"].create_rule.call_count == len(manifest.RULES)
    assert deps["registry"].submit.call_count == len(manifest.RULES)
    # ...but only the non-pending rules were approved
    assert deps["registry"].approve.call_count == len(manifest.RULES) - len(manifest.PENDING_APPROVAL_RULE_KEYS)


def test_pending_approval_rule_is_not_embedded():
    # A pending_approval rule must NOT enter the suggestion corpus (only approved
    # rules are embedded via the real approve route).
    embeddings = MagicMock()
    svc, deps = _svc(embeddings=embeddings)
    _use_create_path(deps)

    svc._build_rules("admin@example.com")

    embedded_count = len(manifest.RULES) - len(manifest.PENDING_APPROVAL_RULE_KEYS)
    assert embeddings.embed_and_store.call_count == embedded_count


def test_pending_approval_rule_key_references_a_real_manifest_rule():
    # Guard: every key in PENDING_APPROVAL_RULE_KEYS must name a real rule so a
    # typo can't silently disable the feature.
    for key in manifest.PENDING_APPROVAL_RULE_KEYS:
        assert key in manifest.RULES_BY_KEY, f"{key!r} is not a manifest rule"


def test_ssn_rule_is_unmapped_in_every_binding():
    # The SSN rule must not appear in any binding's mappings — it stays an
    # unmapped library rule.
    for binding in manifest.BINDINGS:
        assert "ssn_format" not in binding.mappings, f"ssn_format must not be bound to {binding.table}"


# ---------------------------------------------------------------------------
# Item 10: the demo runs a real profiler job on a demo table.
# ---------------------------------------------------------------------------


def test_run_profiling_submits_job_and_records_result_row():
    # The profiling phase creates a temp view, submits the profiler Job, records
    # the RUNNING placeholder, polls dq_profiling_results to a terminal SUCCESS,
    # then drops the view — producing a real profiler result the Profile page renders.
    job_service = MagicMock()
    job_service.submit_run.return_value = 9911
    profiler_view = MagicMock()
    profiler_view.create_view.return_value = "dqx.dqx_studio_tmp.tmp_view_abc"
    svc, deps = _svc(job_service=job_service, profiler_view=profiler_view)
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["app_sql"].query_dicts.return_value = [{"status": "SUCCESS"}]

    run_id = svc._run_profiling("admin@example.com")

    assert run_id is not None
    # a view was created over the demo profile table and dropped afterwards
    profiler_view.create_view.assert_called_once()
    assert manifest.PROFILE_DEMO_TABLE in profiler_view.create_view.call_args.args[0]
    profiler_view.drop_view.assert_called_once_with("dqx.dqx_studio_tmp.tmp_view_abc")
    # the profiler Job was submitted with task_type=profile
    assert job_service.submit_run.call_args.kwargs["task_type"] == "profile"
    # a RUNNING placeholder was recorded into dq_profiling_results
    assert "dq_profiling_results" in job_service.record_run_started.call_args.kwargs["table"]
    assert job_service.record_run_started.call_args.kwargs["run_id"] == run_id


def test_run_profiling_is_invoked_as_a_phase_of_run():
    # run() must invoke the profiling phase (so the demo actually profiles) and
    # report a "profile" phase status.
    job_service = MagicMock()
    job_service.submit_run.return_value = 1
    profiler_view = MagicMock()
    profiler_view.create_view.return_value = "dqx.dqx_studio_tmp.tmp_view_x"
    svc, deps = _svc(job_service=job_service, profiler_view=profiler_view)
    _use_create_path(deps)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"
    deps["app_sql"].query_dicts.return_value = [{"status": "SUCCESS"}]

    svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)

    job_service.submit_run.assert_called_once()
    phases = [c.args[0].phase for c in deps["status"].set.call_args_list]
    assert "profile" in phases
    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state == "succeeded"


def test_run_profiling_is_a_noop_without_collaborators():
    # With no job service / view service (minimal test graph, no compute), the
    # profiling phase is a logged no-op — it never raises.
    svc, _deps = _svc()  # no job_service / profiler_view
    assert svc._run_profiling("admin@example.com") is None


def test_run_profiling_failure_does_not_abort_seed():
    # A profiler failure (submit error / timeout) must be swallowed so the
    # ~30min seed continues; the view is still dropped.
    job_service = MagicMock()
    job_service.submit_run.side_effect = RuntimeError("no warehouse")
    profiler_view = MagicMock()
    profiler_view.create_view.return_value = "dqx.dqx_studio_tmp.tmp_view_y"
    svc, deps = _svc(job_service=job_service, profiler_view=profiler_view)
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"

    # must not raise
    svc._run_profiling("admin@example.com")
    profiler_view.drop_view.assert_called_once_with("dqx.dqx_studio_tmp.tmp_view_y")


def test_run_profiling_failure_does_not_fail_the_whole_seed():
    # Even when profiling fails, run(weeks=0) still reaches a succeeded terminal
    # status — profiling is best-effort.
    job_service = MagicMock()
    job_service.submit_run.side_effect = RuntimeError("no warehouse")
    profiler_view = MagicMock()
    profiler_view.create_view.return_value = "dqx.dqx_studio_tmp.tmp_view_z"
    svc, deps = _svc(job_service=job_service, profiler_view=profiler_view)
    _use_create_path(deps)
    deps["monitored_tables"].register.return_value = MagicMock(binding_id="b1")
    deps["app_sql"].fqn.side_effect = lambda t: f"dqx.dqx_studio.{t}"

    svc.run(user_email="admin@example.com", wipe_first=False, weeks=0)

    last = deps["status"].set.call_args_list[-1].args[0]
    assert last.state == "succeeded"
